use std::fmt;

use capnp::Result as CapnpResult;
use serde_json::Value;
use thiserror::Error;

use crate::{
    control_capnp::{comparison_expression, filter_expression, logical_expression},
    error::EventError,
    store::{AggregateState, select_state_field},
};

#[derive(Debug, Clone)]
pub enum FilterExpr {
    And(Vec<FilterExpr>),
    Or(Vec<FilterExpr>),
    Not(Box<FilterExpr>),
    Comparison { field: String, op: ComparisonOp },
}

#[derive(Debug, Clone)]
pub enum ComparisonOp {
    Equals(FilterValue),
    NotEquals(FilterValue),
    GreaterThan(FilterValue),
    LessThan(FilterValue),
    In(Vec<FilterValue>),
    Like(FilterValue),
}

#[derive(Debug, Clone)]
pub enum FilterValue {
    String(String),
    Number(f64),
    Bool(bool),
    Null,
}

#[derive(Debug, Error)]
pub enum FilterParseError {
    #[error("{0}")]
    Message(String),
}

impl FilterExpr {
    pub fn from_capnp(reader: filter_expression::Reader<'_>) -> Result<Self, EventError> {
        match reader.which().map_err(not_in_schema)? {
            filter_expression::Which::Logical(expr) => {
                let expr = expr.map_err(capnp_error)?;
                parse_logical(expr)
            }
            filter_expression::Which::Comparison(expr) => {
                let expr = expr.map_err(capnp_error)?;
                parse_comparison(expr)
            }
        }
    }

    pub fn matches_aggregate(&self, aggregate: &AggregateState) -> bool {
        match self {
            FilterExpr::And(children) => children
                .iter()
                .all(|child| child.matches_aggregate(aggregate)),
            FilterExpr::Or(children) => children
                .iter()
                .any(|child| child.matches_aggregate(aggregate)),
            FilterExpr::Not(expr) => !expr.matches_aggregate(aggregate),
            FilterExpr::Comparison { field, op } => {
                let value = resolve_field_value(aggregate, field);
                evaluate_comparison(value, op)
            }
        }
    }

    pub fn write_to_capnp(
        &self,
        builder: filter_expression::Builder<'_>,
    ) -> Result<(), EventError> {
        match self {
            FilterExpr::And(children) => {
                let mut logical = builder.init_logical();
                let mut and_list = logical.reborrow().init_and(children.len() as u32);
                for (idx, child) in children.iter().enumerate() {
                    child.write_to_capnp(and_list.reborrow().get(idx as u32))?;
                }
            }
            FilterExpr::Or(children) => {
                let mut logical = builder.init_logical();
                let mut or_list = logical.reborrow().init_or(children.len() as u32);
                for (idx, child) in children.iter().enumerate() {
                    child.write_to_capnp(or_list.reborrow().get(idx as u32))?;
                }
            }
            FilterExpr::Not(expr) => {
                let mut logical = builder.init_logical();
                expr.write_to_capnp(logical.reborrow().init_not())?;
            }
            FilterExpr::Comparison { field, op } => {
                let comparison = builder.init_comparison();
                write_comparison(field, op, comparison)?;
            }
        }
        Ok(())
    }
}

fn parse_logical(expr: logical_expression::Reader<'_>) -> Result<FilterExpr, EventError> {
    match expr.which().map_err(not_in_schema)? {
        logical_expression::Which::And(list) => {
            let list = list.map_err(capnp_error)?;
            let mut nodes = Vec::with_capacity(list.len() as usize);
            for item in list.iter() {
                let expr = item;
                nodes.push(FilterExpr::from_capnp(expr)?);
            }
            Ok(FilterExpr::And(nodes))
        }
        logical_expression::Which::Or(list) => {
            let list = list.map_err(capnp_error)?;
            let mut nodes = Vec::with_capacity(list.len() as usize);
            for item in list.iter() {
                let expr = item;
                nodes.push(FilterExpr::from_capnp(expr)?);
            }
            Ok(FilterExpr::Or(nodes))
        }
        logical_expression::Which::Not(expr) => {
            let expr = expr.map_err(capnp_error)?;
            Ok(FilterExpr::Not(Box::new(FilterExpr::from_capnp(expr)?)))
        }
    }
}

fn parse_comparison(expr: comparison_expression::Reader<'_>) -> Result<FilterExpr, EventError> {
    match expr.which().map_err(not_in_schema)? {
        comparison_expression::Which::Equals(reader) => {
            let reader = reader.map_err(capnp_error)?;
            Ok(FilterExpr::Comparison {
                field: read_text(reader.get_field(), "field")?,
                op: ComparisonOp::Equals(FilterValue::from_capnp(reader.get_value())?),
            })
        }
        comparison_expression::Which::NotEquals(reader) => {
            let reader = reader.map_err(capnp_error)?;
            Ok(FilterExpr::Comparison {
                field: read_text(reader.get_field(), "field")?,
                op: ComparisonOp::NotEquals(FilterValue::from_capnp(reader.get_value())?),
            })
        }
        comparison_expression::Which::GreaterThan(reader) => {
            let reader = reader.map_err(capnp_error)?;
            Ok(FilterExpr::Comparison {
                field: read_text(reader.get_field(), "field")?,
                op: ComparisonOp::GreaterThan(FilterValue::from_capnp(reader.get_value())?),
            })
        }
        comparison_expression::Which::LessThan(reader) => {
            let reader = reader.map_err(capnp_error)?;
            Ok(FilterExpr::Comparison {
                field: read_text(reader.get_field(), "field")?,
                op: ComparisonOp::LessThan(FilterValue::from_capnp(reader.get_value())?),
            })
        }
        comparison_expression::Which::InSet(reader) => {
            let reader = reader.map_err(capnp_error)?;
            let field = read_text(reader.get_field(), "field")?;
            let values = reader.get_values().map_err(capnp_error)?;
            let mut list = Vec::with_capacity(values.len() as usize);
            for value in values.iter() {
                list.push(FilterValue::from_capnp(value)?);
            }
            Ok(FilterExpr::Comparison {
                field,
                op: ComparisonOp::In(list),
            })
        }
        comparison_expression::Which::Like(reader) => {
            let reader = reader.map_err(capnp_error)?;
            Ok(FilterExpr::Comparison {
                field: read_text(reader.get_field(), "field")?,
                op: ComparisonOp::Like(FilterValue::from_capnp(reader.get_value())?),
            })
        }
    }
}

impl FilterValue {
    fn from_capnp(reader: CapnpResult<capnp::text::Reader<'_>>) -> Result<Self, EventError> {
        let text = reader
            .map_err(capnp_error)?
            .to_str()
            .map_err(|err| EventError::Serialization(err.to_string()))?;
        Ok(FilterValue::parse_literal(text))
    }

    fn matches_value(&self, value: &ComparableValue) -> bool {
        match (self, value) {
            (FilterValue::Null, ComparableValue::Null) => true,
            (FilterValue::Bool(lhs), ComparableValue::Bool(rhs)) => lhs == rhs,
            (FilterValue::Number(lhs), ComparableValue::Number(rhs)) => {
                (lhs - rhs).abs() < f64::EPSILON
            }
            (FilterValue::String(lhs), ComparableValue::String(rhs)) => lhs == rhs,
            _ => false,
        }
    }

    fn parse_literal(input: &str) -> Self {
        let trimmed = input.trim();
        if trimmed.eq_ignore_ascii_case("null") {
            FilterValue::Null
        } else if trimmed.eq_ignore_ascii_case("true") {
            FilterValue::Bool(true)
        } else if trimmed.eq_ignore_ascii_case("false") {
            FilterValue::Bool(false)
        } else if let Ok(number) = trimmed.parse::<f64>() {
            FilterValue::Number(number)
        } else {
            FilterValue::String(trimmed.to_string())
        }
    }

    fn to_capnp_text(&self) -> String {
        match self {
            FilterValue::String(value) => value.clone(),
            FilterValue::Number(value) => value.to_string(),
            FilterValue::Bool(value) => value.to_string(),
            FilterValue::Null => "null".to_string(),
        }
    }
}

#[derive(Debug)]
enum ComparableValue {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Unsupported,
}

fn resolve_field_value(aggregate: &AggregateState, field: &str) -> Option<ComparableValue> {
    match field {
        "aggregate_type" | "aggregateType" => {
            Some(ComparableValue::String(aggregate.aggregate_type.clone()))
        }
        "aggregate_id" | "aggregateId" => {
            Some(ComparableValue::String(aggregate.aggregate_id.clone()))
        }
        "merkle_root" | "merkleRoot" => {
            Some(ComparableValue::String(aggregate.merkle_root.clone()))
        }
        "archived" => Some(ComparableValue::Bool(aggregate.archived)),
        "version" => Some(ComparableValue::Number(aggregate.version as f64)),
        other => select_state_field(&aggregate.state, other).map(|value| match value {
            Value::Null => ComparableValue::Null,
            Value::Bool(v) => ComparableValue::Bool(v),
            Value::Number(num) => num
                .as_f64()
                .map(ComparableValue::Number)
                .unwrap_or(ComparableValue::Unsupported),
            Value::String(ref text) => ComparableValue::String(text.clone()),
            _ => ComparableValue::Unsupported,
        }),
    }
}

fn evaluate_comparison(value: Option<ComparableValue>, op: &ComparisonOp) -> bool {
    match value {
        Some(val) => match op {
            ComparisonOp::Equals(expected) => expected.matches_value(&val),
            ComparisonOp::NotEquals(expected) => !expected.matches_value(&val),
            ComparisonOp::GreaterThan(expected) => {
                compare_numbers(&val, expected, Ordering::Greater)
            }
            ComparisonOp::LessThan(expected) => compare_numbers(&val, expected, Ordering::Less),
            ComparisonOp::In(expected) => expected
                .iter()
                .any(|candidate| candidate.matches_value(&val)),
            ComparisonOp::Like(pattern) => match (pattern, val) {
                (FilterValue::String(pat), ComparableValue::String(text)) => {
                    matches_like(&text, pat)
                }
                _ => false,
            },
        },
        None => matches_missing(op),
    }
}

#[derive(Clone, Copy)]
enum Ordering {
    Greater,
    Less,
}

fn compare_numbers(value: &ComparableValue, expected: &FilterValue, ordering: Ordering) -> bool {
    let ComparableValue::Number(lhs) = value else {
        return false;
    };
    let FilterValue::Number(rhs) = expected else {
        return false;
    };
    match ordering {
        Ordering::Greater => *lhs > *rhs,
        Ordering::Less => *lhs < *rhs,
    }
}

fn matches_like(text: &str, pattern: &str) -> bool {
    let mut regex_pattern = String::from("^");
    for ch in pattern.chars() {
        match ch {
            '%' | '*' => regex_pattern.push_str(".*"),
            '_' | '?' => regex_pattern.push('.'),
            '.' | '+' | '(' | ')' | '|' | '^' | '$' | '{' | '}' | '[' | ']' | '\\' => {
                regex_pattern.push('\\');
                regex_pattern.push(ch);
            }
            other => regex_pattern.push(other),
        }
    }
    regex_pattern.push('$');
    regex::Regex::new(&regex_pattern)
        .map(|re| re.is_match(text))
        .unwrap_or(false)
}

fn matches_missing(op: &ComparisonOp) -> bool {
    matches!(op, ComparisonOp::NotEquals(FilterValue::Null))
}

fn write_comparison(
    field: &str,
    op: &ComparisonOp,
    mut builder: comparison_expression::Builder<'_>,
) -> Result<(), EventError> {
    match op {
        ComparisonOp::Equals(value) => {
            let mut eq = builder.reborrow().init_equals();
            eq.set_field(field);
            eq.set_value(&value.to_capnp_text());
        }
        ComparisonOp::NotEquals(value) => {
            let mut ne = builder.reborrow().init_not_equals();
            ne.set_field(field);
            ne.set_value(&value.to_capnp_text());
        }
        ComparisonOp::GreaterThan(value) => {
            let mut gt = builder.reborrow().init_greater_than();
            gt.set_field(field);
            gt.set_value(&value.to_capnp_text());
        }
        ComparisonOp::LessThan(value) => {
            let mut lt = builder.reborrow().init_less_than();
            lt.set_field(field);
            lt.set_value(&value.to_capnp_text());
        }
        ComparisonOp::In(values) => {
            let mut set = builder.reborrow().init_in_set();
            set.set_field(field);
            let mut list = set.reborrow().init_values(values.len() as u32);
            for (idx, value) in values.iter().enumerate() {
                list.set(idx as u32, &value.to_capnp_text());
            }
        }
        ComparisonOp::Like(value) => {
            let mut like = builder.reborrow().init_like();
            like.set_field(field);
            like.set_value(&value.to_capnp_text());
        }
    }
    Ok(())
}

fn read_text(
    reader: CapnpResult<capnp::text::Reader<'_>>,
    label: &str,
) -> Result<String, EventError> {
    let text = reader
        .map_err(capnp_error)?
        .to_str()
        .map_err(|err| EventError::Serialization(format!("invalid utf-8 in {label}: {err}")))?
        .to_string();
    Ok(text)
}

fn capnp_error(err: capnp::Error) -> EventError {
    EventError::Serialization(err.to_string())
}

fn not_in_schema(err: capnp::NotInSchema) -> EventError {
    EventError::Serialization(err.to_string())
}

impl fmt::Display for FilterValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilterValue::String(value) => write!(f, "{value}"),
            FilterValue::Number(value) => write!(f, "{value}"),
            FilterValue::Bool(value) => write!(f, "{value}"),
            FilterValue::Null => write!(f, "null"),
        }
    }
}

// === Shorthand Parser ===

pub fn parse_shorthand(input: &str) -> Result<FilterExpr, FilterParseError> {
    let lexer = Lexer::new(input);
    let tokens = lexer.collect::<Result<Vec<_>, _>>()?;
    if tokens.is_empty() {
        return Err(FilterParseError::Message("filter cannot be empty".into()));
    }
    let mut parser = Parser::new(tokens);
    let expr = parser.parse_expression()?;
    parser.expect_end()?;
    Ok(expr)
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Identifier(String),
    String(String),
    Number(String),
    Boolean(bool),
    Null,
    And,
    Or,
    Not,
    Operator(Operator),
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Operator {
    Eq,
    Neq,
    Gt,
    Lt,
    In,
    Like,
}

struct Lexer<'a> {
    input: &'a str,
    position: usize,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, position: 0 }
    }
}

impl<'a> Iterator for Lexer<'a> {
    type Item = Result<Token, FilterParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.consume_whitespace();
        if self.is_eof() {
            return None;
        }

        let ch = self.peek_char()?;
        let token = if is_identifier_start(ch) {
            self.consume_identifier()
        } else if ch == '"' || ch == '\'' {
            self.consume_string()
        } else if ch.is_ascii_digit() || ch == '-' {
            self.consume_number()
        } else {
            match ch {
                '(' => {
                    self.advance();
                    Ok(Token::LParen)
                }
                ')' => {
                    self.advance();
                    Ok(Token::RParen)
                }
                '[' => {
                    self.advance();
                    Ok(Token::LBracket)
                }
                ']' => {
                    self.advance();
                    Ok(Token::RBracket)
                }
                ',' => {
                    self.advance();
                    Ok(Token::Comma)
                }
                '=' => {
                    self.advance();
                    Ok(Token::Operator(Operator::Eq))
                }
                '!' => {
                    self.advance();
                    if self.peek_char() == Some('=') {
                        self.advance();
                        Ok(Token::Operator(Operator::Neq))
                    } else {
                        Err(FilterParseError::Message("expected '=' after '!'".into()))
                    }
                }
                '>' => {
                    self.advance();
                    Ok(Token::Operator(Operator::Gt))
                }
                '<' => {
                    self.advance();
                    Ok(Token::Operator(Operator::Lt))
                }
                _ => Err(FilterParseError::Message(format!(
                    "unexpected character '{}'",
                    ch
                ))),
            }
        };

        Some(token)
    }
}

impl<'a> Lexer<'a> {
    fn consume_whitespace(&mut self) {
        while let Some(ch) = self.peek_char() {
            if ch.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    fn consume_identifier(&mut self) -> Result<Token, FilterParseError> {
        let start = self.position;
        self.advance();
        while let Some(ch) = self.peek_char() {
            if is_identifier_part(ch) {
                self.advance();
            } else {
                break;
            }
        }
        let ident = &self.input[start..self.position];
        let upper = ident.to_ascii_uppercase();
        match upper.as_str() {
            "AND" => Ok(Token::And),
            "OR" => Ok(Token::Or),
            "NOT" => Ok(Token::Not),
            "IN" => Ok(Token::Operator(Operator::In)),
            "LIKE" => Ok(Token::Operator(Operator::Like)),
            "TRUE" => Ok(Token::Boolean(true)),
            "FALSE" => Ok(Token::Boolean(false)),
            "NULL" => Ok(Token::Null),
            _ => Ok(Token::Identifier(ident.to_string())),
        }
    }

    fn consume_string(&mut self) -> Result<Token, FilterParseError> {
        let quote = self.next_char().unwrap();
        let mut value = String::new();
        while let Some(ch) = self.next_char() {
            if ch == quote {
                return Ok(Token::String(value));
            }
            if ch == '\\' {
                if let Some(escaped) = self.next_char() {
                    value.push(escaped);
                } else {
                    return Err(FilterParseError::Message(
                        "unterminated escape sequence".into(),
                    ));
                }
            } else {
                value.push(ch);
            }
        }
        Err(FilterParseError::Message(
            "unterminated string literal".into(),
        ))
    }

    fn consume_number(&mut self) -> Result<Token, FilterParseError> {
        let start = self.position;
        self.advance();
        while let Some(ch) = self.peek_char() {
            if ch.is_ascii_digit() || ch == '.' {
                self.advance();
            } else {
                break;
            }
        }
        Ok(Token::Number(self.input[start..self.position].to_string()))
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.position..].chars().next()
    }

    fn next_char(&mut self) -> Option<char> {
        let ch = self.peek_char()?;
        self.position += ch.len_utf8();
        Some(ch)
    }

    fn advance(&mut self) {
        self.next_char();
    }

    fn is_eof(&self) -> bool {
        self.position >= self.input.len()
    }
}

fn is_identifier_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_' || ch == '$'
}

fn is_identifier_part(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == '.' || ch == '$'
}

struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            position: 0,
        }
    }

    fn parse_expression(&mut self) -> Result<FilterExpr, FilterParseError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<FilterExpr, FilterParseError> {
        let mut expr = self.parse_and()?;
        while self.match_token(TokenKind::Or) {
            self.advance();
            let right = self.parse_and()?;
            expr = match expr {
                FilterExpr::Or(mut nodes) => {
                    nodes.push(right);
                    FilterExpr::Or(nodes)
                }
                _ => FilterExpr::Or(vec![expr, right]),
            };
        }
        Ok(expr)
    }

    fn parse_and(&mut self) -> Result<FilterExpr, FilterParseError> {
        let mut expr = self.parse_not()?;
        while self.match_token(TokenKind::And) {
            self.advance();
            let right = self.parse_not()?;
            expr = match expr {
                FilterExpr::And(mut nodes) => {
                    nodes.push(right);
                    FilterExpr::And(nodes)
                }
                _ => FilterExpr::And(vec![expr, right]),
            };
        }
        Ok(expr)
    }

    fn parse_not(&mut self) -> Result<FilterExpr, FilterParseError> {
        if self.match_token(TokenKind::Not) {
            self.advance();
            let expr = self.parse_not()?;
            Ok(FilterExpr::Not(Box::new(expr)))
        } else {
            self.parse_primary()
        }
    }

    fn parse_primary(&mut self) -> Result<FilterExpr, FilterParseError> {
        if self.match_token(TokenKind::LParen) {
            self.advance();
            let expr = self.parse_expression()?;
            self.expect(TokenKind::RParen, "')'")?;
            Ok(expr)
        } else {
            self.parse_comparison()
        }
    }

    fn parse_comparison(&mut self) -> Result<FilterExpr, FilterParseError> {
        let field = match self.current_token() {
            Some(Token::Identifier(name)) => {
                let value = name.clone();
                self.advance();
                value
            }
            _ => {
                return Err(FilterParseError::Message(
                    "expected field name in comparison".into(),
                ));
            }
        };

        let mut negate = false;
        if self.match_token(TokenKind::Not) {
            self.advance();
            negate = true;
        }

        let operator = match self.current_token() {
            Some(Token::Operator(operator)) => *operator,
            _ => {
                return Err(FilterParseError::Message(
                    "expected comparison operator".into(),
                ));
            }
        };
        self.advance();

        let expr = match operator {
            Operator::In => {
                self.expect(TokenKind::LBracket, "'['")?;
                let mut values = Vec::new();
                while !self.match_token(TokenKind::RBracket) {
                    values.push(self.parse_value()?);
                    if self.match_token(TokenKind::Comma) {
                        self.advance();
                    } else {
                        break;
                    }
                }
                self.expect(TokenKind::RBracket, "']'")?;
                FilterExpr::Comparison {
                    field,
                    op: ComparisonOp::In(values),
                }
            }
            Operator::Like => {
                let value = self.parse_value()?;
                FilterExpr::Comparison {
                    field,
                    op: ComparisonOp::Like(value),
                }
            }
            Operator::Eq => {
                let value = self.parse_value()?;
                FilterExpr::Comparison {
                    field,
                    op: ComparisonOp::Equals(value),
                }
            }
            Operator::Neq => {
                let value = self.parse_value()?;
                FilterExpr::Comparison {
                    field,
                    op: ComparisonOp::NotEquals(value),
                }
            }
            Operator::Gt => {
                let value = self.parse_value()?;
                FilterExpr::Comparison {
                    field,
                    op: ComparisonOp::GreaterThan(value),
                }
            }
            Operator::Lt => {
                let value = self.parse_value()?;
                FilterExpr::Comparison {
                    field,
                    op: ComparisonOp::LessThan(value),
                }
            }
        };

        if negate {
            Ok(FilterExpr::Not(Box::new(expr)))
        } else {
            Ok(expr)
        }
    }

    fn parse_value(&mut self) -> Result<FilterValue, FilterParseError> {
        let token = self
            .current_token()
            .ok_or_else(|| FilterParseError::Message("unexpected end of filter".into()))?
            .clone();
        self.advance();
        match token {
            Token::String(value) => Ok(FilterValue::String(value)),
            Token::Number(value) => value
                .parse::<f64>()
                .map(FilterValue::Number)
                .map_err(|err| FilterParseError::Message(format!("invalid number: {err}"))),
            Token::Boolean(value) => Ok(FilterValue::Bool(value)),
            Token::Null => Ok(FilterValue::Null),
            Token::Identifier(value) => Ok(FilterValue::String(value)),
            _ => Err(FilterParseError::Message("invalid value literal".into())),
        }
    }

    fn expect(&mut self, kind: TokenKind, label: &str) -> Result<(), FilterParseError> {
        if self.match_token(kind) {
            self.advance();
            Ok(())
        } else {
            Err(FilterParseError::Message(format!("expected {label}")))
        }
    }

    fn expect_end(&self) -> Result<(), FilterParseError> {
        if self.position >= self.tokens.len() {
            Ok(())
        } else {
            Err(FilterParseError::Message(
                "unexpected tokens after expression".into(),
            ))
        }
    }

    fn current_token(&self) -> Option<&Token> {
        self.tokens.get(self.position)
    }

    fn advance(&mut self) {
        if self.position < self.tokens.len() {
            self.position += 1;
        }
    }

    fn match_token(&self, kind: TokenKind) -> bool {
        matches!(
            self.current_token(),
            Some(token) if TokenKind::from(token) == kind
        )
    }
}

#[derive(Clone, Copy, PartialEq)]
enum TokenKind {
    Identifier,
    String,
    Number,
    Boolean,
    Null,
    And,
    Or,
    Not,
    Operator,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
}

impl From<&Token> for TokenKind {
    fn from(token: &Token) -> Self {
        match token {
            Token::Identifier(_) => TokenKind::Identifier,
            Token::String(_) => TokenKind::String,
            Token::Number(_) => TokenKind::Number,
            Token::Boolean(_) => TokenKind::Boolean,
            Token::Null => TokenKind::Null,
            Token::And => TokenKind::And,
            Token::Or => TokenKind::Or,
            Token::Not => TokenKind::Not,
            Token::Operator(_) => TokenKind::Operator,
            Token::LParen => TokenKind::LParen,
            Token::RParen => TokenKind::RParen,
            Token::LBracket => TokenKind::LBracket,
            Token::RBracket => TokenKind::RBracket,
            Token::Comma => TokenKind::Comma,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn sample_aggregate() -> AggregateState {
        let mut state = BTreeMap::new();
        state.insert("status".to_string(), "\"pending\"".to_string());
        state.insert("score".to_string(), "42".to_string());
        state.insert(
            "name".to_string(),
            r#"{"first":"John","last":"Thach"}"#.to_string(),
        );

        AggregateState {
            aggregate_type: "order".to_string(),
            aggregate_id: "order-123".to_string(),
            version: 7,
            state,
            merkle_root: "root".to_string(),
            archived: false,
        }
    }

    #[test]
    fn parse_shorthand_handles_basic_comparison() {
        let expr = parse_shorthand(r#"last_name = "thach""#).expect("parse succeeds");
        match expr {
            FilterExpr::Comparison { field, op } => {
                assert_eq!(field, "last_name");
                match op {
                    ComparisonOp::Equals(FilterValue::String(value)) => {
                        assert_eq!(value, "thach");
                    }
                    _ => panic!("unexpected comparison operator"),
                }
            }
            _ => panic!("expected comparison expression"),
        }
    }

    #[test]
    fn parse_shorthand_supports_nested_logic() {
        let expr = parse_shorthand(
            r#"(status = "pending" OR status = "paid") AND archived = false"#,
        )
        .expect("parse succeeds");

        let FilterExpr::And(parts) = expr else {
            panic!("expected AND expression");
        };
        assert_eq!(parts.len(), 2);

        match &parts[0] {
            FilterExpr::Or(branches) => {
                assert_eq!(branches.len(), 2);
                match &branches[0] {
                    FilterExpr::Comparison { field, op } => {
                        assert_eq!(field, "status");
                        match op {
                            ComparisonOp::Equals(FilterValue::String(value)) => {
                                assert_eq!(value, "pending");
                            }
                            _ => panic!("expected equals comparison"),
                        }
                    }
                    _ => panic!("expected comparison branch"),
                }
            }
            _ => panic!("expected OR branch"),
        }

        match &parts[1] {
            FilterExpr::Comparison { field, op } => {
                assert_eq!(field, "archived");
                match op {
                    ComparisonOp::Equals(FilterValue::Bool(value)) => {
                        assert!(!value);
                    }
                    _ => panic!("expected boolean equals"),
                }
            }
            _ => panic!("expected comparison for second part"),
        }
    }

    #[test]
    fn parse_shorthand_rejects_invalid_syntax() {
        assert!(parse_shorthand("").is_err());
        assert!(parse_shorthand(r#"status = "pending" AND"#).is_err());
        assert!(parse_shorthand(r#"status @ "pending""#).is_err());
    }

    #[test]
    fn matches_aggregate_handles_string_comparison() {
        let aggregate = sample_aggregate();
        let expr = parse_shorthand(r#"status = "pending""#).expect("parse succeeds");
        assert!(expr.matches_aggregate(&aggregate));

        let expr = parse_shorthand(r#"status = "cancelled""#).expect("parse succeeds");
        assert!(!expr.matches_aggregate(&aggregate));
    }

    #[test]
    fn matches_aggregate_handles_in_and_like() {
        let aggregate = sample_aggregate();

        let expr =
            parse_shorthand(r#"status IN ["pending","paid"]"#).expect("parse succeeds");
        assert!(expr.matches_aggregate(&aggregate));

        let expr =
            parse_shorthand(r#"aggregate_id LIKE "order%""#).expect("parse succeeds");
        assert!(expr.matches_aggregate(&aggregate));
    }

    #[test]
    fn matches_aggregate_handles_numeric_and_bool_fields() {
        let aggregate = sample_aggregate();

        let expr = parse_shorthand(r#"score > 40"#).expect("parse succeeds");
        assert!(expr.matches_aggregate(&aggregate));

        let expr = parse_shorthand(r#"score < 40"#).expect("parse succeeds");
        assert!(!expr.matches_aggregate(&aggregate));

        let expr = parse_shorthand(r#"archived = false"#).expect("parse succeeds");
        assert!(expr.matches_aggregate(&aggregate));

        let expr = parse_shorthand(r#"NOT archived = true"#).expect("parse succeeds");
        assert!(expr.matches_aggregate(&aggregate));
    }

    #[test]
    fn matches_aggregate_handles_nested_fields() {
        let aggregate = sample_aggregate();
        let expr = parse_shorthand(r#"name.first = "John""#).expect("parse succeeds");
        assert!(expr.matches_aggregate(&aggregate));

        let expr = parse_shorthand(r#"name.first = "Jane""#).expect("parse succeeds");
        assert!(!expr.matches_aggregate(&aggregate));
    }
}
