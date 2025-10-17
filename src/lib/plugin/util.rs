pub fn sanitize_identifier(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    for (index, ch) in input.chars().enumerate() {
        if ch.is_ascii_alphanumeric() {
            if index == 0 && ch.is_ascii_digit() {
                result.push('_');
            }
            result.push(ch.to_ascii_lowercase());
        } else {
            result.push('_');
        }
    }
    if result.is_empty() {
        "_".into()
    } else {
        result
    }
}

pub fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}
