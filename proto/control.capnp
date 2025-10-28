@0xc3d1ec2a1e3f5b26;

struct ControlRequest {
  id @0 :UInt64;
  payload :union {
    listAggregates @1 :ListAggregatesRequest;
    getAggregate @2 :GetAggregateRequest;
    listEvents @3 :ListEventsRequest;
    appendEvent @4 :AppendEventRequest;
    verifyAggregate @5 :VerifyAggregateRequest;
    patchEvent @6 :PatchEventRequest;
    selectAggregate @7 :SelectAggregateRequest;
    createAggregate @8 :CreateAggregateRequest;
  }
}

struct ControlResponse {
  id @0 :UInt64;
  payload :union {
    listAggregates @1 :ListAggregatesResponse;
    getAggregate @2 :GetAggregateResponse;
    listEvents @3 :ListEventsResponse;
    appendEvent @4 :AppendEventResponse;
    verifyAggregate @5 :VerifyAggregateResponse;
    selectAggregate @6 :SelectAggregateResponse;
    error @7 :ControlError;
    createAggregate @8 :CreateAggregateResponse;
  }
}

struct ListAggregatesRequest {
  skip @0 :UInt64;
  take @1 :UInt64;
  hasTake @2 :Bool;
  filter @3 :FilterExpression;
  hasFilter @4 :Bool;
  sort @5 :List(AggregateSort);
  hasSort @6 :Bool;
  includeArchived @7 :Bool;
  archivedOnly @8 :Bool;
}

struct ListAggregatesResponse {
  aggregatesJson @0 :Text;
}

struct GetAggregateRequest {
  aggregateType @0 :Text;
  aggregateId @1 :Text;
}

struct GetAggregateResponse {
  found @0 :Bool;
  aggregateJson @1 :Text;
}

struct ListEventsRequest {
  aggregateType @0 :Text;
  aggregateId @1 :Text;
  skip @2 :UInt64;
  take @3 :UInt64;
  hasTake @4 :Bool;
  filter @5 :FilterExpression;
  hasFilter @6 :Bool;
}

struct ListEventsResponse {
  eventsJson @0 :Text;
}

struct AppendEventRequest {
  token @0 :Text;
  aggregateType @1 :Text;
  aggregateId @2 :Text;
  eventType @3 :Text;
  payloadJson @4 :Text;
  note @5 :Text;
  hasNote @6 :Bool;
  metadataJson @7 :Text;
  hasMetadata @8 :Bool;
  requireExisting @9 :Bool;
}

struct AppendEventResponse {
  eventJson @0 :Text;
}

struct PatchEventRequest {
  token @0 :Text;
  aggregateType @1 :Text;
  aggregateId @2 :Text;
  eventType @3 :Text;
  patchJson @4 :Text;
  note @5 :Text;
  hasNote @6 :Bool;
  metadataJson @7 :Text;
  hasMetadata @8 :Bool;
}

struct VerifyAggregateRequest {
  aggregateType @0 :Text;
  aggregateId @1 :Text;
}

struct VerifyAggregateResponse {
  merkleRoot @0 :Text;
}

struct SelectAggregateRequest {
  aggregateType @0 :Text;
  aggregateId @1 :Text;
  fields @2 :List(Text);
}

struct SelectAggregateResponse {
  found @0 :Bool;
  selectionJson @1 :Text;
}

struct CreateAggregateRequest {
  token @0 :Text;
  aggregateType @1 :Text;
  aggregateId @2 :Text;
  eventType @3 :Text;
  payloadJson @4 :Text;
  note @5 :Text;
  hasNote @6 :Bool;
  metadataJson @7 :Text;
  hasMetadata @8 :Bool;
}

struct CreateAggregateResponse {
  aggregateJson @0 :Text;
}

struct AggregateSort {
  field @0 :AggregateSortField;
  descending @1 :Bool;
}

enum AggregateSortField {
  aggregateType @0;
  aggregateId @1;
  version @2;
  merkleRoot @3;
  archived @4;
}

struct FilterExpression {
  union {
    logical @0 :LogicalExpression;
    comparison @1 :ComparisonExpression;
  }
}

struct LogicalExpression {
  union {
    and @0 :List(FilterExpression);
    or @1 :List(FilterExpression);
    not @2 :FilterExpression;
  }
}

struct ComparisonExpression {
  union {
    equals @0 :Comparison;
    notEquals @1 :Comparison;
    greaterThan @2 :Comparison;
    lessThan @3 :Comparison;
    inSet @4 :SetComparison;
    like @5 :Comparison;
  }
}

struct Comparison {
  field @0 :Text;
  value @1 :Text;
}

struct SetComparison {
  field @0 :Text;
  values @1 :List(Text);
}

struct ControlError {
  code @0 :Text;
  message @1 :Text;
}
