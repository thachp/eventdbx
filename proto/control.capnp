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
  }
}

struct ListAggregatesRequest {
  skip @0 :UInt64;
  take @1 :UInt64;
  hasTake @2 :Bool;
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
  patchJson @5 :Text;
  note @6 :Text;
  hasNote @7 :Bool;
  metadataJson @8 :Text;
  hasMetadata @9 :Bool;
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

struct ControlError {
  code @0 :Text;
  message @1 :Text;
}
