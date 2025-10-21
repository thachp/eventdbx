@0xdf2749081f1a2e2f;

struct ReplicationHello {
  protocolVersion @0 :UInt16;
  expectedPublicKey @1 :Data;
}

struct ReplicationHelloResponse {
  accepted @0 :Bool;
  message @1 :Text;
}

struct AggregatePosition {
  aggregateType @0 :Text;
  aggregateId @1 :Text;
  version @2 :UInt64;
}

struct PullEventsRequest {
  aggregateType @0 :Text;
  aggregateId @1 :Text;
  fromVersion @2 :UInt64;
  limit @3 :UInt32;
}

struct EventRecord {
  aggregateType @0 :Text;
  aggregateId @1 :Text;
  eventType @2 :Text;
  version @3 :UInt64;
  payload @4 :Data;
  metadata @5 :Data;
  merkleRoot @6 :Text;
  hash @7 :Text;
}

struct ApplyEventsRequest {
  sequence @0 :UInt64;
  events @1 :List(EventRecord);
}

struct ApplyEventsResponse {
  appliedSequence @0 :UInt64;
}

struct ApplySchemasRequest {
  schemasJson @0 :Data;
}

struct ApplySchemasResponse {
  aggregateCount @0 :UInt32;
}

struct PullSchemasResponse {
  schemasJson @0 :Data;
}

struct ListPositionsResponse {
  positions @0 :List(AggregatePosition);
}

struct PullEventsResponse {
  events @0 :List(EventRecord);
}

struct ErrorResponse {
  message @0 :Text;
}

struct ReplicationRequest {
  union {
    listPositions @0 :Void;
    pullEvents @1 :PullEventsRequest;
    applyEvents @2 :ApplyEventsRequest;
    pullSchemas @3 :Void;
    applySchemas @4 :ApplySchemasRequest;
  }
}

struct ReplicationResponse {
  union {
    ok @0 :Void;
    listPositions @1 :ListPositionsResponse;
    pullEvents @2 :PullEventsResponse;
    applyEvents @3 :ApplyEventsResponse;
    pullSchemas @4 :PullSchemasResponse;
    applySchemas @5 :ApplySchemasResponse;
    error @6 :ErrorResponse;
  }
}
