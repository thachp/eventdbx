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
    setAggregateArchive @9 :SetAggregateArchiveRequest;
    listSchemas @10 :ListSchemasRequest;
    replaceSchemas @11 :ReplaceSchemasRequest;
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
    setAggregateArchive @9 :SetAggregateArchiveResponse;
    listSchemas @10 :ListSchemasResponse;
    replaceSchemas @11 :ReplaceSchemasResponse;
  }
}

struct ControlHello {
  protocolVersion @0 :UInt16;
  token @1 :Text;
}

struct ControlHelloResponse {
  accepted @0 :Bool;
  message @1 :Text;
}

struct ListAggregatesRequest {
  cursor @0 :Text;
  hasCursor @1 :Bool;
  take @2 :UInt64;
  hasTake @3 :Bool;
  filter @4 :Text;
  hasFilter @5 :Bool;
  sort @6 :List(AggregateSort);
  hasSort @7 :Bool;
  includeArchived @8 :Bool;
  archivedOnly @9 :Bool;
  token @10 :Text;
}

struct ListAggregatesResponse {
  aggregatesJson @0 :Text;
  nextCursor @1 :Text;
  hasNextCursor @2 :Bool;
}

struct GetAggregateRequest {
  aggregateType @0 :Text;
  aggregateId @1 :Text;
  token @2 :Text;
}

struct GetAggregateResponse {
  found @0 :Bool;
  aggregateJson @1 :Text;
}

struct ListEventsRequest {
  aggregateType @0 :Text;
  aggregateId @1 :Text;
  cursor @2 :Text;
  hasCursor @3 :Bool;
  take @4 :UInt64;
  hasTake @5 :Bool;
  filter @6 :Text;
  hasFilter @7 :Bool;
  token @8 :Text;
}

struct ListEventsResponse {
  eventsJson @0 :Text;
  nextCursor @1 :Text;
  hasNextCursor @2 :Bool;
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
  token @3 :Text;
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

struct SetAggregateArchiveRequest {
  token @0 :Text;
  aggregateType @1 :Text;
  aggregateId @2 :Text;
  archived @3 :Bool;
  comment @4 :Text;
  hasComment @5 :Bool;
}

struct SetAggregateArchiveResponse {
  aggregateJson @0 :Text;
}

struct ListSchemasRequest {
  token @0 :Text;
}

struct ListSchemasResponse {
  schemasJson @0 :Text;
}

struct ReplaceSchemasRequest {
  token @0 :Text;
  schemasJson @1 :Text;
}

struct ReplaceSchemasResponse {
  replaced @0 :UInt32;
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

struct ControlError {
  code @0 :Text;
  message @1 :Text;
}
