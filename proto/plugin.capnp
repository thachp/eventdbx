@0xd4b23bfa4df1bcd9;

struct PluginEnvelope {
  message :union {
    init @0 :PluginInit;
    event @1 :PluginEvent;
    ack @2 :PluginAck;
    error @3 :PluginError;
  }
}

struct PluginInit {
  pluginName @0 :Text;
  version @1 :Text;
  target @2 :Text;
}

struct PluginEvent {
  sequence @0 :UInt64;
  aggregateType @1 :Text;
  aggregateId @2 :Text;
  eventType @3 :Text;
  eventVersion @4 :UInt64;
  eventId @5 :Text;
  createdAtEpochMicros @6 :Int64;
  payloadJson @7 :Text;
  metadataJson @8 :Text;
  hash @9 :Text;
  merkleRoot @10 :Text;
  stateVersion @11 :UInt64;
  stateArchived @12 :Bool;
  stateMerkleRoot @13 :Text;
  stateEntries @14 :List(StateEntry);
  schemaJson @15 :Text;
  extensionsJson @16 :Text;
}

struct StateEntry {
  key @0 :Text;
  value @1 :Text;
}

struct PluginAck {
  sequence @0 :UInt64;
}

struct PluginError {
  sequence @0 :UInt64;
  message @1 :Text;
}
