@0xd5ec7c0aa378d5cb;

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

struct TenantAssignRequest {
  token @0 :Text;
  tenantId @1 :Text;
  shardId @2 :Text;
}

struct TenantAssignResponse {
  changed @0 :Bool;
  shardId @1 :Text;
}

struct TenantUnassignRequest {
  token @0 :Text;
  tenantId @1 :Text;
}

struct TenantUnassignResponse {
  changed @0 :Bool;
}

struct TenantQuotaSetRequest {
  token @0 :Text;
  tenantId @1 :Text;
  maxStorageMb @2 :UInt64;
}

struct TenantQuotaSetResponse {
  changed @0 :Bool;
  quotaMb @1 :UInt64;
  hasQuota @2 :Bool;
}

struct TenantQuotaClearRequest {
  token @0 :Text;
  tenantId @1 :Text;
}

struct TenantQuotaClearResponse {
  changed @0 :Bool;
}

struct TenantQuotaRecalcRequest {
  token @0 :Text;
  tenantId @1 :Text;
}

struct TenantQuotaRecalcResponse {
  storageBytes @0 :UInt64;
}

struct TenantReloadRequest {
  token @0 :Text;
  tenantId @1 :Text;
}

struct TenantReloadResponse {
  reloaded @0 :Bool;
}

struct TenantSchemaPublishRequest {
  token @0 :Text;
  tenantId @1 :Text;
  reason @2 :Text;
  hasReason @3 :Bool;
  actor @4 :Text;
  hasActor @5 :Bool;
  labels @6 :List(Text);
  activate @7 :Bool;
  force @8 :Bool;
  reload @9 :Bool;
}

struct TenantSchemaPublishResponse {
  versionId @0 :Text;
  activated @1 :Bool;
  skipped @2 :Bool;
}
