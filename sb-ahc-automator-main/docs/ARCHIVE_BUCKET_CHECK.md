# Archive Bucket Check

## Purpose

Verifies that logs and metrics archive buckets are configured in Coralogix.

## APIs Used

| Target | Method | Endpoint |
|--------|--------|----------|
| Logs | gRPC | `TargetService/GetTarget` (archive.v2) |
| Metrics | REST | `GET /api/v1/metricsArchiveConfig` |

## Logic

- **Logs:** gRPC TargetService/GetTarget. Pass if response contains `s3` key.
- **Metrics:** REST API `GET /api/v1/metricsArchiveConfig`. Response: `{"Ok": {"bucket_name": "...", "region": "..."}}` when configured, `{"Ok": null}` when not. Auth: cx_api_key or session_token/company_id.

## Output

```json
{
  "archive_buckets": {
    "logs": { "active": true, "bucket": "...", "region": "...", "format": "..." },
    "metrics": { "active": true, "bucket": "...", "region": "..." }
  }
}
```

With `extend_output: false`, only `active` is included.

## Config

- `extend_output` — When true, adds bucket, region, format for logs; bucket, region for metrics.
- `grpc_query_map` — Maps query names to gRPC service paths.

## Fine-tuning

- **Primary path:** `_get_metrics_via_grpcurl()` uses `com.coralogix.archive.v1.MetricsConfigureService/GetTarget` — avoids betterproto oneof parsing issues on Lambda.
- **Fallback path:** `_get_metrics_via_get_tenant_config()` uses GetTenantConfig (betterproto) if grpcurl fails.
- **Response parsing:** Bucket/region extracted from grpcurl `target.s3` or `s3`; from GetTenantConfig: `tenantConfig.s3`, `storageConfig.s3`, or `bucketName`/`bucket_name`.
