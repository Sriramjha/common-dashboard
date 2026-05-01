# Enrichments Check

## Purpose

Lists enrichments by type (geo, security) and checks for required fields.

## APIs Used

| Method | Service |
|--------|---------|
| gRPC | `EnrichmentServiceStub.get_enrichments` |

## Logic

- Classifies enrichments by `enrichmentType.geoIp` or `suspiciousIp`.
- Sets `geo_cx_security_source_ip` and `geo_cx_security_destination_ip` based on field presence.

## Output

```json
{
  "enrichments": {
    "geo": [...],
    "security": [...],
    "geo_cx_security_source_ip": true,
    "geo_cx_security_destination_ip": true
  }
}
```

## Fine-tuning

- Field names in enrichment type checks.
- Add new enrichment categories as needed.
