# Extensions Check

## Purpose

Lists deployed extensions, compares versions to latest, and reports security extensions status.

## APIs Used

| Method | Service |
|--------|---------|
| gRPC | `ExtensionDeploymentServiceStub.get_deployed_extensions` |
| grpcurl | `ExtensionService/GetAllExtensions` (max-msg-sz 50MB) |

## Logic

- Fetches deployed extensions and all available extensions.
- Compares versions: `update_available` if latest > deployed; `updated` otherwise.
- Security extensions identified via `SECURITY_EXTENSION_IDS` constant.

## Output

```json
{
  "extensions": {
    "amount": 5,
    "updated": ["Extension A", "Extension B"],
    "update_available": ["Extension C"]
  },
  "security_extensions": {
    "extension-id-1": true,
    "extension-id-2": false
  }
}
```

## Config

- `SECURITY_EXTENSION_IDS` — List of extension IDs considered security-related.
- `CORALOGIX_SYSTEM_IDS` — System extensions (consolidated in output).
- `MAX_MSG_BYTES` — gRPC message size limit (default 50MB).

## Fine-tuning

- Add/remove IDs in `SECURITY_EXTENSION_IDS`.
- Adjust `_compare_versions()` for custom version comparison logic.
