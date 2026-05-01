# Default Dashboard Check

## Purpose

Finds the dashboard marked as default in the catalog.

## APIs Used

| Method | Service |
|--------|---------|
| grpcurl | `DashboardCatalogService/GetDashboardCatalog` |

## Logic

- Returns the first item with `isDefault: true`.

## Output

```json
{
  "default_dashboard": "Dashboard Name"
}
```

Or `null` if no default is set.
