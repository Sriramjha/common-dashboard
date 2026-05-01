# Dashboard Folders Check

## Purpose

Counts dashboards in folders vs not in any folder.

## APIs Used

| Method | Service |
|--------|---------|
| grpcurl | `DashboardCatalogService/GetDashboardCatalog` |

## Logic

- In folder if `folder.id` is present; else `not_in_folder`.

## Output

```json
{
  "dashboards": {
    "total": 25,
    "in_folder": 20,
    "not_in_folder": 5,
    "not_in_folder_names": ["Dashboard A", "Dashboard B", ...]
  }
}
```

## Fine-tuning

- Folder structure parsing if API changes.
