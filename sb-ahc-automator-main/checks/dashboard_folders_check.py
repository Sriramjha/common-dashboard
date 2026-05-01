"""
Dashboard folders check.

One gRPC call:
  com.coralogixapis.dashboards.v1.services.DashboardCatalogService/GetDashboardCatalog

Classifies every custom dashboard as:
  in_folder     — dashboard.folder.id is present and non-empty
  not_in_folder — folder is absent or has no id (dashboard sits at root)

Output (output.json):
  "dashboards": {
    "total": 47,
    "in_folder": 43,
    "not_in_folder": 4,
    "not_in_folder_names": ["AWS Non Human Identities", ...]
  }
"""
import json
import os
import subprocess

from modules.builder import Builder


class Main:
    def __init__(self, init_obj: Builder):
        self.session_token = init_obj.session_token
        self.company_id    = init_obj.company_id
        self.endpoint      = init_obj.endpoint
        self.sb_logger     = init_obj.sb_logger
        self.code_dir      = init_obj.code_dir
        self.grpcurl_path  = getattr(init_obj, 'grpcurl_path', 'grpcurl') or 'grpcurl'

    def run_check(self):
        result = {
            "dashboards": {
                "total":             0,
                "in_folder":         0,
                "not_in_folder":     0,
                "not_in_folder_names": [],
            }
        }

        params = [
            self.grpcurl_path,
            "-H", f"Authorization: Bearer {self.session_token}/{self.company_id}",
            "-d", "{}",
            f"{self.endpoint}:443",
            "com.coralogixapis.dashboards.v1.services.DashboardCatalogService/GetDashboardCatalog",
        ]

        grpc_failed = False
        try:
            resp = subprocess.run(
                params,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=30,
            )
            if resp.returncode != 0:
                err = resp.stderr.decode().strip()
                self.sb_logger.warning(f"Dashboard folders check: gRPC call failed — {err}")
                grpc_failed = True
            else:
                data  = json.loads(resp.stdout.decode())
                items = data.get("items", [])

                in_folder     = []
                not_in_folder = []

                for item in items:
                    folder = item.get("folder")
                    if folder and folder.get("id"):
                        in_folder.append(item.get("name", ""))
                    else:
                        not_in_folder.append(item.get("name", ""))

                result["dashboards"] = {
                    "total":               len(items),
                    "in_folder":           len(in_folder),
                    "not_in_folder":       len(not_in_folder),
                    "not_in_folder_names": not_in_folder,
                }

        except subprocess.TimeoutExpired:
            self.sb_logger.warning("Dashboard folders check: timed out")
            grpc_failed = True
        except Exception as e:
            self.sb_logger.warning(f"Dashboard folders check: {e}")
            grpc_failed = True

        output_dir = os.path.join(self.code_dir, "output")
        if grpc_failed:
            with open(os.path.join(output_dir, "output.json"), "a") as f:
                f.write(json.dumps({
                    "dashboard_folders_error": {"status": "FAILED", "error": "Could not fetch dashboards"},
                    "dashboards": result["dashboards"],
                }, indent=2, default=str) + "\n")
        else:
            with open(os.path.join(output_dir, "output.json"), "a") as f:
                f.write(json.dumps(result, indent=2, default=str) + "\n")
        self.sb_logger.element_info("Dashboard folders check completed")
