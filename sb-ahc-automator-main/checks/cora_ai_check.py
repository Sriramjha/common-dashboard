"""
Cora AI check — reads the 4 AI toggle settings via GenAiService/Settings (gRPC).

Toggle 1 — "Olly & CORA" (master AI toggle)    → coralogixEnabled
Toggle 2 — "DataPrime Query Assistance"         → queryAssistantEnabled
Toggle 3 — "Explain log"                        → explainLogEnabled
Toggle 4 — "Knowledge Assistance"               → platformAwarenessEnabled

On failure: log reason to console and continue; do not fail the run.
"""
import os
import json
import subprocess
import json
from modules.builder import Builder


class Main:
    def __init__(self, init_obj: Builder):
        self.session_token = init_obj.session_token
        self.company_id = init_obj.company_id
        self.endpoint = init_obj.endpoint
        self.code_dir = init_obj.code_dir
        self.sb_logger = init_obj.sb_logger
        self.grpcurl_path = getattr(init_obj, 'grpcurl_path', 'grpcurl') or 'grpcurl'

    def get_ai_settings(self):
        params = [
            self.grpcurl_path,
            "-H", f"Authorization: Bearer {self.session_token}/{self.company_id}",
            "-d", "{}",
            f"{self.endpoint}:443",
            "com.coralogix.genai.v1.GenAiService/Settings",
        ]
        resp = subprocess.run(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout = resp.stdout.decode("utf-8").strip()
        stderr = resp.stderr.decode("utf-8").strip()
        if not stdout:
            raise RuntimeError(f"grpcurl error: {stderr}")
        return json.loads(stdout)

    def run_check(self):
        try:
            ai = self.get_ai_settings()
            result = {
                "cora_ai": {
                    "dataprime_query_assistance_enabled": ai.get("queryAssistantEnabled", False),
                    "explain_log_enabled": ai.get("explainLogEnabled", False),
                    "knowledge_assistance_enabled": ai.get("platformAwarenessEnabled", False),
                }
            }
        except Exception as e:
            self.sb_logger.warning(f"Cora AI check failed: {e}")
            result = {
                "cora_ai": {
                    "dataprime_query_assistance_enabled": None,
                    "explain_log_enabled": None,
                    "knowledge_assistance_enabled": None,
                    "error": str(e),
                }
            }

        output_dir = os.path.join(self.code_dir, "output")
        with open(os.path.join(output_dir, "output.json"), "a") as f:
            f.write(json.dumps(result, indent=2, default=str) + "\n")
        self.sb_logger.element_info("Cora AI check completed")
