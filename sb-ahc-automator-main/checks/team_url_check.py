"""
Team URL check.
Calls REST GET /api/v1/user/team, finds the entry matching company_id,
and writes the team URL right after the header line in output.yaml.
On failure: log reason to console and continue; do not fail the run.
"""
import os
import json
import requests
from modules.builder import Builder
from modules.region_config import get_api_host, get_team_domain


class Main:
    def __init__(self, init_obj: Builder):
        self.session_token = init_obj.session_token
        self.company_id = init_obj.company_id
        self.code_dir = init_obj.code_dir
        self.sb_logger = init_obj.sb_logger
        self.cx_region = (init_obj.cx_region or "").strip().lower() or "eu1"

    def get_team_url(self):
        host = get_api_host(self.cx_region)
        domain = get_team_domain(self.cx_region)
        url = f"https://{host}/api/v1/user/team"
        headers = {"Authorization": f"Bearer {self.session_token}/{self.company_id}"}
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        teams = resp.json().get("data", [])
        for team in teams:
            if str(team.get("id")) == str(self.company_id):
                team_url_slug = team.get("team_url") or team.get("teamUrl") or team.get("name")
                return f"https://{team_url_slug}.{domain}/"
        return None

    def run_check(self):
        try:
            team_url = self.get_team_url()
        except Exception as e:
            self.sb_logger.warning(f"Team URL check failed: {e}")
            team_url = None

        result = {"team_url": team_url}
        output_dir = os.path.join(self.code_dir, "output")
        with open(os.path.join(output_dir, "output.json"), "a") as f:
            f.write(json.dumps(result, indent=2, default=str) + "\n")
        self.sb_logger.element_info("Team URL check completed")
