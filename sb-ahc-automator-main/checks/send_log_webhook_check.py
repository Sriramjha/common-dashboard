"""
Send Log Webhook check.

Checks whether at least one SEND_LOG webhook has been configured.

Output:
  send_log_webhook_created: true   ← if count > 0
  send_log_webhook_created: false  ← if count == 0 or type absent
"""
import asyncio
import os

import json

from grpclib.client import Channel
from modules.builder import Builder
from modules.outgoing_webhooks.v1 import OutgoingWebhooksServiceStub


class Main:
    def __init__(self, init_obj: Builder):
        self.session_token = init_obj.session_token
        self.company_id = init_obj.company_id
        self.endpoint = init_obj.endpoint
        self.code_dir = init_obj.code_dir
        self.sb_logger = init_obj.sb_logger
        self.metadata = init_obj.metadata
        self.grpc_config = init_obj.grpc_config

    async def _get_send_log_created(self) -> bool:
        channel = Channel(host=self.endpoint, port=443, ssl=True, config=self.grpc_config)
        try:
            stub = OutgoingWebhooksServiceStub(channel)
            response = await stub.list_outgoing_webhook_types(metadata=self.metadata)
            for webhook in response.to_pydict().get("webhooks", []):
                wtype = webhook.get("type")
                label = webhook.get("label", "")
                # to_pydict() returns type as integer enum (4 = SEND_LOG)
                if wtype == 4 or wtype == "SEND_LOG" or label == "Send log":
                    return int(webhook.get("count", 0)) > 0
            return False
        finally:
            channel.close()

    def run_check(self):
        try:
            send_log_created = asyncio.run(self._get_send_log_created())
            result = {"send_log_webhook_created": send_log_created}
        except Exception as e:
            self.sb_logger.warning(f"Send log webhook check failed: {e}")
            result = {"send_log_webhook_created": None, "error": str(e)}

        output_dir = os.path.join(self.code_dir, "output")
        with open(os.path.join(output_dir, "output.json"), "a") as f:
            f.write(json.dumps(result, indent=2, default=str) + "\n")
        self.sb_logger.element_info("Send log webhook check completed")
