import json
import os
import json
import asyncio
from modules.builder import Builder
from grpclib.client import Channel
# from modules.alerts import AlertServiceStub
from modules.alerts.v3 import AlertDefsServiceStub, ListAlertDefsRequest
from modules.outgoing_webhooks.v1 import OutgoingWebhooksServiceStub


class Main:
    def __init__(self, init_obj: Builder):
        self.session_token = init_obj.session_token
        self.company_id = init_obj.company_id
        self.endpoint = init_obj.endpoint
        self.code_dir = init_obj.code_dir
        self.sb_logger = init_obj.sb_logger
        self.extend_output = init_obj.extend_output
        self.metadata = init_obj.metadata
        self.cx_region = init_obj.cx_region
        self.grpc_config = init_obj.grpc_config

    async def get_raw_output(self, grpc_query):
        channel = Channel(host=self.endpoint, port=443, ssl=True, config=self.grpc_config)
        try:
            if grpc_query == "ListOutgoingWebhookTypes":
                stub = OutgoingWebhooksServiceStub(channel)
                request = await stub.list_outgoing_webhook_types(metadata=self.metadata)
                response = request.to_pydict()
                return response
            elif grpc_query == "ListAlertDefs":
                stub = AlertDefsServiceStub(channel)
                list_alert_defs_request = ListAlertDefsRequest()
                request = await stub.list_alert_defs(
                    list_alert_defs_request=list_alert_defs_request,
                    metadata=self.metadata)
                response = request.to_dict().get("alertDefs")
                return response

        finally:
            channel.close()

    def webhooks_filter_results(self):
        raw_webhooks = None
        webhook_error = None
        try:
            raw_webhooks = asyncio.run(self.get_raw_output("ListOutgoingWebhookTypes"))
        except Exception as e:
            webhook_error = str(e)
            self.sb_logger.error(f"Failed to get a response from Coralogix - {e}")

        webhooks = []
        counter = 0
        if raw_webhooks and "webhooks" in raw_webhooks:
            for webhook in raw_webhooks["webhooks"]:
                if "count" in webhook:
                    counter += webhook["count"]
                    webhook["connections_count"] = webhook["count"]
                    del webhook["count"]
                    del webhook["type"]
                    webhooks.append(webhook)
        result = {"outbound_webhooks": {"amount": counter, "details": webhooks}}
        if webhook_error:
            result["webhook_error"] = {"status": "FAILED", "error": webhook_error}
        return result

    def check_alerts_for_webhooks(self):
        # FUTURE OPTION - currently the response doesn't include the webhooks assignments
        raw_alerts = asyncio.run(self.get_raw_output("ListAlertDefs"))
        return raw_alerts

    def run_check(self):
        fin_results = self.webhooks_filter_results()
        output_dir = os.path.join(self.code_dir, "output")

        with open(os.path.join(output_dir, "output.json"), "a") as file:
            file.write(json.dumps(fin_results, indent=2, default=str) + "\n")
            self.sb_logger.element_info("Webhooks check completed")
