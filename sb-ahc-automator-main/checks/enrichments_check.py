import json
import asyncio
import os
import json
from modules.builder import Builder
from grpclib.client import Channel
from modules.enrichments import EnrichmentServiceStub


class Main:
    def __init__(self, init_obj: Builder):
        self.sb_logger = init_obj.sb_logger
        self.endpoint = init_obj.endpoint
        self.metadata = init_obj.metadata
        self.code_dir = init_obj.code_dir
        self.grpc_config = init_obj.grpc_config

    async def get_raw_output(self):
        channel = Channel(host=self.endpoint, port=443, ssl=True, config=self.grpc_config)
        try:
            stub = EnrichmentServiceStub(channel)
            request = await stub.get_enrichments(metadata=self.metadata)
            response = request.enrichments
            enrichments = []
            for enrichment in response:
                enrichments.append(json.loads(enrichment.to_json()))
            return enrichments
        except Exception as e:
            self.sb_logger.error(f"Failed to get a response from Coralogix - {e}")
            return None
        finally:
            channel.close()

    def filtered_results(self):
        raw_enrichments = asyncio.run(self.get_raw_output())
        if raw_enrichments is None:
            return None
        enrichments = {"geo": [], "security": []}
        for enrichment in raw_enrichments:
            enrichment_type = [k for k, v in enrichment["enrichmentType"].items() if k == "geoIp" or k == "suspiciousIp"]
            if enrichment_type:
                if enrichment_type[0] == "geoIp":
                    enrichments["geo"].append(enrichment["fieldName"])
                elif enrichment_type[0] == "suspiciousIp":
                    enrichments["security"].append(enrichment["fieldName"])

        geo_fields = enrichments["geo"]
        enrichments["geo_cx_security_source_ip"]      = "cx_security.source_ip"      in geo_fields
        enrichments["geo_cx_security_destination_ip"] = "cx_security.destination_ip" in geo_fields

        return {"enrichments": enrichments}

    def run_check(self):
        fin_results = self.filtered_results()
        output_dir = os.path.join(self.code_dir, "output")

        if fin_results is None:
            with open(os.path.join(output_dir, "output.json"), "a") as file:
                file.write(json.dumps({
                    "enrichments_error": {"status": "FAILED", "error": "Could not fetch enrichments"},
                }, indent=2, default=str) + "\n")
        else:
            with open(os.path.join(output_dir, "output.json"), "a") as file:
                file.write(json.dumps(fin_results, indent=2, default=str) + "\n")
        self.sb_logger.element_info("Enrichments check completed")
