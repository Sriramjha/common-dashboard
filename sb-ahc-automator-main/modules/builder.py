import os
from grpclib.config import Configuration

# Allow larger gRPC responses (default 4 MiB); Coralogix can return e.g. extension payloads > 10 MiB
GRPC_CHANNEL_CONFIG = Configuration(
    http2_connection_window_size=20 * 1024 * 1024,
    http2_stream_window_size=20 * 1024 * 1024,
)


def get_grpcurl_path(deployment_root: str = None) -> str:
    """Return path to grpcurl binary. In Lambda, use deployment root; else use PATH."""
    if deployment_root:
        path = os.path.join(deployment_root, 'grpcurl')
        if os.path.isfile(path) and os.access(path, os.X_OK):
            return path
    return 'grpcurl'  # Fallback to PATH


class Builder:
    def __init__(self, session_token, company_id, endpoint, metadata, sb_logger, extend_output, archive_bucket_configured, code_dir, cx_api_key, cx_region, mcp_checks=None, deployment_root=None):
        self.session_token = session_token
        self.grpc_config = GRPC_CHANNEL_CONFIG
        self.company_id = company_id
        self.endpoint = endpoint
        self.metadata = metadata
        self.grpc_query_map = {}
        self.sb_logger = sb_logger
        self.extend_output = extend_output
        self.archive_bucket_configured = archive_bucket_configured
        self.code_dir = code_dir
        self.cx_api_key = cx_api_key
        self.cx_region = cx_region
        self.mcp_checks = mcp_checks or []
        self.deployment_root = deployment_root
        self.grpcurl_path = get_grpcurl_path(deployment_root)
