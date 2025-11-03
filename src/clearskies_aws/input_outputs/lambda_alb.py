from __future__ import annotations

from typing import Any

from clearskies.configs import String
from clearskies.input_outputs import Headers

from clearskies_aws.input_outputs import lambda_input_output


class LambdaAlb(lambda_input_output.LambdaInputOutput):
    """Application Load Balancer specific Lambda input/output handler."""

    def __init__(self, event: dict[str, Any], context: dict[str, Any]):
        # Call parent constructor
        super().__init__(event, context)

        # ALB specific initialization
        self.request_method = event.get("httpMethod", "GET").upper()
        self.path = event.get("path", "/")

        # Extract query parameters (ALB only has single value query parameters)
        self.query_parameters = event.get("queryStringParameters") or {}

        # Extract headers (ALB only has single value headers)
        headers_dict = {}
        for key, value in event.get("headers", {}).items():
            headers_dict[key.lower()] = str(value)

        self.request_headers = Headers(headers_dict)

    def get_client_ip(self) -> str:
        """Get the client IP address from ALB headers."""
        # ALB always provides client IP via X-Forwarded-For header
        forwarded_for = self.request_headers.get("x-forwarded-for")
        if not forwarded_for:
            raise KeyError(
                "The x-forwarded-for header wasn't present in the request, and it should always exist for anything behind an ALB.  You are probably using the wrong context."
            )

        # X-Forwarded-For can contain multiple IPs, take the first one
        return forwarded_for.split(",")[0].strip()

    def context_specifics(self) -> dict[str, Any]:
        """Provide ALB specific context data."""
        request_context = self.event.get("requestContext", {})
        elb = request_context.get("elb", {})

        return {
            **super().context_specifics(),
            "path": self.path,
            "target_group_arn": elb.get("targetGroupArn"),
        }
