from __future__ import annotations

from typing import Any

from clearskies import Model
from types_boto3_ses import SESClient

from clearskies_aws.actions.ses import SES as BaseSES


class SES(BaseSES):
    calls: list[dict[str, Any]] | None = None

    @classmethod
    def mock(cls, di):
        cls.calls = []
        di.mock_class(BaseSES, SES)

    def _execute_action(self, client: SESClient, model: Model) -> None:
        """Send a notification as configured."""
        if SES.calls is None:
            SES.calls = []
        utcnow = self.di.build("utcnow")

        SES.calls.append(
            {
                "from": self.sender,
                "to": self._resolve_destination("to", model),
                "cc": self._resolve_destination("cc", model),
                "bcc": self._resolve_destination("bcc", model),
                "subject": self._resolve_subject(model, utcnow),
                "message": self._resolve_message_as_html(model, utcnow),
            }
        )
