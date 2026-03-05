from __future__ import annotations

from typing import Any

from clearskies import Model

from clearskies_aws.actions.ses import SES as BaseSES


class SES(BaseSES):
    calls: list[dict[str, Any]] | None = None

    @classmethod
    def mock(cls, di):
        cls.calls = []
        di.mock_class(BaseSES, SES)

    def __call__(self, model: Model) -> None:
        """Record SES send email call without actually sending."""
        if SES.calls is None:
            SES.calls = []
        utcnow = self.di.build("utcnow")

        SES.calls.append(
            {
                "from": self.sender,
                "to": self.resolve_destination("to", model),
                "cc": self.resolve_destination("cc", model),
                "bcc": self.resolve_destination("bcc", model),
                "subject": self.resolve_subject(model, utcnow),
                "message": self.resolve_message_as_html(model, utcnow),
            }
        )
