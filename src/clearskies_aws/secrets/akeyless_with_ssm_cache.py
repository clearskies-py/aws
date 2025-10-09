from __future__ import annotations

import re
from typing import Any

from clearskies.secrets.akeyless import Akeyless
from types_boto3_ssm import SSMClient

from clearskies_aws.secrets import parameter_store


class AkeylessWithSsmCache(parameter_store.ParameterStore, Akeyless):
    def get(self, path: str, refresh: bool = False) -> str | None:  # type: ignore[override]
        # AWS SSM parameter paths only allow a-z, A-Z, 0-9, -, _, ., /, @, and :
        # Replace any disallowed characters with hyphens
        ssm_name = re.sub(r"[^a-zA-Z0-9\-_\./@:]", "-", path)
        # if we're not forcing a refresh, then see if it is in paramater store
        if not refresh:
            missing = False
            try:
                response = self.ssm.get_parameter(Name=ssm_name, WithDecryption=True)
            except self.ssm.exceptions.ParameterNotFound:
                missing = True
            if not missing:
                value = response["Parameter"].get("Value", "")
                if value:
                    return value

        # otherwise get it out of Akeyless
        value = str(super().get(path))

        # and make sure and store the new value in parameter store
        if value:
            self.ssm.put_parameter(
                Name=ssm_name,
                Value=value,
                Type="SecureString",
                Overwrite=True,
            )

        return value

    def update(self, path: str, value: Any) -> bool:  # type: ignore[override]
        res = self._api.update_secret_val(
            self.akeyless.UpdateSecretVal(name=path, value=str(value), token=self._get_token())
        )
        self.ssm.put_parameter(
            Name=re.sub(r"[^a-zA-Z0-9\-_\./@:]", "-", path),
            Value=value,
            Type="SecureString",
            Overwrite=True,
        )
        return True

    def upsert(self, path: str, value: Any) -> bool:  # type: ignore[override]
        try:
            self.update(path, value)
        except Exception as e:
            self.create(path, value)
        return True
