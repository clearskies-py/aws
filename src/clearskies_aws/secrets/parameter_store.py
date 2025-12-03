from __future__ import annotations

from botocore.exceptions import ClientError
from clearskies.exceptions.not_found import NotFound
from types_boto3_ssm import SSMClient

from clearskies_aws.secrets import secrets


class ParameterStore(secrets.Secrets):
    ssm: SSMClient

    def __init__(self):
        super().__init__()
        self.ssm = self.boto3.client("ssm", region_name=self.environment.get("AWS_REGION"))

    def create(self, path: str, value: str) -> bool:
        return self.update(path, value)

    def get(self, path: str, silent_if_not_found: bool = False) -> str | None:  # type: ignore[override]
        try:
            result = self.ssm.get_parameter(Name=path, WithDecryption=True)
        except ClientError as e:
            error = e.response.get("Error", {})
            if error.get("Code") == "ResourceNotFoundException":
                if silent_if_not_found:
                    return None
                raise NotFound(f"Could not find secret '{path}' in parameter store")
            raise e
        return result["Parameter"].get("Value", "")

    def list_secrets(self, path: str) -> list[str]:
        response = self.ssm.get_parameters_by_path(Path=path, Recursive=False)
        return [parameter["Name"] for parameter in response["Parameters"] if "Name" in parameter]

    def update(self, path: str, value: str) -> bool:  # type: ignore[override]
        response = self.ssm.put_parameter(
            Name=path,
            Value=value,
            Type="String",
            Overwrite=True,
        )
        return True

    def upsert(self, path: str, value: str) -> bool:  # type: ignore[override]
        return self.update(path, value)

    def list_sub_folders(
        self,
        path: str,
    ) -> list[str]:  # type: ignore[override]
        raise NotImplementedError("Parameter store doesn't support list_sub_folders.")
