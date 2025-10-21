from __future__ import annotations

import traceback

from clearskies.authentication import Public
from clearskies.contexts.context import Context

from clearskies_aws.input_outputs import LambdaSqsStandard as LambdaSqsStandardInputOutput


class LambdaSqsStandardPartialBatch(Context):
    def __call__(self, event, context, url="", method="POST"):
        item_failures = []
        for record in event["Records"]:
            print("Processing message " + record["messageId"], record["body"])
            try:
                self.execute_application(
                    LambdaSqsStandardInputOutput(record["body"], event, context, url=url, method=method)
                )
            except Exception as e:
                print("Failed message " + record["messageId"] + " being returned for retry.  Error error: " + str(e))
                traceback.print_tb(e.__traceback__)
                item_failures.append({"itemIdentifier": record["messageId"]})

        if item_failures:
            return {
                "batchItemFailures": item_failures,
            }
        return {}
