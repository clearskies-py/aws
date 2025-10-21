from __future__ import annotations

from typing import Any

from clearskies.contexts.context import Context

from clearskies_aws.input_outputs import LambdaAlb as LambdaAlbInputOutput


class LambdaAlb(Context):
    """
    Run a clearskies application in a lambda behind an application load balancer.

    There's nothing special here: just build your application, use the LambdaAlb context in a standard AWS lambda
    handler, and attach your lambda to an ALB.  This generally expects that the ALB will forward all requests to
    the clearskies application, which will therefore handle all routing.  However, you can also use path-based
    routing in your target group to forward some subset of requests to separate lambdas, each using this same
    context.  When you do this, keep in mind that AWS still passes along the full path (including the part handled
    by the ALB), so you want to make sure that your clearskies application is configured with the full URL as well.

    Per AWS norms, you should create the context in the "root" of your python application, and then invoke it
    inside a standard lambda handler function.  This will allow AWS to cache the full application, improving
    performance.  If you create and invoke the context inside of your lambda handler, it will effectively turn
    off any caching.  In addition, clearskies does a fair amount of configuration validation when you create the
    context, so this work will be repeated on every call.

    ```
    import clearskies
    import clearskies_aws
    from clearskies.validators import Required, Unique
    from clearskies import columns


    class User(clearskies.Model):
        id_column_name = "id"
        backend = clearskies.backends.MemoryBackend()

        id = columns.Uuid()
        name = columns.String(validators=[Required()])
        username = columns.String(
            validators=[
                Required(),
                Unique(),
            ]
        )
        age = columns.Integer(validators=[Required()])
        created_at = columns.Created()
        updated_at = columns.Updated()


    application = clearskies_aws.contexts.LambdaAlb(
        clearskies.endpoints.RestfulApi(
            url="users",
            model_class=User,
            readable_column_names=["id", "name", "username", "age", "created_at", "updated_at"],
            writeable_column_names=["name", "username", "age"],
            sortable_column_names=["id", "name", "username", "age", "created_at", "updated_at"],
            searchable_column_names=["id", "name", "username", "age", "created_at", "updated_at"],
            default_sort_column_name="name",
        )
    )


    def lambda_handler(event, context):
        return application(event, context)
    ```

    ### Context for Callables

    When using this context, two additional named arguments become available to any callables invoked by clearskies:
    `event` and `context`.  These correspond to the original `event` and `context` variables provided by AWS to
    the lambda.
    """

    def __call__(self, event: dict[str, Any], context: dict[str, Any]) -> Any:  # type: ignore[override]
        return self.execute_application(LambdaAlbInputOutput(event, context))
