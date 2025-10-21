from __future__ import annotations

from typing import Any

from clearskies.contexts.context import Context

from clearskies_aws.input_outputs import (
    LambdaApiGatewayWebSocket as LambdaApiGatewayWebSocketInputOutput,
)


class LambdaApiGatewayWebSocket(Context):
    """
    Run a clearskies application behind an API Gateway that is configured for use as a websocket.

    Websockets work much differently than standard API endpoints.  Most importantly, none of the standard HTTP
    concepts exist.  Websockets requests don't have any of:

     1. URL Path
     2. Query Parameters
     3. HTTP Headers
     4. Response Headers
     5. An HTTP Response

    So in short, everything works completely differently.  The reason is because a websocket is a
    two-way communication channel that's created over a TCP/IP connection.  It does start with an HTTP request,
    but this is a one time request when the communication channel is first created.  Later messages (which
    are where the bulk of the communication happens) travel over the already-open connection, so
    the communication looks nothing like HTTP.  Usually, the data traveling over this connection is
    a JSON payload, and since the connection is already opened it doesn't have any of the metadata associated
    with an HTTP request (hence the lack of url/query/headers).  In addition, the communication is no longer
    transactional - messages from the client to the server do not come with a direct response, and the server
    can send messages to the client without needing the former to initiate the conversation.

    Routing and authorization are usually handled in-band, which means that the routing parameters or authentication
    data are added directly to the JSON body sent over the open connection.  This often results in applications
    having to handle such things themselves, since the typical standards of web frameworks won't match up.  In
    the case of routing with an API Gateway, it has its own suggested standard of setting a routekey where the
    API gateway will check for an application-defined route parameter in the request body and use this to route
    to an appropriate lambda.  With clearskies, you can also use the `clearskies.endpoints.JsonParamEndpointGroup`
    to accomplish the same.

    With a websocket through API Gateway, headers are available during the `on_connect` phase, so you can always
    perform authentication then and record the result with the connection id (which can be used much like a
    session id).  Otherwise, authentication is typically handled by including the authentication token in every
    message payload.

    ### Sending Messages

    An important part of using websockets is being able to manage and send messages to clients.  To help with this,
    there is a base model class in `clearskies_aws.models.WebSocketConnectionModel`.  Check the documentation for
    this class to understand how this is managed and see a "starter" websocket application.

    """

    def __call__(self, event: dict[str, Any], context: dict[str, Any], url: str = "") -> Any:  # type: ignore[override]
        return self.execute_application(LambdaApiGatewayWebSocketInputOutput(event, context, url))
