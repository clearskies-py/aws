from __future__ import annotations

import json

import clearskies

import clearskies_aws


class WebSocketConnectionModel(clearskies.Model):
    """
    Help manage message sending to websocket connections.

    ## Working with Websockets

    This is a partial model class to help send messages to websocket connections in an API gateway.
    With a API Gateway managed websocket, the API gateway assigns an id to every connection, and you
    send messages to the API gateway itself flagged for some client, via its connection id.  It
    helps to understand that you don't need to be connected to the websocket itself to send messages
    to the things connected to it.  Instead, you just need the necessary permission on the API gateway
    and you need to know the connection id of the client you want to send a message to.  For reference,
    the necessary AWS permission is:

    ```
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["execute-api:ManageConnections"],
                "Resource": "arn:aws:execute-api:${aws_region_name}:${aws_account_id}:${api_gateway_id}/{stage}/*",
            }
        ],
    }
    ```

    A simple flow that reproduces a pub/sub approach is:

     1. Client connects to the API gateway via a websocket, and the backend records the connection id
        in a backend somewhere
     2. Client sends a message through the websocket to "register"/"subscribe" for some resource, and
         the backend service updates the record for the connection to record what resource it is
         subscribed to
     3. If a server needs to send a message to everyone subscribed to a resource, it queries the backends
        for all records connected to the resource in question and sends a message to their connection ids
        through the backend.
     4. If a client needs to send a message to everyone subscribed to a resource, it sends a message
        through the websocket to some backend service which then passes the message along to the appropriate
        connections, just as in #3 above.

    Most examples with Websockets and API Gateway use dynamodb for storage.  You can, of course, use whatever
    backend you want.  Still, below is an example pub/sub application to demonstrate how to build a basic
    websocket app:

    ```
    import clearskies
    import clearskies_aws

    #####################
    ## Our model class ##
    #####################

    class Client(clearskies_aws.models.WebSocketConnectionModel):
        backend = clearskies_aws.backends.DynamoDbBackend()

        # the base WebSocketConnectionModel class defines a string
        # column called `connection_id` and sets it as the id column,
        # so those are already set.  We just need any additional columns
        resource_id = clearskies.columns.String()

    ###########################
    ## Our application logic ##
    ###########################

    def on_connect(clients, connection_id):
        clients.create({"connection_id": connection_id})

    def on_subscribe(clients, connection_id, request_data):
        client = clients.find(f"connection_id={connection_id}")
        if client.exists:
            # we blindly save the request id, which makes it user-generated.  This also
            # allows the client to "unsubscribe" by sending up a blank resource
            client.save({"resource_id": request_data["resource_id"])

    def on_publish(clients, connection_id, request_data):
        my_client = clients.find(f"connection_id={connection_id}")
        message = request_data.get("message")

        # The problem with our standard input validation is that we can't return a response
        # to the client with an error message.  This is not a transactional system.  Instead,
        # we would have to send the client a new message with the error in it, which is not
        # something the clearskies endpoints are designed for.
        if not message or not my_client.resource_id:
            return

        for client in clients.where(f"resource_id={my_client.resource_id}").paginate_all():
            if client.connection_id == my_client.connection_id:
                continue

            # the send function is provided by clearskies_aws.models.WebSocketConnectionModel
            client.send({"message": message})

    def on_disconnect(clients, connection_id):
        clients.find(f"connection_id={connection_id}").delete(except_if_not_exists=False)

    ######################################
    ## Wiring it all up with clearskies ##
    ######################################

    # We're going to build one application, even though each action gets it's own lambda.
    # The URLs aren't used for routing, but simply to allow us to select which function
    # is associated with each lambda.  Actual routing still happens in the API Gateway
    websocket_application = clearskies_aws.contexts.LambdaApiGatewayWebSocket(
        clearskies.EndpointGroup([
            clearskies.endpoints.Callable(
                on_connect,
                url="on_connect",
            ),
            clearskies.endpoints.Callable(
                on_subscribe,
                url="on_subscribe",
            ),
            clearskies.endpoints.Callable(
                on_publish,
                url="on_publish",
            ),
            clearskies.endpoints.Callable(
                on_disconnect,
                url="on_disconnect",
            ),
        ]),
        classes=[Client],
    )

    ################################
    ## The actual lambda handlers ##
    ################################

    def on_connect_handler(event, context):
        return websocket_application(url="on_connect")

    def on_subscribe_handler(event, context):
        return websocket_application(url="on_subscribe")

    def on_publish_handler(event, context):
        return websocket_application(url="on_publish")

    def on_disconnect_handler(event, context):
        return websocket_application(url="on_disconnect")
    ```

    """

    id_column_name = "connection_id"

    boto3 = clearskies_aws.di.inject.Boto3()
    connection_id = clearskies.columns.String()
    input_output = clearskies.di.inject.InputOutput()

    def send(self, message):
        if not self:
            raise ValueError("Cannot send message to non-existent connection.")
        if not self.connection_id:
            raise ValueError(
                f"Hmmm... I couldn't find the connection id for the {self.__class__.__name__}.  I'm picky about id column names.  Can you please make sure I have a column called connection_id and that it contains the connection id?"
            )

        domain = self.input_output.context_specifics()["domain"]
        stage = self.input_output.context_specifics()["stage"]
        # only include the stage if we're using the default AWS domain - not with a custom domain
        if ".amazonaws.com" in domain:
            endpoint_url = f"https://{domain}/{stage}"
        else:
            endpoint_url = f"https://{domain}"
        api_gateway = self.boto3.client("apigatewaymanagementapi", endpoint_url=endpoint_url)

        bytes_message = json.dumps(message).encode("utf-8")
        try:
            response = api_gateway.post_to_connection(Data=bytes_message, ConnectionId=self.connection_id)
        except api_gateway.exceptions.GoneException:
            self.delete()
        return response
