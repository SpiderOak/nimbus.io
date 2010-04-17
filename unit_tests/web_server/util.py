import uuid
import itertools

from diyapi_web_server.amqp_handler import AMQPHandler


class FakeMessage(object):
    def __init__(self, routing_key, body, request_id=None):
        self.routing_key = routing_key
        self.body = body
        if request_id is not None:
            self.request_id = request_id

    def marshall(self):
        return self.body


class MockChannel(object):
    """Stand in for AMQP channel that records published messages."""
    def __init__(self):
        self.messages = []

    def basic_publish(self, *args, **kwargs):
        self.messages.append((args, kwargs))


class FakeAMQPHandler(AMQPHandler):
    """An AMQPHandler that sends replies itself."""
    def __init__(self):
        super(FakeAMQPHandler, self).__init__()
        self.replies_to_send = {}

    def send_message(self, message, exchange=None):
        replies = super(FakeAMQPHandler, self).send_message(message, exchange)
        for reply in self.replies_to_send.get(message.request_id, ()):
            replies.put(reply)
        return replies


def fake_uuid_gen():
    for i in itertools.count():
        yield uuid.UUID(int=i)
