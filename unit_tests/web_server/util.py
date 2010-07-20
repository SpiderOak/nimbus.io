import uuid
import itertools
from collections import defaultdict

from gevent.queue import Queue


class FakeMessage(object):
    def __init__(self, routing_key, body, request_id=None):
        self.error = False
        self.routing_key = routing_key
        self.body = body
        if request_id is not None:
            self.request_id = request_id

    def marshall(self):
        args = [self.routing_key, self.body]
        try:
            args.append(self.request_id)
        except AttributeError:
            pass
        return '\t'.join(args)

    @classmethod
    def unmarshall(cls, data):
        return cls(*data.split('\t'))


class FakeAMQPMessage(object):
    def __init__(self, routing_key, body):
        self.delivery_info = dict(routing_key=routing_key)
        self.body = body


class MockChannel(object):
    """Stand in for AMQP channel that records published messages."""
    def __init__(self):
        self.messages = []

    def basic_publish(self, *args, **kwargs):
        self.messages.append((args, kwargs))


class FakeAMQPHandler(object):
    """An AMQPHandler that sends replies itself."""
    exchange = 'test_exchange'
    queue_name = 'test_queue'
    routing_key_binding = 'test_queue.*'

    def __init__(self):
        self.messages = []
        self.replies_to_send = defaultdict(Queue)
        self.replies_to_send_by_exchange = defaultdict(Queue)
        self.subscriptions = defaultdict(list)

    def subscribe(self, message_type, callback):
        self.subscriptions[message_type].append(callback)

    def _call_subscriptions(self, message):
        for callback in self.subscriptions[type(message)]:
            callback(message)

    def send_message(self, message, exchange=None):
        if exchange is None:
            exchange = self.exchange
        self.messages.append((message, exchange))
        try:
            if (message.request_id, exchange) in self.replies_to_send_by_exchange:
                return self.replies_to_send_by_exchange[message.request_id, exchange]
            if message.request_id in self.replies_to_send:
                return self.replies_to_send[message.request_id]
        except AttributeError:
            return None
        return Queue()


def fake_uuid_gen():
    for i in itertools.count():
        yield uuid.UUID(int=i)


class MockSqlCursor(object):
    def __init__(self):
        self.rows = {}
        self.queries = []
        self.results = None

    def execute(self, query, args=()):
        self.queries.append((query, args))
        try:
            self.results = list(self.rows[(query, tuple(args))])
        except KeyError:
            self.results = None

    def fetchone(self):
        if self.results:
            return self.results[0]
        return None

    def fetchall(self):
        return self.results


class MockSqlConnection(object):
    def __init__(self):
        self._cursor = MockSqlCursor()

    def cursor(self):
        return self._cursor


def fake_time():
    return 12345.123


class FakeAuthenticator(object):
    def __init__(self, remote_user):
        self.remote_user = remote_user

    def authenticate(self, req):
        if self.remote_user is not None:
            req.remote_user = self.remote_user
            return True
        return False


def fake_sample(population, k):
    """deterministic replacement for random.sample"""
    return list(population)[:k]


def fake_choice(population):
    """deterministic replacement for random.choice"""
    return list(population)[0]


class FakeAccounter(object):
    def __init__(self):
        self._added = defaultdict(int)
        self._retrieved = defaultdict(int)
        self._removed = defaultdict(int)

    def added(self, avatar_id, timestamp, bytes):
        self._added[avatar_id, timestamp] += bytes

    def retrieved(self, avatar_id, timestamp, bytes):
        self._retrieved[avatar_id, timestamp] += bytes

    def removed(self, avatar_id, timestamp, bytes):
        self._removed[avatar_id, timestamp] += bytes
