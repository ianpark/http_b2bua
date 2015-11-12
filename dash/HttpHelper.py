from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.protocol import Protocol
from twisted.internet import defer
from twisted.web.client import Agent
from twisted.web.http_headers import Headers

""" Make the ClientFactory silent """
from twisted.web import client
client._HTTP11ClientFactory.noisy = False

"""
Asynchrnous HTTP Download
"""
def getResource(path):
    agent = Agent(reactor)
    d = agent.request(
        'GET',
        path,
        Headers({'User-Agent': ['playout proxy']}),
        None)

    def handle_response(response):
        class SimpleReceiver(Protocol):
            def __init__(s, d):
                s.buf = ''; s.d = d

            def dataReceived(s, data):
                s.buf += data

            def connectionLost(s, reason):
                # TODO: test if reason is twisted.web.client.ResponseDone, if not, do an errback
                s.d.callback(s.buf)

        d = defer.Deferred()
        response.deliverBody(SimpleReceiver(d))
        return d
    d.addCallback(handle_response)
    return d
"""
Asynchrnous HTTP upload
"""
def postResource(path, data):
    agent = Agent(reactor)
    from twisted.web.client import FileBodyProducer
    from io import BytesIO
    d = agent.request(
        'POST',
        path,
        Headers({'User-Agent': ['playout proxy']}),
        FileBodyProducer(BytesIO(data)))

    def handle_response(response):
        return defer.succeed('')

    d.addCallbacks(handle_response)
    return d

"""
Asynchrnous HTTP delete
"""
def deleteResource(path):
    agent = Agent(reactor)
    from twisted.web.client import FileBodyProducer
    from io import BytesIO
    d = agent.request(
        'DELETE',
        path,
        Headers({'User-Agent': ['playout proxy']}),
        None)

    def handle_response(response):
        return defer.succeed('')

    d.addCallbacks(handle_response)
    return d