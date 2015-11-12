from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.protocol import Protocol
from twisted.internet import defer
from twisted.web.client import Agent
from twisted.web.client import ResponseDone
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
        if response.code == 206:
            return defer.succeed('')
        else:
            class SimpleReceiver(Protocol):
                def __init__(s, d):
                    s.buf = ''
                    s.d = d

                def dataReceived(s, data):
                    s.buf += data

                def connectionLost(s, reason):
                    if response.code < 300:
                        s.d.callback(s.buf)
                    else:
                        s.d.errback(RuntimeError("Failed download: {} {}".format(response.code, response.phrase)))

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
        if response.code < 300:
            return defer.succeed('')
        else:
            return defer.fail(RuntimeError("Failed upload: {} {}".format(response.code, response.phrase)))

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