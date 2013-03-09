#!/usr/bin/python

import json

from twisted.internet import reactor
from twisted.internet.protocol import DatagramProtocol, ServerFactory
from twisted.protocols.basic import LineReceiver
from autobahn.websocket import WebSocketServerFactory, WebSocketServerProtocol, listenWS

config = {
    'input_port' : 12719,
    'websocket_port' : 13719,
    'text_port' : 14719,
    'max_channels' : 100,
}

class MessageRouter(object):
    def __init__(self):
        self.subs = {}

    def subscribe(self, channel, listener):
        if channel not in self.subs:
            self.subs[channel] = []
        self.subs[channel].append(listener)

    def unsubscribe(self, channel, listener):
        self.subs[channel].remove(listener)
        if not self.subs[channel]:
            del self.subs[channel]

    def deliver(self, channel, message):
        if channel in self.subs:
            for listener in self.subs[channel]:
                listener.deliver(message)

class Subscriber(object):
    def __init__(self, target):
        self.target = target
        self.channels = []

    def subscribe(self, channel):
        if len(self.channels) >= config['max_channels']:
            raise ProtocolError('Exceeded maximum subscription limit of %i channels' % config['max_channels'])
        if channel in self.channels:
            raise ProtocolError('You are already subscribed to the channel %s' % channel)

        self.channels.append(channel)
        router.subscribe(channel, self)
    
    def unsubscribe(self, channel):
        if channel not in self.channels:
            raise ProtocolError('Trying to unsubscribe from channel you are not subscribed to')

        self.channels.remove(channel)
        router.unsubscribe(channel, self)

    def unsubscribeAll(self):
        for channel in self.channels:
            router.unsubscribe(channel, self)
        self.channels = []

    def deliver(self, message):
        self.target.deliver(message)

    def handleJSONCommand(self, commandText):
        command = json.loads(commandText)

        if '@' not in command:
            raise ProtocolError('No command specified')

        if command['@'] in ('subscribe', 'unsubscribe') and 'channel' not in command:
            raise ProtocolError('No channel specified')

        if command['@'] == 'subscribe':
            self.subscribe(command['channel'])
            return { "@" : "success" }
        if command['@'] == 'unsubscribe':
            self.unsubscribe(command['channel'])
            return { "@" : "success" }
        if command['@'] == 'list-subscriptions':
            return { "@" : "subscriptions", "channels" : self.channels }

        raise ProtocolError('Unknown command: %s' % command['@'])

router = MessageRouter()

class ProtocolError(Exception):
    pass

def handleError(err):
    if type(err) in (ProtocolError, ValueError):
        return str(err)
    else:
        return "Internal error"

class MediaWikiRCInput(DatagramProtocol):
    def datagramReceived(self, data, (host, port)):
        change = json.loads(data)
        router.deliver(change['channel'], change)

class WebSocketRCFeed(WebSocketServerProtocol):
    def connectionMade(self):
        WebSocketServerProtocol.connectionMade(self)
        self.subscriber = Subscriber(self)

    def onClose(self, wasClean, code, reason):
        self.subscriber.unsubscribeAll()

    def message(self, data):
        self.sendMessage(json.dumps(data))

    def onMessage(self, data, is_binary):
        try:
            self.message( self.subscriber.handleJSONCommand(data) )
        except Exception as err:
            self.message({'@' : 'error', 'message' : handleError(err)})

    def deliver(self, message):
        self.message(message)

class SimpleTextRCFeed(LineReceiver):
    def connectionMade(self):
        LineReceiver.connectionMade(self)
        self.subscriber = Subscriber(self)

    def connectionLost(self, reason):
        self.subscriber.unsubscribeAll()

    def message(self, data):
        self.transport.write(json.dumps(data) + "\r\n")

    def lineReceived(self, line):
        try:
            self.message( self.subscriber.handleJSONCommand(line) )
        except Exception as err:
            self.message({'@' : 'error', 'message' : handleError(err)})

    def deliver(self, message):
        self.message(message)

ws_factory = WebSocketServerFactory( "ws://localhost:%i" % config['websocket_port'] )
ws_factory.protocol = WebSocketRCFeed
listenWS(ws_factory)

st_factory = ServerFactory()
st_factory.protocol = SimpleTextRCFeed
reactor.listenTCP(config['text_port'], st_factory)

reactor.listenUDP(config['input_port'], MediaWikiRCInput())

reactor.run()

