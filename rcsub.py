#!/usr/bin/python

import json

from twisted.internet import reactor
from twisted.internet.protocol import DatagramProtocol
from twisted.protocols.basic import LineReceiver
from autobahn.websocket import WebSocketServerFactory, WebSocketServerProtocol, listenWS

config = {
    'input_port' : 12719,
    'websocket_port' : 13719,
    'max_channels' : 100,
}

class MessageRouter(object):
    subs = {}

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
    channels = []

    def __init__(self, target):
        self.target = target

    def subscribe(self, channel):
        if len(self.channels) >= config['max_channels']:
            raise ProtocolError('Exceeded maximum subscription limit of %i channels' % config['max_channels'])

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

    def handleJSONCommand(self, command):
        if '@' not in command:
            raise ProtocolError('No command specified')

        if command['@'] == 'subscribe':
            self.subscribe(command['channel'])
            return { "@" : "success" }
        if command['@'] == 'unsubscribe':
            self.unsubscribe(command['channel'])
            return { "@" : "success" }
        if command['@'] == 'list-subscriptions':
            return { "@" : "subscriptions", "channels" : self.channels }

        return None

router = MessageRouter()

class ProtocolError(Exception):
    pass

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
            message = json.loads(data)
            
            response = self.subscriber.handleJSONCommand(message)
            if response:
                self.message(response)
            else:
                raise ProtocolError('Unknown command: %s' % message['@'])
        except Exception as err:
            self.message({'@' : 'error'})

    def deliver(self, message):
        self.message(message)

factory = WebSocketServerFactory( "ws://localhost:%i" % config['websocket_port'] )
factory.protocol = WebSocketRCFeed
listenWS(factory)

reactor.listenUDP(config['input_port'], MediaWikiRCInput())
reactor.run()

