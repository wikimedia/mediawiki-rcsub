#!/usr/bin/python

import json, sys

from twisted.internet import reactor
from twisted.internet.protocol import DatagramProtocol, ServerFactory
from twisted.protocols.basic import LineReceiver
from autobahn.websocket import WebSocketServerFactory, WebSocketServerProtocol, listenWS


class Configuration(object):
    updatable = { "max_channels" }

    def __init__(self, filename):
        self.filename = filename
        configFile = open(filename)
        data = json.load(configFile)
        self.__dict__.update(data)
        self.data = data

    def reload(self):
        configFile = open(self.filename)
        data = json.load(configFile)

        update = { key : value for key, value in data.items() if getattr(self, key) != value }
        if not all(key in self.updatable for key in update):
            raise ProtocolError("Cannot update %s while server is running" % key)

        self.__dict__.update(data)
        self.data = data

class MessageRouter(object):
    def __init__(self):
        self.subs = {}
        self.numChannels = 0
        self.numSubs = 0
        self.numMessages = 0
        self.numDeliveries = 0

    def subscribe(self, channel, listener):
        if channel not in self.subs:
            self.subs[channel] = []
            self.numChannels += 1

        self.subs[channel].append(listener)
        self.numSubs += 1

    def unsubscribe(self, channel, listener):
        self.subs[channel].remove(listener)
        self.numSubs -= 1

        if not self.subs[channel]:
            del self.subs[channel]
            self.numChannels -= 1

    def deliver(self, channel, message):
        self.numMessages += 1

        if channel in self.subs:
            for listener in self.subs[channel]:
                listener.deliver(message)
                self.numDeliveries += 1

class Subscriber(object):
    def __init__(self, target):
        self.target = target
        self.channels = []

    def subscribe(self, channel):
        if len(self.channels) >= config.max_channels:
            raise ProtocolError('Exceeded maximum subscription limit of %i channels' % config.max_channels)
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

class ControlProtocol(LineReceiver):
    delimiter = "\n"

    def message(self, msg):
        self.transport.write( json.dumps(msg) + "\n" )

    def lineReceived(self, data):
        try:
            command = json.loads(data)

            if command["@"] == "stats":
                stats = { "@" : "stats" }
                stats["channels"] = router.numChannels
                stats["subs"] = router.numSubs
                stats["messages"] = router.numMessages
                stats["deliveries"] = router.numDeliveries
                self.message(stats)
            elif command["@"] == "stop":
                reactor.stop()
            elif command["@"] == "get-config":
                self.message(config.data)
            elif command["@"] == "reload-config":
                config.reload()
                self.message({ "@" : "success" })
            else:
                raise ProtocolError("Unknown command")

        except Exception as err:
            self.message({ "@" : "error", "message" : str(err) })

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

config = Configuration(sys.argv[1])

control_factory = ServerFactory()
control_factory.protocol = ControlProtocol
reactor.listenUNIX(config.control_path, control_factory)

ws_factory = WebSocketServerFactory( "ws://localhost:%i" % config.websocket_port )
ws_factory.protocol = WebSocketRCFeed
listenWS(ws_factory)

st_factory = ServerFactory()
st_factory.protocol = SimpleTextRCFeed
reactor.listenTCP(config.text_port, st_factory)

reactor.listenUDP(config.input_port, MediaWikiRCInput())

reactor.run()

