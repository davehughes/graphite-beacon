import urllib
from tornado import gen, httpclient as hc

from . import AbstractHandler, LOGGER


class HipChatHandler(AbstractHandler):

    name = 'hipchat'

    # Default options
    defaults = {
        'room': None,
        'key': None,
    }

    colors = {
        'critical': 'red',
        'warning': 'magenta',
        'normal': 'green',
    }

    def init_handler(self):
        self.room = self.options.get('room')
        self.key = self.options.get('key')
        self.author = self.options.get('author', 'Graphite Beacon')
        self.notify_levels = self.options.get('notify_levels', [])
        assert self.room, 'Hipchat room is not defined.'
        assert self.key, 'Hipchat key is not defined.'
        self.client = hc.AsyncHTTPClient()

    @gen.coroutine
    def notify(self, level, *args, **kwargs):
        LOGGER.debug("Handler (%s) %s", self.name, level)

        message = self.get_short(level, *args, **kwargs)

        if level in self.notify_levels:
            message += ' ' + ' '.join('@' + name for name in self.notify_levels[level])

        data = {
            'room_id': self.room,
            'from': self.author,
            'message': message,
            'notify': 1,
            'color': self.colors.get(level, 'blue'),
            'message_format': 'text',
        }
        body = urllib.urlencode(data)

        url = 'https://api.hipchat.com/v1/rooms/message?auth_token=' + self.key

        LOGGER.debug("Request %s %s", url, body)

        yield self.client.fetch(
            url,
            method='POST', body=body,
            headers={'Content-Type': 'application/x-www-form-urlencoded'}
        )
