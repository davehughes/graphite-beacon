import json
import urllib
from tornado import gen, httpclient as hc

from . import AbstractHandler, LOGGER


class HipChatHandler(AbstractHandler):

    name = 'hipchat'
    SUPPORTED_API_VERSIONS = [1, 2]

    # Default options
    defaults = {
        'url': 'https://api.hipchat.com',
        'room': None,
        'key': None,
        'api_version': 2,
    }

    colors = {
        'critical': 'red',
        'warning': 'yellow',
        'normal': 'green',
    }

    def init_handler(self):
        self.room = self.options.get('room')
        self.key = self.options.get('key')
        self.api_version = self.options.get('api_version', 2)
        assert self.room, 'Hipchat room is not defined.'
        assert self.key, 'Hipchat key is not defined.'
        assert (self.api_version in self.SUPPORTED_API_VERSIONS,
            'Hipchat API version {} is not supported'.format(self.api_version))
        self.client = hc.AsyncHTTPClient()

    @gen.coroutine
    def notify(self, level, *args, **kwargs):
        LOGGER.debug("Handler (%s) %s", self.name, level)

        message = self.get_short(level, *args, **kwargs).decode('UTF-8')
        notify_levels = self.options.get('notify_levels', [])
        if level in notify_levels:
            message += ' ' + ' '.join('@' + name for name in notify_levels[level])

        if self.api_version == 1:
            data = {
                'message': message,
                'color': self.colors.get(level, 'gray'),
                'message_format': 'text',
                'notify': 1,
                'room_id': self.room,
                'from': self.options.get('author', 'Graphite Beacon'),
            }
            url = '{url}/v1/rooms/message?auth_token={token}'.format(
                    url=self.options['url'],
                    token=self.key,
                    )
            yield self.client.fetch(
                url,
                method='POST',
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                body=urllib.urlencode(data),
            )

        elif self.api_version == 2:
            data = {
                'message': message,
                'color': self.colors.get(level, 'gray'),
                'message_format': 'text',
                'notify': True,
            }
            url = '{url}/v2/room/{room}/notification?auth_token={token}'.format(
                    url=self.options['url'],
                    room=self.room,
                    token=self.key,
                    )
            yield self.client.fetch(
                url=url,
                method='POST',
                headers={'Content-Type': 'application/json'},
                body=json.dumps(data)
            )

        else:
            LOGGER.error('Unsupported Hipchat API version: {}'
                         .format(self.api_version))
