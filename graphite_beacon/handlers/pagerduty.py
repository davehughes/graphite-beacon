import json
from tornado import gen, httpclient as hc

from . import AbstractHandler, LOGGER


class PagerDutyHandler(AbstractHandler):

    name = 'pagerduty'

    # Default options
    defaults = {
        'service_key': None,
    }

    def init_handler(self):
        self.service_key = self.option.get('service_key')
        assert self.service_key, 'PagerDuty service_key setting is required'
        self.client = hc.AsyncHTTPClient()

    @gen.coroutine
    def notify(self, level, alert, value, target=None, ntype=None, rule=None):
        LOGGER.debug("Handler (%s) %s", self.name, level)
        message = self.get_short(level, alert, value,
                                 target=target, ntype=ntype, rule=rule)

        if level == 'critical':
            event_type = 'trigger'
        elif level == 'normal':
            event_type = 'resolve'
        else:
            return

        yield self.client.fetch(
            'https://events.pagerduty.com/generic/2010-04-15/create_event.json',
            method='POST',
            body=json.dumps({
                'service_key': self.service_key,
                'event_type': event_type,
                'description': message,
                'incident_key': alert.name,
                'client': 'Graphite Beacon',
                'details': {
                    'rule': rule,
                },
            }),
            headers={'Content-Type': 'application/json'},
        )
