import os
from re import compile as re, M

import json
import yaml
import logging
from tornado import ioloop, log

from .alerts import BaseAlert
from .utils import parse_interval
from .handlers import registry


LOGGER = log.gen_log

COMMENT_RE = re('//\s+.*$', M)


class Reactor(object):

    """ Class description. """

    defaults = {
        'auth_password': None,
        'auth_username': None,
        'config': 'config.json',
        'critical_handlers': ['log'],
        'debug': False,
        'format': 'short',
        'graphite_url': 'http://localhost',
        'history_size': '1day',
        'interval': '10minute',
        'logging': 'info',
        'method': 'average',
        'normal_handlers': ['log'],
        'pidfile': None,
        'prefix': '[BEACON]',
        'repeat_interval': '2hour',
        'request_timeout': 20.0,
        'send_initial': False,
        'warning_handlers': ['log'],
    }

    def __init__(self, **options):
        self.alerts = set()
        self.loop = ioloop.IOLoop.instance()
        self.options = None
        self.reinit(**options)
        self.callback = ioloop.PeriodicCallback(
            self.repeat, parse_interval(self.options['repeat_interval']))

    def reinit(self, *args, **options):
        '''
        (Re)initialize the reactor by reading and merging its configuration
        and refreshing alerts and handlers.  Since this is used for both
        initial loading and hot reinitialization, the implementation takes the
        approach of performing as much refreshing as possible without touching
        the reactor's state, since there are a number of things that can go
        wrong in this process that could otherwise cause a bad partial state to
        be loaded.

        In the case of refreshing alerts and handlers, those objects have
        dependencies on a reasonably functional reactor object with a full
        config, so we assign new configuration and roll it back in the case of
        failure.
        '''
        LOGGER.info('Read configuration')

        old_options = self.options
        old_defaults = self.defaults
        old_alerts = self.alerts
        
        # Merge options into defaults for the purpose of loading the config,
        # but don't immediately update the stored defaults.  Once the config
        # is fully loaded, store the defaults.
        new_defaults = dict(self.defaults)
        new_defaults.update(options)
        new_options = self.load_configuration(new_defaults)

        self.options = new_options
        self.defaults = new_defaults

        LOGGER.setLevel(_get_numeric_log_level(self.options.get('logging', 'info')))
        registry.clean()

        try:
            new_alerts = set(BaseAlert.get(self, **opts)
                             for opts in new_options.get('alerts', []))
            new_handlers = self.load_handlers(new_options)
        except Exception as e:
            self.options = old_options
            self.defaults = old_defaults
            raise

        self.handlers = new_handlers
        old_alerts, self.alerts = self.alerts, new_alerts

        for alert in list(old_alerts):
            alert.stop()
            self.alerts.remove(alert)

        for alert in self.alerts:
            alert.start()

        LOGGER.debug('Loaded with options:')
        LOGGER.debug(json.dumps(self.options, indent=2))
        return self

    def load_configuration(self, defaults):
        """
        Loads the configuration, starting with `defaults` as a base. Partial
        configurations are merged in the following order, with later items
        overriding earlier items:

        + `defaults` passed to this method
        + File named by `defaults`.config, and nested includes
        + Files listed in `defaults`.include (in order) and nested
          includes
        """
        configs = [defaults]

        try:
            includes = defaults.pop('include', [])
            includes.append(defaults.get('config'))

            while len(includes) > 0:
                include_path = includes.pop()
                if not include_path:
                    continue
                config_object = self.load_config_file(include_path)

                # Push any nested includes onto the stack
                nested_includes = config_object.pop('include', [])
                nested_includes.reverse()
                includes.extend(nested_includes)

                configs.append(config_object)
        except (IOError, ValueError, yaml.error.YAMLError):
            e = InvalidConfigError(include_path)
            LOGGER.error(e.message)
            raise e

        # Merge all config objects into one
        merged_config = {}
        for config in configs:
            merge_configurations(merged_config, config)
        return merged_config

    def load_config_file(self, path):
        if not path:
            return {}

        LOGGER.info('Load configuration: %s' % path)
        with open(path) as fconfig:
            source = COMMENT_RE.sub("", fconfig.read())
            return yaml.load(source)

    def load_handlers(self, options, levels=None):
        levels = levels or ['normal', 'warning', 'critical']
        handlers = {}
        for level in levels:
            handler_set = set()
            for name in options.get('{}_handlers'.format(level)):
                try:
                    handler_set.add(registry.get(self, name))
                except Exception as e:
                    LOGGER.error('Handler "%s" did not init. Error: %s' % (name, e))
                    raise
        return handlers

    def reinit_handlers(self, level='warning'):
        for name in self.options['%s_handlers' % level]:
            try:
                self.handlers[level].add(registry.get(self, name))
            except Exception as e:
                LOGGER.error('Handler "%s" did not init. Error: %s' % (name, e))

    def repeat(self):
        LOGGER.info('Reset alerts')
        for alert in self.alerts:
            alert.reset()

    def start(self, *args):
        if self.options.get('pidfile'):
            with open(self.options.get('pidfile'), 'w') as fpid:
                fpid.write(str(os.getpid()))
        self.callback.start()
        LOGGER.info('Reactor starts')
        self.loop.start()

    def stop(self, *args):
        self.callback.stop()
        self.loop.stop()
        if self.options.get('pidfile'):
            os.unlink(self.options.get('pidfile'))
        LOGGER.info('Reactor has stopped')

    def notify(self, level, alert, value, target=None, ntype=None, rule=None):
        """ Provide the event to the handlers. """

        LOGGER.info('Notify %s:%s:%s:%s', level, alert, value, target or "")

        if ntype is None:
            ntype = alert.source

        for handler in self.handlers.get(level, []):
            handler.notify(level, alert, value, target=target, ntype=ntype, rule=rule)

_LOG_LEVELS = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARN': logging.WARN,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}


def _get_numeric_log_level(level):
    """Convert a textual log level to the numeric constants expected by the
    :meth:`logging.Logger.setLevel` method.

    This is required for compatibility with Python 2.6 where there is no conversion
    performed by the ``setLevel`` method. In Python 2.7 textual names are converted
    to numeric constants automatically.

    :param basestring name: Textual log level name
    :return: Numeric log level constant
    :rtype: int
    """
    if not isinstance(level, int):
        try:
            return _LOG_LEVELS[str(level).upper()]
        except KeyError:
            raise ValueError("Unknown log level: %s" % level)
    return level



def merge_configurations(a, b):
    """
    Merges b into a and returns the merged result.

    NOTE: tuples and arbitrary objects are not handled as it is totally
    ambiguous what should happen.
    """
    key = None
    try:
        if (a is None
                or isinstance(a, str)
                or isinstance(a, unicode)
                or isinstance(a, int)
                or isinstance(a, long)
                or isinstance(a, float)):
            # border case for first run or if a is a primitive
            a = b
        elif isinstance(a, list):
            # lists can be only appended
            if isinstance(b, list):
                # merge lists
                a.extend(b)
            else:
                # append to list
                a.append(b)
        elif isinstance(a, dict):
            # dicts must be merged
            if isinstance(b, dict):
                for key in b:
                    if key in a:
                        a[key] = merge_configurations(a[key], b[key])
                    else:
                        a[key] = b[key]
            else:
                msg = 'Cannot merge non-dict "{}" into dict "{}"'.format(b, a)
                raise ConfigMergeError(msg)
        else:
            msg = 'NOT IMPLEMENTED "{}" into "{}"'.format(b, a)
            raise ConfigMergeError(msg)
    except TypeError, e:
        msg_tpl = 'TypeError "{}" in key "{}" when merging "{}" into "{}"'
        msg = msg_tpl.format(e, key, b, a)
        raise ConfigMergeError(msg)
    return a


class InvalidConfigError(ValueError):
    def __init__(self, config_path):
        super(ValueError, self).__init__('Invalid config file: %s' % config_path)
    

class ConfigMergeError(Exception):
    pass
