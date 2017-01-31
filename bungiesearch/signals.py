from collections import defaultdict
from importlib import import_module
from threading import Lock, Timer

from django.db.models import signals
from django.utils.functional import cached_property

from . import Bungiesearch
from .utils import delete_index_item, update_index


def get_signal_processor():
    signals = Bungiesearch.BUNGIE['SIGNALS']
    if 'SIGNAL_CLASS' in signals:
        signal_path = signals['SIGNAL_CLASS'].split('.')
        signal_module = import_module('.'.join(signal_path[:-1]))
        signal_class = getattr(signal_module, signal_path[-1])
    else:
        signal_class = BungieSignalProcessor
    return signal_class()


class BungieSignalProcessor(object):

    __index_lock = Lock()
    __items_to_be_indexed = defaultdict(list)
    __timers = {}

    def post_save_connector(self, sender, instance, **kwargs):
        try:
            Bungiesearch.get_index(sender, via_class=True)
        except KeyError:
            return  # This model is not managed by Bungiesearch.

        items = None
        with self.__index_lock:
            self.__items_to_be_indexed[sender].append(instance)
            if len(self.__items_to_be_indexed[sender]) >= self.buffer_size:
                items = self.__items_to_be_indexed[sender]
                # Let's now empty this buffer.
                self.__items_to_be_indexed[sender] = []
            elif sender not in self.__timers:
                timer = Timer(5, self.flush_buffer, args=[sender])
                timer.start()
                self.__timers[sender] = timer

        if items:
            update_index(items, sender.__name__, bulk_size=self.buffer_size)

    def pre_delete_connector(self, sender, instance, **kwargs):
        try:
            Bungiesearch.get_index(sender, via_class=True)
        except KeyError:
            return  # This model is not managed by Bungiesearch.

        delete_index_item(instance, sender.__name__)

    def flush_buffer(self, sender):
        with self.__index_lock:
            items = self.__items_to_be_indexed[sender]
            self.__items_to_be_indexed[sender] = []
            self.__timers.pop(sender, None)

        if items:
            update_index(items, sender.__name__, bulk_size=self.buffer_size)

    def setup(self, model):
        signals.post_save.connect(self.post_save_connector, sender=model)
        signals.pre_delete.connect(self.pre_delete_connector, sender=model)

    def teardown(self, model):
        signals.pre_delete.disconnect(self.pre_delete_connector, sender=model)
        signals.post_save.disconnect(self.post_save_connector, sender=model)

    @cached_property
    def buffer_size(self):
        try:
            return Bungiesearch.BUNGIE['SIGNALS']['BUFFER_SIZE']
        except KeyError:
            return 100
