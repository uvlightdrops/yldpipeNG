from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')


class StorageCache:
    def __init__(self, loader_class, *args, **kwargs):
        self._cache = {}
        self._loader_class = loader_class
        self._args = args
        self._kwargs = kwargs

    def get_resource(self, key, *args, **kwargs):
        if key not in self._cache:
            type = kwargs.get('type', None)
            if type is None:
                logger.error('StorageCache: type should be given only at first call for key %s', key)
                raise Exception('type should be given at first call')

            self._cache[key] = self._loader_class(type, *args, **kwargs)
            logger.debug('loaded resource %s for key %s', self._cache[key], key)
        else:
            logger.debug('StorageCache: using cached resource for key %s', key)
        return self._cache[key]

    def fetch_resource(self, key):
        return self._cache.get(key, None)

    def clear_cache(self):
        self._cache.clear()


class ResourceWrapper:
    def __init__(self, inner, name):
        self.inner = inner
        self.name = name
        self.version = 0

    def clone(self):
        # shallow/deep copy je nach Ressourcentyp; hier exemplarisch deep copy
        import copy
        new_inner = copy.deepcopy(self.inner)
        new_wrapper = ResourceWrapper(new_inner, self.name)
        new_wrapper.version = self.version + 1
        return new_wrapper

    def bump_name(self):
        # konvention: resource_key_version
        return f"{self.name}_v{self.version}"

    # delegiere Methoden an inner falls nötig
    def __getattr__(self, item):
        return getattr(self.inner, item)
