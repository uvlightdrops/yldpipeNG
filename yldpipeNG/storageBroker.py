# from yaml_config_support.YamlConfigSupport import YamlConfigSupport
from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')

from yldpipeNG.yamlStorage import YamlStorage
from yldpipeNG.yamldirStorage import YamldirStorage
from yldpipeNG.jsonStorage import JsonStorage, FirefoxBookmarksStorage
from yldpipeNG.kdbxStorage import KdbxStorage

from framecache_support.dbReader import DbReader

class StorageBroker:

    def st_class_factory(self, class_name, *args, **kwargs):
        rws = kwargs.pop('rws', 's')  # r, w, s
        if rws == 'r':
            class_name = class_name+'Reader'
        elif rws == 'w':
            class_name = class_name+'Writer'
        else:
            class_name = class_name+'Storage'
        # define mapping in config XXX
        classes = {
            #'SICache': SICache,
            #'MetadataSearch': MetadataSearch,
            'yamlStorage': YamlStorage,
            'yamldirStorage': YamldirStorage,
            'jsonStorage': JsonStorage,
            'firefox_bookmarksStorage': FirefoxBookmarksStorage,
            'kdbxStorage': KdbxStorage,
            'dbReader': DbReader,
        }
        # We dont need to pass on type, it is not of use anymore
        #return classes[class_name]()
        # XXX maybe later use the args and kwargs for more in the read and writer classes
        return classes[class_name](*args, **kwargs)

    # XXX combine both, usage check!
    # XXX add klass_cfg as param ? And use default from config as fallback

    def init_storage_src_class(self, *args, **kwargs):
        klass_cfg = self.cfg_profile['storage_src']
        klass_obj = self.st_class_factory(klass_cfg, 's', *args, **kwargs)
        klass_obj.src_or_dst = 'src'
        return klass_obj

    def init_storage_dst_class(self):
        klass_cfg = self.cfg_profile['storage_dst']
        klass_obj = self.st_class_factory(klass_cfg, 's', *args, **kwargs)
        klass_obj.src_or_dst = 'dst'
        return klass_obj


# dont make dependency to DataBroker
# we use the class as a member of the main logic class tree
