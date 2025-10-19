
from yldpipeNG.anytreeStorage import AnytreeStorage
from yldpipeNG.customNodes import CustomNode
from anytree.importer import DictImporter
# this class, is it for just one schema? How to suport several schemas oder any schema?
# possible : use schema lib
# or define schena in our way, data_master
#
from flowpy.utils import setup_logger
logfn = __name__+'.log'
logger = setup_logger(__name__, logfn)


class YamlStorage(AnytreeStorage):
    """ class for yaml tree DB """
    importer = DictImporter()

    def __init__(self, data=None):
        # logger.debug('data: %s', data)
        if data is None:
            data = {}
        self.data = data
        self.root_node = CustomNode('root')

    def set_src(self, fp):
        logger.debug('Setting fp=%s for type: %s ', fp, self.src_or_dst)
        self.fp = fp
        if self.src_or_dst == 'src':
            self.load()

    def write(self):
        with open(self.fp, 'w') as file:
            yaml.dump(self.data, file)

    def load(self):
        with open(self.fp) as file:
            self.yaml = yaml.load(file, Loader=yaml.FullLoader)

    def find(self, key):
        self.yaml = self.data
        logger.debug('self.yaml: %s', self.yaml)
        for k, v in self.yaml.iteritems():
            logger.debug('k: %s, v: %s', k, v)
            if k == key:
                # yield v
                return v
            elif isinstance(v, dict):
                for result in self.find(key, v):
                    # yield result
                    return result
            elif isinstance(v, list):
                for d in v:
                    logger.debug('d: %s', d)
                    for result in self.find(key, d):
                        # yield result
                        return result


    def _find_entry_by_path(self, path):
        # provide path to yaml file and key somehow
        pass

    # is used for yamldirStorage also !
    def _find_group_by_path(self, path: list or str):
        # this is a general thing, can be useful for all storage types XXX
        # random logic, what path format is used?  mypath: slashed path
        logger.debug('path: %s', path)
        if isinstance(path, str):
            if path.find('/') > 0 or path=='root':
                # logger.debug('discovered slashed path %s', path)
                path_slashed = path
            else:
                raise ValueError

        if isinstance(path, list):
            # XXX might get list like [ 'my/path/here' ] with one element
            # logger.debug('list path discovered: %s', path)
            path_slashed = '/'.join(path)

        logger.debug('path_slashed: %s', path_slashed)
        return self.anytree_find_groups_by_path(path_slashed, attr_name='mypath')



    def find_entry_by_path(self, path):
        pass

