
from anytree import Node

from flowpy.utils import setup_logger
from yldpipeNG.statsSupport import StatsSupport

logfn = __name__+'.log'
logger = setup_logger(__name__, logfn)

class Entry:
    title = 'n/a'
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
    def __repr__(self):
        return "Entry: "+self.title


class CustomNode(Node):
    """ node for firefox bookmarks json
    """
    _mypath = None
    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        # this is a list of CustomNode objects
        logger.debug('self.path: %s', self.path)
        # that is a path starting with "root/x/y/z", no leading /
        self.mypath = '/'.join([str(node.name) for node in self.path])
        #logger.debug('self.mypath: %s', self.mypath)
        self.entries = []
        # logger.debug('self._path: %s', self._mypath)
    @property
    def mypath(self):
        return self._mypath
    @mypath.setter
    def mypath(self, val):
        self._mypath = val
    def reset_mypath(self):
        self.mypath = '/'.join( [str(node.name) for node in self.path] )

    @property
    def subgroups(self):
        # if all children have no further kids
        sgroups = []
        for child in self.children:
            if child.typeCode == 2:
                sgroups.append(child)

        logger.debug('subgroups: %s', sgroups)
        return sgroups

    @property
    def title(self):
        if hasattr(self, 'name'):
            return self.name
        return self.title
    @title.setter
    def title(self, val):
        self.name = val


class EquipSet(CustomNode, StatsSupport):
    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.yaml_d = kwargs['yaml_d']
        self.load_entries()

    @property
    def repr(self):
        return '_'.join(self.mypath.split('/')[1:])

    def load_entries(self):
        self.entries = [item if type(item)==str else 'add' for item in self.yaml_d['items'] ]
        logger.debug('entries: %s', self.entries)

    def dump_entries(self, attrs, df):
        self.stats_init()
        # somehow to substitute the entries loop in tbw dump_group_entries
        logger.debug('entries: %s', self.entries)
        for entry in self.entries:
            row = {}
            for attr in attrs:
                row[attr] = entry # just a string

            row['pk'] = self.count

            self.count += 1
            # XXX unfinished, how can we return the table "dataframe?" to the main app?
            ldf = len(df)
            df.loc[ldf] = row
        # logger.debug(df.head())
        # return df
        # buggy # self.stats_report(name='ES_dump'+self.name)
