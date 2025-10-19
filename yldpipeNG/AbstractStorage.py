from abc import ABC, abstractmethod
from pykeepass.baseelement import BaseElement

# XXX this is pykeepass specific, we want abstract base class for other storage types
class AbstractStorage(ABC):

    def set_src(self, path):
        pass

    def _find_group(self, recursive=True, path=None, group=None, **kwargs):
        pass

    @abstractmethod
    def _find_group_by_path(self, path: list or str) -> BaseElement:
        pass

    def _find_group_by_path_slashed(self, path_slashed: str) -> BaseElement:
        pass

    def _find_group_by_name(self, name: str) -> BaseElement:
        """ find group by name """
        pass

    @abstractmethod
    def _find_entry_by_path(self, path):
        pass

    # not useful up to now
    def load_hierarchy(self, path):
        pass
