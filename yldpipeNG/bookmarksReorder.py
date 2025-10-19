from yldpipeNG.frameIOandCacheSupport import FrameIOandCacheSupport
from YamlConfigSupport import YamlConfigSupport

from flowpy.utils import setup_logger
import logging
logger = setup_logger(__name__, __name__+'.log', level=logging.DEBUG)
lg = setup_logger(__name__+'_2', __name__+'_2.log')

class BookmarksReorder(FrameIOandCacheSupport, YamlConfigSupport):
    sub = 'bmarks/'

    def __init__(self):
        super().__init__()

        tkeys_d = {}
        xlsx_groups = [
            'xlsx_framedumps',
            'xlsx_framedumps_groups',
            'xlsx_framedumps_others',
        ]

    def run(self):
        pass

    def test_init_dfio_dicts(self):
        pass







