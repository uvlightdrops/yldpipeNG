
from anytree import NodeMixin, Node, AnyNode, RenderTree, PreOrderIter
import itertools

import logging
from flowpy.utils import setup_logger
logfn = __name__+'.log'
logger = setup_logger(__name__, logfn, level=logging.DEBUG)

"""
class CustomNode(NodeMixin):
    def __init__(self, name, children=None):
        self.name = name
        self.children = children or []

    def render(self, with_attr=False):
        for pre, fill, node in RenderTree(self):
            #print("%s%s" % (pre, node.name))
            if with_attr:
                if hasattr(node, 'dest'):   
                    print("%s%s-->%s" % (pre, node.id, node.dest.name))
                else:
                    print("%s%s" % (pre, node.id))
            else:
                print("%s%s" % (pre, node.id))
"""

class TreeSupport:
    def preOrderIter(self, root):
        for node in PreOrderIter(root):
            print(node.name)
