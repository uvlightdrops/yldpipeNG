from yldpipeNG.AbstractStorage import AbstractStorage
from anytree.search import find_by_attr
from anytree import RenderTree, LevelOrderIter
import anytree
 # , Node, PreOrder
from anytree.exporter import DictExporter, JsonExporter
from yldpipeNG.customNodes import CustomNode, Entry
import yaml, json
# import os, glob
from flowpy.utils import setup_logger
logfn = __name__+'.log'
logger = setup_logger(__name__, logfn)


class AnytreeStorage(AbstractStorage):
    """ class for anytree storage """
    root_node = None
    use_default_group = False
    errorlist = []
    count_errors = 0
    # XXX pass fp_in and fp_out path (data_in, data_out)

    def export(self, fp, format='yaml'):
        logger.debug('exporting to %s ', str(fp))
        logger.debug('exporting in %s format', format)
        # self.prepare_export(fp, format=format)

        if format == 'pure_hierarchy':
            attriter = lambda attrs: [(v, '') for k, v in attrs if k == "name"]
            childiter= lambda children: [(child.name, None) for child in children]
            exporter = CustomDictExporter(attriter=attriter, childiter=childiter)

        if format == 'yaml':
            exporter = DictExporter()
            #exporter = DictExporter(attriter=lambda attrs: [(k, v) for k, v in attrs if k == "a"])

        if format == 'json':
            exporter = JsonExporter()

        blob = exporter.export(self.root_node)
        logger.debug('len(blob) %s, exporting as %s', len(blob), fp)
        with open(fp, 'w') as file:
            logger.debug('format: %s', format)
            if format in ['yaml', 'pure_hierarchy']:
                yaml.dump(blob, file)
            if format == 'json':
                json.dump(blob, file)

    def get_levelorderiter(self):
        return LevelOrderIter(self.root_node)

    def iter_get_groups_all(self):
        # g_list = [[node.mypath for node in children] for children in LevelOrderGroupIter(self.root_node)]
        [node.reset_mypath() for node in LevelOrderIter(self.root_node)]
        group_list = [node.mypath for node in LevelOrderIter(self.root_node)]
        g_list_l = [ sl_path.split('/') for sl_path in group_list ]
        logger.debug('g_list_l: %s', g_list_l)
        return group_list

    # find by name and typeCode
    # find by mypath
    def anytree_find_groups_by_path(self, path, **kwargs):
        return self.anytree_find_groups_by_attr(path, attr_name='mypath')

    def anytree_find_groups_by_attr(self, val, attr_name='mypath', **kwargs):
        # in yaml cfg I need to avoid space in keys. So here replace it back
        #res = find_by_attr(self.root_node, val, name='title')
        #val = 'root/'+val
        #res = find_by_attr(self.root_node, val, name=name, **kwargs)
        # 2024-11-04
        # okay this callback works, find_by_attr does not for custom attributes, or maybe because it is a property
        def checkmp(node):
            #logger.debug('node: %s', node)
            if hasattr(node, attr_name):
                # logger.debug('node.%s: %s', attr_name, getattr(node, attr_name))
                if getattr(node, attr_name) == val:
                    return True
            return False

        if attr_name == 'mypath' and not val.startswith('root/'):
            val = 'root/' + val
        res = anytree.search.find(self.root_node, checkmp, **kwargs)
        # res = find_by_attr(self.root_node, val, name=attr_name, **kwargs)
        # logger.debug('find_by_attr: attr_name=%s, val is %s', attr_name, val)
        #for child in self.root_node.children:
            #logger.debug(child.mypath)
        if res:
            logger.debug('for val:%s result found: %s', val, res.mypath)
            out = res.mypath
        else:
            logger.debug('for val:%s - NOT found', val)
            out = 'NONE'
            if self.use_default_group:
                res = self.root_node
                out = res.mypath

        # logger.debug('FIND: In %s - res: %s', self.__class__.__name__, out)
        return res

    def load_hierarchy(self, path):
        pass

    def set_hierarchy_from_yaml_file(self, fp):
        with open(fp) as file:
            self.yaml = yaml.load(file, Loader=yaml.FullLoader)

    def set_hierarchy_from_yaml_string(self, yaml):
        self.yaml = yaml


    def _import(self):
        self.root = self.importer.import_(self.yaml)
        logger.debug('self.root: %s', self.root)
        for node in self.root.children:
            logger.debug('node.title: %s', node.title)
        logger.debug('root children: %s', self.root.children)


    def prepare_export(self, fp, format='yaml'):
        pass

    # unused currently
    def attr_copy(self, src, dst):
        """ copies attributes from a dict to an object """
        [setattr(dst, attr, src[attr]) for attr in self.attrs]

    def render(self):
        lines = []
        for pre, fill, node in RenderTree(self.root_node):
            # print("%s%s" % (pre, node.name))
            lenstr = '-'
            if len(node.entries) > 0:
                lenstr = "(%d)" % len(node.entries)
            lines.append("%s%s %s" % (pre, node.name, lenstr))
        with open(self.fp.parent.joinpath('tree.txt'), 'w') as f:
            logger.debug('writing tree to %s', self.fp.parent.joinpath('tree.txt'))
            f.write('\n'.join(lines))


    #def save(self):
        #super().save()

    def write(self):
        pass

    def create_tree_from_json(self, attrs):
        pass
    def create_tree_from_kdbx(self):
        pass



    def create_tree_from_yaml(self, yaml, attrs):
        """ create a tree from a yaml """
        # if root is given in data, use it, else create a root node
        #root.mypath = 'root'
        self.attrs = attrs
        if not self.root_node:
            self.root_node = CustomNode('root')
        self.rec_yaml(yaml, self.root_node)
        logger.debug('root_node: %s', self.root_node)
        # self.render()

    def rec_yaml(self, data, node):
        """ recurse nested dict (ie from yaml) and add all content as tree descendants """
        #logger.debug("enter recursion with node name %s, path=%s", node.name, node.mypath)
        #if data:
        #logger.debug('data: %s', data)
        # [ setattr(node, attr, data.get(attr, None)) for attr in self.attrs ]

        if isinstance(data, dict):
            # logger.debug(data.keys())
            for attr in self.attrs:
                val = data.get(attr, None)
                # logger.debug('set attr %s to value %s', attr, val)
                setattr(node, attr, val)
            for key, item in data.items():
                # logger.debug('key: %s, item: %s', key, item)
                child_node = CustomNode(key, parent=node)
                child_node.title = key
                # logger.debug('child_node: %s', child_node.name)
                self.rec_yaml(item, child_node)
        else:
            #logger.debug('data NO dict : %s', data)
            if data:
                node.entries = data
                """
                for item in data:
                    e = Entry(title=item)
                    node.entries.append(e)
                """


class CustomDictExporter(DictExporter):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def export(self, node):
        return self.__export(node, dict, self.attriter, self.childiter)

    def __export(self, node, dictcls, attriter, childiter, level=1):
        #logger.debug('node: %s', node.title)
        #attr_values = attriter(self._iter_attr_values(node))
        #data = dictcls(attr_values)
        #data = dictcls()
        d = {}
        maxlevel = self.maxlevel
        if maxlevel is None or level < maxlevel:
            if node.children:
                d = dictcls()
            for child in node.children:
                d[child.name] =  self.__export(child, dictcls, attriter, childiter, level=level + 1)
        return d
