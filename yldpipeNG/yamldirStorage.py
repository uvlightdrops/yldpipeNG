import yaml
from yldpipeNG.customNodes import CustomNode, EquipSet
from yldpipeNG.yamlStorage import YamlStorage

from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')


#class YamldirStorage(AnytreeStorage):
class YamldirStorage(YamlStorage):

    def load(self):
        # overwriting YamlStorage.load
        attrs = []
        logger.debug('attrs: %s', attrs)
        #self.create_tree_from_yaml_dir(attrs)

    def iter_get_groups_all(self):
        [node.reset_mypath() for node in self.get_levelorderiter()]
        group_list = [node.mypath for node in self.get_levelorderiter() if isinstance(node, EquipSet)]

        g_list_l = [ sl_path.split('/') for sl_path in group_list ]
        logger.debug('g_list_l: %s', g_list_l)
        return group_list

    def create_tree_from_yaml_dir(self, attrs):
        import yamale
        self.attrs = attrs
        #path = self.cfg_si['db_src'] + '/yamldir'
        path = self.fp
        logger.debug('path: %s', path)
        ps = path.joinpath('schema.yaml')
        schema = yamale.make_schema(ps)
        #yaml_files = glob.glob(os.path.join(path, '*.yml'))
        yaml_files = path.glob('*.yml')
        logger.debug('yaml_files: %s', yaml_files)
        if not self.root_node:
            self.root_node = CustomNode('Root')

        for fn in yaml_files:  #[:10]: # os.listdir(path):
            fn_ = fn.stem
            logger.debug('\n----- fn_: %s', fn_)
            parts = fn_.split('_')
            data = yamale.make_data(fn) #, parser='ruamel')
            yamale.validate(schema, data)

            tmp = self.root_node
            # We define the path of the node in creation by checking filename
            # and split by underscore. thats find so far
            # yeah so for a node to have its equipset, you need an own file for it. also ok
            # AND when is is mypath
            while len(parts) > 1:
                # logger.debug('parts: %s', parts)
                p_name = parts.pop(0)
                node = None
                # logger.debug('tmp.children: %s', tmp.children)
                # The child node might exist already - we loop files in a flat dir
                # but we need this recursivly
                for child in tmp.children:
                    if child.name == p_name:
                        logger.debug('found child: %s', child.name)
                        node = child
                        break   # quit the for loop, we found our tree node

                # if node was not yet in current children, we need to create it
                if not node:
                    node = CustomNode(p_name, parent=tmp)
                    # XXX test
                    # this seems not to be needed, i dont understand yet
                    # logger.debug(node.mypath)
                    # node.reset_mypath()
                    logger.debug('created %s - name %s, %s', node, p_name, node.mypath)

                # if there are parts left we need to go deeper with a new parent
                tmp = node

            leaf_name = parts.pop(0)

            with open(path.joinpath(fn)) as f:
                yaml_d = yaml.load(f, Loader=yaml.FullLoader)
                es = EquipSet(leaf_name, yaml_d=yaml_d)
                es.parent = tmp
                # after assigning parent we can calulcate the path from root
                es.reset_mypath()
                # logger.debug('es: %s mypath %s', es.name, es.mypath)

        self.render()
