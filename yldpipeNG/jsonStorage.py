from yldpipeNG.anytreeStorage import AnytreeStorage

from anytree.importer import JsonImporter

class JsonStorage(AnytreeStorage):
    """ class for caching """
    importer = JsonImporter()

    def __init__(self, data=None):
        logger.debug('JsonStorage init')
        if data is None:
            data = {}
        self.data = data
        self.fcontent = None

    def set_src(self, fp):
        self.fp = fp
        parent = dirname(fp)
        if not exists(parent):
            logger.debug('creating dir %s', parent)
            makedirs(parent)


    def read(self):
        fp = self.fp
        with open(fp, 'r') as file:
            self.fcontent = json.load(file)

    def _import(self):
        self.root = self.importer.import_(self.fcontent)
        # print(RenderTree(self.root))

    def create_tree_from_json(self, attrs):  # root case
        if self.fcontent is None:
            self.read()
        json = self.fcontent

        root_node = CustomNode("root", title="root", id=1, root='placesRoot')
        root_node.parent = None
        root_node.uri = None
        # cfg not avail in this class
        #self.attrs = list(json.keys())
        self.attrs = attrs  # ['title', 'uri']
        # logger.debug('attrs: %s', self.attrs)
        self._walk_tree(json, root_node)
        self.root_node = root_node
        self.tree = root_node
        self.render()

    def create_entry(self, json):
        entry = Entry(**json)
        return entry

    def find_groups_by_path(self, path, **kwargs):
        path = 'root/'+path
        val = path.replace('_', ' ')
        #kwargs = {'typeCode': 2}
        kwargs = {}
        return super().find_groups_by_path(val, name='mypath', **kwargs)

    def write(self):
        fp = self.fp
        with open(fp, 'w') as file:
            json.dump(self.data, file)


class FirefoxBookmarksStorage(JsonStorage):
    add_entries = False
    # for firefox bookmark json, or json with children attribute
    def _walk_tree(self, json, node):  # recursive case
        # given json fragment is checked first if it has children
        # if yes, loop over the sub json fragments, create new CustomNode and recurse
        # if not, it is a leaf node, and a new node is created
        [setattr(node, attr, json.get(attr, None)) for attr in self.attrs]
        # logger.debug('json-title: %s', json['title'])
        if 'children' in json.keys():
            for sub_json in json['children']:
                if sub_json['typeCode'] == 2:
                    title_sub = sub_json['title']
                    if title_sub == '':
                        title_sub = sub_json['guid']
                    # logger.debug('typeCode, title: %s, %s', sub_json['typeCode'], title_sub)
                    child_node = CustomNode(title_sub, parent=node)
                    self._walk_tree(sub_json, child_node)
                if sub_json['typeCode'] == 1 and self.add_entries:
                    entry = self.create_entry(sub_json)
                    node.entries.append(entry)
        else:
            pass
            # logger.debug('leaf-node: %s', json['title'])

