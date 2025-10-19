import os.path
from shutil import copyfile
from yldpipeNG.anytreeStorage import AnytreeStorage
import logging, sys
import itertools
import random, string
from flowpy.utils import setup_logger
from yldpipeNG.anytreeStorage import CustomNode
import pandas as pd
logger = setup_logger(__name__, __name__+'.log', level=logging.DEBUG)
from pykeepass import PyKeePass

class keepassNode(CustomNode):
    # subnodes in anytree are called children
    # entries is something different, a item of content
    """
    @property
    def children(self):
        return self.entries
    @children.setter
    def children(self, val):
        self.entries = val
    """

# The storage classes should not know about the source/dest meaning of the treeReorderBuilder
class KdbxStorage(AnytreeStorage, PyKeePass):

    def __init__(self, *args, **kwargs):
        AnytreeStorage.__init__(self)
        self._args = args
        self._kwargs = kwargs.copy()
        kwargs.pop('pw', None)
        logger.debug('KdbxStorage init, args: %s, kwargs: %s', args, kwargs)
        #if 'filename' in kwargs:
        #    self.filename = kwargs['filename']

    def do_save(self):
        logger.debug('saving to %s', self.db_path_abs)
        PyKeePass.save(self, filename=self.db_path_abs)

    # should get absolute path
    def set_src(self, db_path):
        self.db_path_abs = db_path
        filename = db_path.name
        logger.debug('filename: %s', filename)
        password = self._kwargs.get('pw', None)
        logger.debug('db_path: %s', db_path)
        PyKeePass.__init__(self, self.db_path_abs, password=password)
        #logger.debug('type: %s', self.src_or_dst)
        #logger.debug('groups: %s', self.groups)

    ###
    def generate_pykeepass_tree(self):
        # generate pykeepass tree from anytree tree
        self.root_group.name = 'root'
        logger.debug('self.root_group: %s', self.root_group)
        self._rec_anytree_create_pykeepass_tree(self.root_node, self.root_group)

    def _rec_anytree_create_pykeepass_tree(self, node, group):
        # logger.debug('node: %s', node)
        if node.children == []:
            return
        else:
            for child in node.children:
                # logger.debug('child: %s', child)
                subgroup = self.add_group(group, child.name)
                #subgroup.entries = child.entries
                self._rec_anytree_create_pykeepass_tree(child, subgroup)

    ###
    def create_tree_from_kdbx(self):
        self.root_node = keepassNode('/')
        #self.root_node = keepassNode('Root')
        self.root_node.entries = []
        self._rec_tree(self.root_node, self.root_group)
        # self.render()

    def _rec_tree(self, node, group):
        if group.subgroups == []:
            return
        else:
            #logger.debug('node: %s', node.children)
            for subgroup in group.subgroups:
                subnode = keepassNode(subgroup.name)
                subnode.entries = subgroup.entries
                temp = list(itertools.chain(node.children, [subnode]))
                node.children = temp
                self._rec_tree(subnode, subgroup)

    #def save(self):
        # save anytree structure to a pykeepass structure
    #    pass

    """
    def prepare_export_from_anytree(self, fp, format='yaml'):
        self.create_tree_from_yaml(self.data)

    def prepare_export(self, fp, format='yaml'):
        # walk pykeepass tree and prepare for export 
        logger.debug('self.root_group: %s', self.root_group)
        self.data = self._rec_groups(self.root_group) #, data)
        logger.debug('self.data: %s', self.data)

    def _rec_groups(self, node): #, data):
        data = {}
        if True:
            for subgroup in node.subgroups:
                #data.append( { node.name: res } )
                data[subgroup.name] = self._rec_groups(subgroup)  # , data)
            return data
    """

    def _find_group_by_path(self, path, **kwargs):
        import ast
        # XXX try to get rid of ast
        # path is allowed be string or list
        #logger.debug('path: %s, type(path): %s', path, type(path))
        if type(path) == type(''):
            if '[' in path:
                lpath = ast.literal_eval(path)
            else:
                lpath = [path]
        if type(path) == type([]):
            #if len(path) == 1:
            lpath = path
        logger.debug('sd: %s . path: %s => %s', self.src_or_dst, path, lpath)
        res = PyKeePass.find_groups(self, path=lpath, name=lpath[-1], **kwargs)
        #res = PyKeePass.find_groups_by_path(self, lpath)
        # XXX check for type or exception handling?
        if res is None:
            logger.warning('in %s group not found: %s', self.src_or_dst, path)
        else:
            pass
            logger.debug('res: %s', res)
        return res

    def _find_group(self, recursive=True, path=None, group=None, **kwargs):
        logger.debug('path: %s, group: %s', path, group)
        sys.exit()
        res = self.find_groups(recursive=recursive, path=path, group=group, **kwargs)
        # logger.debug('res: %s', res)
        if res is None:
            logger.warning('in %s group not found: %s', self.src_or_dst, path)
            raise Exception('DEBUG break')
        return res[0]

    def _find_entry_by_path(self, path):
        # logger.debug('path: %s', path)
        res = self.find_entries(recursive=True, path=path, **kwargs)
        if res:
            if type(res) == type([]):
                return res[0]
            elif type(res) == type(''):
                return res
            else:
                logger.warning('entry with path %s not found', path)

    def faker(self):
        length = random.randint(6, 10)
        letters_and_digits = string.ascii_letters + string.digits
        random_string = ''.join(random.choice(letters_and_digits) for i in range(length))
        return random_string

    def _add_entry(self, group, row) -> bool:
        attrs_req = [] #'password']
        attrs_opt = ['username', 'title', 'password', 'notes', 'url']
        for attr in attrs_req:
            if row[attr] is None:
                logger.error('attr is None: %s', attr)
                self.count_errors += 1
                self.errorlist.append('attr is None: %s - %s' %(attr, row['title']))
                return False

        if (row['username'] is None) and (row['title'] is None):
            self.count_errors += 1
            logger.error('username AND title is None ')
            self.errorlist.append('username and title is None')
            return False

        # XXX get rid of pandas here! move to treeBuilderBase
        for attr in attrs_opt:
            # if row[attr] is None:
            # logger.debug(type(row[attr]))
            if pd.isna(row[attr]) or row[attr] == 'nan':
                # row[attr] = ''
                row[attr] = self.faker()

        # logger.debug('row: %s', row)
        #if True:
        try:
            import datetime
            #logger.debug(row)
            entry = self.add_entry(group,
                row['title'],
                row['username'],
                row['password'],
                url = row['url'],
                notes = row['notes'],
                # XXX error in excel write / devel
                # expiry_time = row['expiry_time'],
                expiry_time = datetime.datetime(2026, 12, 31, 23, 0, 0),
                tags = row['tags'],
                otp = row['otp'],
                icon = row['icon'],
                )
            # logger.debug('row : expires, expiry_time : %s, %s', row['expires'], row['expiry_time'])
            # logger.debug('OK copied: %s, %s, %s ', entry.title, entry.expires, entry.expiry_time)
            if row['expires'] == True:
                entry.expires = True
            else:
                entry.expires = False
            return True
        except Exception as e:
        #else:
            logger.error('exception occurred: %s', e)
            self.count_errors += 1
            logger.error("NOT ADDED: values: %s ", str([str(x) for x in row.values ]) )
            #logger.error("NOT ADDED: values: %s ", str([str(x) for x in row.values() ]) )
            self.errorlist.append('FAIL: %s \t %s' %(row['title'],row['username']))
            return False

        return None
