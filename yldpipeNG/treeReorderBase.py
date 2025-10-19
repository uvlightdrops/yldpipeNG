from shutil import copyfile
import os
from flowpy.utils import setup_logger
import logging

#from framecache_support.dataBroker import DataBroker
from yldpipeNG.storageBroker import StorageBroker
from yldpipeNG.statsSupport import StatsSupport

logger = setup_logger(__name__, __name__+'.log')
lg = setup_logger(__name__+'_2', __name__+'_2.log')

# XXX naming TreeStorageBase ?
class TreeReorderBase(StorageBroker, StatsSupport):  # YamlConfigSupport):

    def __init__(self):
        # set in __init__ of TreeReorderBuilder
        self.dt_fn = 'kp_tree_team.yml'
        # self.dt_d = self.load_config(self.dt_fn)
        logger.info("-----------------------------------------------")

    def set_src(self, db_path, pw):
        lg.debug('db_path: %s', db_path)
        self.cfg_si['pw'] = pw
        fp = self.cfg_si['data_in_sub'].joinpath(self.cfg_si['db_src'])
        self.kp_src = self.init_storage_src_class()
        setattr(self.kp_src, 'cfg_si', self.cfg_si)
        # logger.debug(str(self.cfg_si.items()))
        self.kp_src.set_src(fp)

        attrs = self.cfg_kp_process_fields['kp_old_fields'] + self.cfg_kp_process_fields['kp_same_fields']
        # if not json, unused in base class AnytreeStorage
        self.kp_src.create_tree_from_json(attrs)
        self.kp_src.create_tree_from_kdbx()
        # should move some time , too specific XXX
        if self.cfg_profile['storage_src'] == 'yamldir':
            self.kp_src.create_tree_from_yaml_dir(attrs)
        #self.kp_src._import()
        # export discovered tree to yaml
        export_formats = self.cfg_si.get('tree_original_export_formats', [])
        for format in export_formats:
            fn = "%s_%s.%s" % (self.cfg_si['tree_original_export_fn'], format, format )
            fp = self.cfg_si['data_out_sub'].joinpath(fn)
            self.kp_src.export(fp, format=format)

    def set_dst(self, pw):
        self.kp_dst = self.init_storage_dst_class()
        db_path_abs = self.cfg_si['data_out_sub'].joinpath(self.cfg_si['db_dst'])
        self.cfg_si['pw'] = pw
        setattr(self.kp_dst, 'cfg_si', self.cfg_si)
        # if not os.path.exists(db_path_abs):
        #    logger.error('file not found: %s', db_path_abs)
        # ypipe workaround
        if True:
        #if 'copy_empty_db' in self.cfg_kp_logic_ctrl['work']:
            db_path_empty = self.cfg_si['data_out_sub'].joinpath(self.cfg_si['db_file_empty'])
            db_path_dst   = db_path_abs
            copyfile(db_path_empty, db_path_dst)
            logger.debug('copied empty db to: %s', db_path_dst)

        self.kp_dst.set_src(db_path_abs)

        # if there is a new manually edited hierarchy given from config
        # ypipe workaround
        if True:
        #if 'init_tree_from_yaml_config' in self.cfg_kp_logic_ctrl['work']:
            # fp = self.config_dir.joinpath(self.dt_fn)
            yaml = self.cfg_kp_tree_team
            self.kp_dst.set_hierarchy_from_yaml_string(yaml)

            self.init_tree_from_yaml_config()


    def init_tree_from_yaml_config(self):
        attrs = self.cfg_kp_process_fields['kp_same_fields']
        self.kp_dst.create_tree_from_yaml(self.kp_dst.yaml, attrs)
        # self.kp_dst.do_save()  # makes no sense here?
        self.kp_dst.export(self.cfg_si['data_out_sub'].joinpath('export.yml'), format='yaml')
        lg.debug('self.kp_dst.root_group: %s', self.kp_dst.root_group)
        self.kp_dst.generate_pykeepass_tree()
        # self.kp_dst.do_save()


    def add_df_to_new_tree(self, df):
        lg.debug('len(df): %s', len(df))
        for i, row in df.iterrows():
            # logger.debug('group_path_new: %s', row['group_path_new'])
            group_dst = self.kp_dst._find_group_by_path(row['group_path_new'])
            if group_dst is None:
                logger.error('group_dst is None: %s', row['group_path_new'])
            self.prepare_and_add(group_dst, row)

    """
    # XXX Is this use at all?
    # XXX provide df as argument to method ?
    def add_to_new_tree(self, case_name):
        attr_needed = ['group_path_new', 'title_new', 'username_new', 'password', 'url', 'notes', 'fk']
        df = self.df_wanted[case_name]
        df_found = df[df['status'].str.contains('FOUND')]
        lg.debug('len(df): %s, len(df_found): %s', len(df), len(df_found))

        for i, row_ser in df_found[attr_needed].iterrows():
            row = row_ser.to_dict()
            group_dst = self.kp_dst._find_group_by_path(row['group_path_new'])
            # logger.debug(row)
            if group_dst is None:
                logger.error('group_dst is None: %s', row['group_path_new'])
            self.prepare_and_add(group_dst, row)
            # logger.debug('group_path_new: %s, group_dst: %s', row['group_path_new'], group_dst)
    """


    def prepare_and_add(self, group_dst, row):
        row['title'] = row['title_new']
        row['username'] = row['username_new']
        #logger.debug(row['title'])
        self.kp_dst._add_entry(group_dst, row)

    # def create_new_etree_rec_from_dict(self, data):
    def create_new_etree_rec_from_dict(self):
        """ create new kp tree and walk tree to work on entry transfer """
        # copy empty DB file to dest db file, for fresh start
        db_path_empty = self.config_dir.joinpath(self.cfg_si['db_file_empty'])
        lg.debug('db_path_empty: %s', db_path_empty)
        db_path_dst = self.cfg_si['data_out_sub'].joinpath(self.cfg_si['db_dst'])
        db_path_dst_s = str(db_path_dst)
        copyfile(db_path_empty, db_path_dst)


