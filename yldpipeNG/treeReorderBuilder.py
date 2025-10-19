import pandas as pd
from collections import namedtuple
# from numpy.ctypeslib import load_library
from framecache_support.frameIOandCacheSupport import FrameIOandCacheSupport
from yaml_config_support.yamlConfigSupport import YamlConfigSupport

from yldpipeNG.treeReorderBase import TreeReorderBase
from yldpipeNG.treeReorderBuilderBase import TreeReorderBuilderBase
from yldpipeNG.treeReorderBuilderWanted import TreeReorderBuilderWanted
from yldpipeNG.transformFunc import TransformFunc
from yldpipeNG.fieldOps import FieldOps
from yldpipeNG.storageBroker import StorageBroker
# from SICache import SICache
from pathlib import Path
from flowpy.utils import setup_logger
# import logging
import yaml

# from .yldpipeNG.keepassReader import treeReorderReader

# from yldpipeNG.yldp_tests.creds import db_path

logger = setup_logger(__name__, __name__+'.log')
lg = setup_logger(__name__+'_2', __name__+'_2.log')
# XXX better read from txt or yaml file data_master i.e.
# change to non relative import, so other projekt like ypipe can define own
from creds import pw
# from random import randint
import platform
uname_info = platform.uname()
if uname_info.system == 'Windows':
    try:
        import win32com.client
    except:
        pass

# from treeReorderBuilderBase import fnlist


class TreeReorderBuilder(TreeReorderBase, TreeReorderBuilderWanted, TreeReorderBuilderBase, StorageBroker,
                         FrameIOandCacheSupport, TransformFunc, FieldOps, YamlConfigSupport):

    def __init__(self, app: str, data_path: str, master_config_dir='', phase=''):
        self.app = app
        self.app_type = 'tree'
        self.sub = app + '/'
        # for YamlConfigSupport
        self.phase = phase
        #self.phase_subdir = ''
        self.phase_subdir = self.phase
        # for statsSupport
        self.count_errors = 0
        self.errorlist = []

        # XXX move to start of init_framecache
        # ?? XXX make framecache an attribute of this class ?
        FrameIOandCacheSupport.__init__(self)
        logger.debug('master_config_dir: %s, subdir: %s', master_config_dir, self.sub)
        self.master_config_dir = Path(master_config_dir)
        self.config_dir = self.master_config_dir.joinpath(self.sub)
        #self.ycs_setup_base(self.config_dir)
        self.data_path = data_path
        self.debug_fn_suffixnr = 0

        # -> YamlConfigSupport
        #fnlist = self.load_config('fnlist.yml', phase_subdir=None)['fnlist']
        # fnlist currently always from main config dir
        self.fnlist = self.load_config('fnlist.yml', phase_subdir='')['fnlist']
        self.cfg_age = self.load_config_master('vals_a-g-e.yml')
        # legacy
        # XXX move to KeepassBuilderBase ? IO related
        #self.si_data = None # see SICache
        # YamlConfigSupport
        self.cache_configs(self.fnlist)

        #yaml_str = yaml.dump(self.cfg_kp_wanted_logic, default_flow_style=False, allow_unicode=True)
        #logger.debug('self.cfg_kp_wanted_logic')
        #logger.debug('\n' + yaml_str)
        # additional logic for config files
        # called from YamlConfigSupport
        #self.additional_yaml_config_logic()

        # minor stuff:
        TreeReorderBase.__init__(self)
        # XXX move to frameIOandCacheSupport ?
        self.buffer_names = {}
        # XXX why here already and separate?
        self.df_d['entries_old'] = {}
        self.buffer_names_d['entries_old'] = {}
        self.init_external_data()
        lg.debug('TreeReorderBuilder init done')

    # plugin method for YamlConfigSupport
    def additional_yaml_config_logic(self):
        # groups with own wanted_logic cfg file
        yml_list = self.config_dir.glob('group_logic_*.yml')
        done = []
        for fn in yml_list:
            group_case_name = fn.stem[12:]
            logger.debug('group_case_name: %s', group_case_name)
            groupname = group_case_name
            done.append(group_case_name)
            # XXX BUG , same key is overridden. rewrite case handling
            # Parse your YAML into a dictionary, then validate against your model.
            with open(fn) as f:
                yml = yaml.load(f, Loader=yaml.FullLoader)
            #logger.debug('groupname: %s', groupname)
            self.cfg_kp_wanted_logic['groups'][groupname] = yml
        # other groups, with simple copyall logic
        simple_list = self.cfg_kp_logic_ctrl_groups.get('loop_copyall', []) # + othres XXX
        # XXX maybe own loop for rec
        simple_list+= self.cfg_kp_logic_ctrl_groups.get('loop_copyall_rec', [])
        for fn in simple_list:
            gl = { 'group_name': {'old': fn, 'new': fn} }
            self.cfg_kp_wanted_logic[fn] = gl
        # logger.debug('HACKED %s', str(done))
            yaml_str = yaml.dump(self.cfg_kp_wanted_logic[fn], default_flow_style=False)
            logger.debug(yaml_str)

    def init_external_data(self):
        external_data_class = self.cfg_si.get('external_data_class', None)
        if external_data_class:
            si_reader1 = self.init_reader_class_by_type('csv')
            si_reader2 = self.init_reader_class_by_type('csv')

            """
            self.broker = self.st_class_factory(external_data_class, 'literal', rws='r')
            setattr(self.broker, 'cfg_si', self.cfg_si)
            self.broker.name = 'hosts_used'
            self.broker.brid = 0
            self.broker.set_reader(self.reader)

            self.broker2 = self.st_class_factory(external_data_class, 'literal')
            setattr(self.broker2, 'cfg_si', self.cfg_si)
            self.broker2.name = 'hosts_FREE'
            self.broker2.brid = 1
            reader2 = self.init_reader_class()
            reader2.set_src_dir(self.cfg_si['data_in_sub'])
            self.broker2.set_reader(reader2)
            """


    def init_storage_and_fields_TMP(self):
        # XXX move to KeepassBuilderBase
        # source DB
        db_path = self.cfg_si['data_in_sub'].joinpath(self.cfg_si['db_src'])
        # -> treeReorderBase
        self.set_src(db_path=db_path, pw=pw)
        self.set_dst(pw=self.cfg_si['pw_dst'])
        # self.create_new_etree_rec_from_dict()
        lg.info('Loaded source storage DB at %s', db_path)

        # self.create_new_etree_rec_from_dict() # veraltet.
        # XXX -> frameIOandCacheSupport
        self.build_fieldlists(self.cfg_kp_process_fields)

    def init_group_lists(self):
        # calc self.groups_old_cases
        self.calc_oldgroups_list_from_cases()
        lg.debug('self.groups_old_cases: %s', self.groups_old_cases)
        self.groups_new_crit = self.cfg_kp_logic_ctrl_groups.get('loop_crit', [])
        self.groups_new_hs = self.cfg_kp_logic_ctrl_groups.get('loop_hostspecific', [])
        self.groups_new_copyall = self.cfg_kp_logic_ctrl_groups.get('loop_copyall', [])

        # this is not a list of strings - so this makes no sense
        # self.groups_old = self.cfg_kp_logic_ctrl_groups.get('loop_copy_bypath', [])
        self.groups_old_bypath, x  = self.groups_old_from_groupmap_defined_bypath()

        # get old groups corresponding to loop_crit groups
        # include this later again

        # for now this is the same
        self.groups_old_hs = self.groups_new_hs



    # XXX just trying to get a list of the src groups = old groups for loops like dump_entries
    def groups_old_from_groupmap_defined_bypath(self):
        groups_old, groups_new = [], []
        loop_copy_bypath = self.cfg_kp_logic_ctrl_groups.get('loop_copy_bypath', [] )
        if loop_copy_bypath is not None:
            for item in loop_copy_bypath:
                # group_src = self.kp_src._find_group_by_path(item['src'])
                # group_dst = self.kp_dst._find_group_by_path(item['dst'])
                groups_old.append(item['src'])
                groups_new.append(item['dst'])
        lg.debug('groups_old: %s', groups_old)
        return groups_old, groups_new

    def allgroups_others_do(self):
        work = self.cfg_kp_logic_ctrl['work']
        logger.debug('work: %s', work)

        if 'loop_copy_bypath' in work:
            loop_copy_bypath = self.cfg_kp_logic_ctrl_groups.get('loop_copy_bypath', [])
            if loop_copy_bypath:

                for item in loop_copy_bypath:
                    group_src = self.kp_src._find_group_by_path(item['src'])
                    group_dst = self.kp_dst._find_group_by_path(item['dst'])
                    if not group_src:
                        lg.error('group_src not found: %s', item['src'])
                        continue
                    if not group_dst:
                        lg.error('group_dst not found: %s', item['dst'])
                        continue

                    lg.debug("group_src: %s, group_dst: %s", group_src.path, group_dst.path)
                    self.group_do_entries_copyall(group_src, group_dst)



        if 'loop_copyall' in work:
            group_list = self.cfg_kp_logic_ctrl_groups['loop_copyall']
            logger.debug('group_lists: %s', group_list)
            for group_name in group_list:
                # group_name is a path, XXX refactor
                #logic
                path_src = group_name  # because it is a root path
                path_entry = self.cfg_kp_pathmap_true.get(group_name)
                if path_entry is None:
                    # set to group_name as root path compponent
                    path_dst = [group_name]
                if type(path_entry) == type([]):
                    path_dst = path_entry
                lg.debug('for groupname: %s path_dst: %s', group_name, path_dst)
                group_src = self.kp_src._find_group_by_path(path_src)
                group_dst = self.kp_dst._find_group_by_path(path_dst)
                if not group_src:
                    lg.error('group_src not found: %s', group_name)
                    continue
                if not group_dst:
                    lg.error('group_dst not found: %s', group_name)
                    continue

                lg.debug("group_src: %s, group_dst: %s", group_src.path, group_dst.path)
                self.group_do_entries_copyall(group_src, group_dst)
        # XXX both  copy methods can be one DRY, flag if subgroups?

        if 'loop_copyall_rec' in work:
            group_list = self.cfg_kp_logic_ctrl_groups['loop_copyall_rec']
            lg.debug('group_lists: %s', group_list)
            for group_name in group_list:
                path = group_name
                group_src = self.kp_src._find_group_by_path(path)
                group_dst = self.kp_dst._find_group_by_path(path)
                #if (not group_src or not group_dst):
                #    continue
                if not group_src:
                    lg.error('group_src not found: %s', group_name)
                    continue
                if not group_dst:
                    lg.error('group_dst not found: %s', group_name)
                    continue
                #lg.debug('group_src: %s, group_dst: %s', group_src._path, group_dst._path)
                lg.debug("group_src: %s, group_dst: %s", group_src.path, group_dst.path)
                # lg.debug('group_src: %s, group_dst: %s', group_src.mypath, group_dst.mypath)
                lg.debug("subgroups: %s", group_src.subgroups)

                self.group_do_entries_copyall(group_src, group_dst)
                # logger.debug("subgroups: %s", group_src.subgroups)
                if group_src.subgroups:
                    for subgroup_src in group_src.subgroups:
                        lg.debug('subgroup_src.name: %s', subgroup_src.name)
                        blacklist = self.cfg_kp_logic_ctrl_groups.get('skip', [])
                        # lg.debug('blacklist: %s', blacklist)
                        if (subgroup_src.name in blacklist):
                            continue
                        subgroup_dst = self.kp_dst._find_group_by_path( [path, subgroup_src.name] )
                        #path_dst = self.cfg_kp_pathmap_true.get(group_name, group_name)
                        if subgroup_dst is None:
                            lg.error('subgroup_dst not found: %s', [path, subgroup_src.name])
                            self.count_errors += 1
                            self.errorlist.append('subgroup_dst not found: %s' % [path, subgroup_src.name])
                            continue
                        self.group_do_entries_copyall(subgroup_src, subgroup_dst)
                        lg.debug('copied subgroup: %s', subgroup_src.name)


    def group_do_entries_copyall(self, group_src, group_dst):
        lg.debug('entries count: %s', len(group_src.entries))
        df = pd.DataFrame(columns=self.frame_fields['entries_raw_table'])
        self.stats_init()
        attrs = (self.cfg_kp_process_fields['kp_pure_fields'] +
                 self.cfg_kp_process_fields['kp_same_fields'] +
                 self.cfg_kp_process_fields['kp_extra_fields'] )
        # lg.debug('attrs: %s', attrs)
        # XXX use dataframe to update the table
        for entry in group_src.entries:
        #for entry in group_src.children:
            row = {}
            for attr in attrs:
                if attr.endswith('_old'):
                    row[attr] = getattr(entry, attr[:-4])
                else:
                    row[attr] = getattr(entry, attr)
            row['group_path_new'] = group_dst.path

            ldf = len(df)
            df.loc[ldf] = row
            self.count += 1
        dt_fields = self.cfg_kp_process_fields.get('dt_fields', [])
        if dt_fields is None:
            dt_fields = []
        for dt_field in dt_fields:
            df[dt_field] = df[dt_field].dt.tz_localize(None)
        self.df_d['copyall'][group_dst.name] = df
        self.buffer_names_d['copyall'][group_dst.name] = group_dst.name
        self.stats_report(name='copyall_'+group_dst.name)


    def groups_map_new_to_old(self, group_name_new):
        # the corresponding group on the first subdir level
        logic_pathmap_backwards = self.cfg_kp_pathmap_backwards.get(group_name_new, None)
        # lg.debug('logic_pathmap_backwards : %s', logic_pathmap_backwards)
        if logic_pathmap_backwards is None:
            # lg.debug('keep name as is: %s', group_name_new)
            return group_name_new
        if 'old' in logic_pathmap_backwards.keys():
            group_name_old = logic_pathmap_backwards['old']
        else:
            group_name_old = logic_pathmap_backwards
            # lg.debug('name set to entry in subdir list: %s', group_name_old)
        return group_name_old

