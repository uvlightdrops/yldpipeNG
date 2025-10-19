import pandas as pd
import re
import string
# import datetime
import random

from flowpy.utils import setup_logger
from yldpipeNG.customNodes import EquipSet

logger = setup_logger(__name__, __name__+'.log')
lg = setup_logger(__name__+'_2', __name__+'_2.log')

class TreeReorderBuilderWanted:

    def init_members(self):
        self.step_skip = self.cfg_kp_logic_ctrl['step_skip']
        #self.groups_new_hs
        #self.group_new_crit
        #self.groups_old

    # we want a method just to take list of groups / group_paths
    # and loop the dumping method
    # these are always "old" groups from the source tree
    def allgroups_dump_entries(self):
        lg.debug('Entering allgroups_dump_entries')
        work = self.cfg_kp_logic_ctrl.get('work', [])
        # is this crit groups?
        if 'dump_case_groups' in work:
            table_def_name = 'entries_old_table'
            self.dump_group_entries_NEW(self.groups_old_cases, table_def_name)

        if 'dump_wanted' in work:
            table_def_name = 'entries_old_table'
            self.dump_group_entries_NEW(self.groups_old_cases, table_def_name)
            #self.dump_group_entries_NEW(self.groups_old, table_def_name)

        if 'dump_copyall' in work:
            table_def_name = 'entries_old_copy_table'
            self.dump_group_entries_NEW(self.groups_new_copyall, table_def_name)

        if 'dump_hostspecific' in work:
            table_def_name = 'entries_old_table'
            self.dump_group_entries_NEW(self.groups_old_hs, table_def_name)

    def dump_group_entries_NEW(self, group_list, table_def_name):
        # is group_list a list of group paths?
        lg.debug('group_list: %s', group_list)
        self.frame_fields_cur = self.frame_fields[table_def_name]
        for group_name in group_list:
            lg.debug('group_name: %s', group_name)
            self.dump_group_entries(group_name, 'not needed')

    def dump_group_entries(self, group_name_old, group_name_new):
        # group_name_old is meanwhile rather a group identificator, can be a list / path (useful?)
        kp_pf = self.cfg_kp_process_fields
        lg.debug('___Entering. group_name_old: %s', group_name_old)

        group_obj_old = self.kp_src._find_group_by_path(group_name_old)
        if group_obj_old is None:
            lg.error('find groups returns None for %s', group_name_old)
            return  # skip this groupname

        # XXX need to go elsewhere
        if isinstance(group_obj_old, EquipSet):
            attrs = self.cfg_kp_process_fields['kp_old_fields']
            df = pd.DataFrame(columns=attrs)
            group_obj_old.dump_entries(attrs, df)
            # group_id = '_'.join(group_name_old)
            # is now repr method of ES
            group_id = group_obj_old.repr
            lg.debug('group_id: %s, ES mypath: %s', group_id, group_obj_old.mypath)
            self.df_d['entries_old'][group_id] = df.fillna('')
            self.buffer_names_d['entries_old'][group_id] = group_id
            lg.debug(len(df))
            return


        # attrs = kp_pf['kp_old_fields'] + kp_pf['kp_same_fields']
        # All attributes of ususal keepass entry
        attrs_entries_old = kp_pf['kp_pure_fields'] + kp_pf['kp_same_fields'] + kp_pf['kp_extra_fields']
        df_entries = pd.DataFrame(columns=attrs_entries_old)

        if not 'check_entry_for_prominent_terms' in self.cfg_kp_logic_ctrl['step_skip']:
            df_entries_tagged = pd.DataFrame(columns=self.frame_fields_cur)

        self.stats_init()
        self.stats_df_init()

        entries = group_obj_old.entries
        lg.debug('len entries: %s', len(entries))

        for entry in entries:
            row = {}
            row['status'] = 'NEW'
            row['status_info'] = ''
            # logger.debug('entry title: %s', entry.title)
            # discover termn in all entries of current group
            if not 'check_entry_for_prominent_terms' in self.cfg_kp_logic_ctrl['step_skip']:
                row_tagged = self.check_entry_for_prominent_terms(entry, row)
                # row_tagged['sig_app'] = role_prefix
            # XXX can check_entry_for_prominent_terms be after the old val assertion?

            # XXX not very exact
            # get old attributes, their original names of course miss the _old suffix
            for attr in self.cfg_kp_process_fields['kp_old_fields']:
                row[attr] = getattr(entry, attr[:-4])
                # logger.debug('OLD attr: %s, val: %s', attr, row[attr])

            for attr in attrs_entries_old:
                row[attr] = getattr(entry, attr)

            if 'path_old' in row:
                row['path_old'] = str(row['path_old'])  # ahy? XXX dont need

            row['pk'] = self.count
            self.count += 1
            ldf = len(df_entries)

            df_entries.loc[ldf] = row
            df_entries_tagged.loc[ldf] = row_tagged

        self.stats_report(name='10_dump_'+group_name_old)

        ## df_entries = df_entries.sort_values(by=self.cfg_kp_wanted_logic['sort'])
        self.df_d['entries_old'][group_name_old] = df_entries.fillna('')
        self.buffer_names_d['entries_old'][group_name_old] = group_name_old

        if not 'check_entry_for_prominent_terms' in self.cfg_kp_logic_ctrl['step_skip']:
            self.df_d['entries_old_tagged'][group_name_old] = df_entries_tagged.fillna('')
            self.buffer_names_d['entries_old_tagged'][group_name_old] = group_name_old
        lg.debug('group: %s', group_name_old)

    def allgroups_old_bycases_remove_unknown(self):
        for group_name_old in self.groups_old_cases:
            # if both steps are skipped we copy the dataframe to the next steps index "entr_old_tag_unknown_removed"
            # XXX irritating
            lg.debug('copy df (entr_old_tag_unknown_removed) of group %s', group_name_old)
            self.df_d['entr_old_tag_unknown_removed'][group_name_old] = self.df_d['entries_old_tagged'][group_name_old].copy()
            self.buffer_names_d['entr_old_tag_unknown_removed'][group_name_old] = group_name_old
            #continue
            """
            """

            lg.debug('group_name_old: %s', group_name_old)
            if not 'remove_unknown_entries' in self.step_skip:
                self.remove_entries_bystatus(group_name_old,'NOT IN USE', 'entries_old_tagged', 'eo2')
                self.remove_entries_bystatus(group_name_old,'unknown', 'eo2', 'eo3')

            if not 'remove_sandbox_entries' in self.step_skip:
                self.remove_entries_bystatus(group_name_old,'sandbox', 'eo3', 'entr_old_tag_unknown_removed')

    def calc_oldgroups_list_from_cases(self, case_name_list=None):
        lg.debug('case_name_list: %s', case_name_list)
        if case_name_list is None:
            # case_name_list = self.cfg_kp_logic_ctrl_groups.get('loop_copy_bypath', [])
            case_name_list = self.cfg_kp_logic_ctrl_groups.get('loop_crit', [])
            case_name_list += self.cfg_kp_logic_ctrl_groups.get('loop_hostspecific', [])

        lg.debug('case_name_list: %s', case_name_list)
        self.groups_old_cases = []

        for case_name in case_name_list:
            logic = self.cfg_kp_wanted_logic[case_name]
            group_name_old = logic['group_name']['old']
            lg.debug('found old group named %s', group_name_old)
            if group_name_old not in self.groups_old_cases:
                self.groups_old_cases.append(group_name_old)


    # XXX rename : just allgroups_do_cases
    # still master method
    def allgroups_age_do_cases(self, groups_new):
        # import yaml
        # self.calc_oldgroups_list_from_cases(case_name_list=groups_new)
        lg.debug('groups_new: %s', groups_new)
        # lg.debug('oldgroups_list: %s', self.oldgroups_list)
        self.step_skip = self.cfg_kp_logic_ctrl['step_skip']
        # make a copy so we can remmeber the orig
        # tmp_list = self.oldgroups_list

        for case_name in groups_new:
            logic = self.cfg_kp_wanted_logic[case_name]
            # lg.debug('logic for %s = %s', case_name, logic)
            #lg.debug(self.cfg_kp_wanted_logic[case_name])
            group_name_old = logic['group_name']['old']
            group_name_new = logic['group_name']['new']
            lg.debug('group_name_new: %s from old %s', group_name_new, group_name_old)
            group_obj_old = self.kp_src._find_group_by_path(group_name_old)
            if group_obj_old is None:
                lg.error('group not found: %s', group_name_old)
                continue
            # XXX we could init dump_entries here instead of at its own method
            # looping case names here
            if not 'generate_wanted_table' in self.step_skip:
                if logic['type']=='crit':
                    self.group_generate_wanted_table(group_obj_old, group_name_new, case_name, logic)
                if logic['type']=='hostspecific':
                    self.group_generate_wanted_table_hostspecific(group_obj_old, group_name_new, logic)

            # not correct here, it is about old groups


    def allgroups_old_add_role_index(self):
        for group_name_old in self.groups_old_cases:
            lg.debug('META add role_index: %s', group_name_old)
            if not 'ri_by_host' in self.step_skip:
                # ri by hostname solves many
                self.group_ri_by_hostname(group_name_old)

            if not 'add_ri_to_oldentries' in self.step_skip:
                # the rest, also good
                self.group_add_ri_to_oldentries(group_name_old)

    def remove_entries_bystatus(self, group_name_old, status_unwanted, tkey_src, tkey_dst=None):
        if tkey_dst is None:
            tkey_dst = tkey_src
        lg.debug('tkey_src: %s, self.df_d.keys(): %s', tkey_src, self.df_d[tkey_src].keys())
        df = self.df_d[tkey_src][group_name_old] # #.copy()
        self.df_d[tkey_dst][group_name_old] = df[ df['status'] != status_unwanted ]
        self.buffer_names_d[tkey_dst][group_name_old] = group_name_old
        lg.debug('len df: %s after rm %s', len(df), status_unwanted)
        # return df

    def allgroups_do_cases_merge(self, groups_new):
        step_skip = self.cfg_kp_logic_ctrl['step_skip']

        for case_name in groups_new:
            logic = self.cfg_kp_wanted_logic[case_name]
            group_name_old = logic['group_name']['old']
            group_name_new = logic['group_name']['new']
            group_obj_old = self.kp_src._find_group_by_path(group_name_old)
            if group_obj_old is None:
                lg.error('group not found: %s', group_name_old)
                continue

            # merge old and wanted by role_index / hostname
            if not 'merge' in step_skip:
                lg.debug('[merge] group_name_new: %s to old %s', group_name_new, group_name_old)
                if logic['type'] == 'crit':
                    self.group_match_by_serverlist_tags(group_obj_old.name, group_name_new, case_name)
                if logic['type'] == 'hostspecific':
                    self.group_match_by_hostname(group_name_old, group_name_new, case_name)

            if not 'calc_and_update_title_and_username' in step_skip:
                if logic['type'] == 'crit':
                    self.calc_and_update_title_and_username(group_name_old, group_name_new, case_name, logic)

            if not 'insert_into_tree' in step_skip:
                self.add_df_to_new_tree(self.df_d['for_insert'][case_name])
            if not 'insert_dummy_into_tree' in step_skip:
                self.add_df_to_new_tree(self.df_d['wanted_as_dummy'][case_name])
            self.buffer_names[case_name] = case_name


    def group_generate_wanted_table_hostspecific(self, group_obj_old, group_name_new, group_logic):
        fieldnames = self.frame_fields['wanted_hostspecific_table']
        df = pd.DataFrame(columns=fieldnames)
        if group_logic.get('generate_wanted') == False:
            self.df_d['wanted'][case_name] = df
            return
        self.stats_init()
        items = group_logic.get('items')
        # suffix = group_logic.get('suffix')
        attrs_new_pat_d = group_logic.get('attrs_new_pat_d')
        #hostname_list = self.broker.call_method('get_all_of_x', 'hostname')
        hostname_list = self.broker.call_method('get_attr_all_of_x', self.cfg_si['host_field_name'])
        lvlrow = {
            'group_new': group_name_new,
            'group_path_new': [group_name_new],
            'group_old': group_obj_old.name,
            'status': 'NEW',
            'app': group_logic.get('app'),
        }
        for hostname in hostname_list:
            for item in items:
                row = lvlrow.copy()
                row['hostname'] = hostname[7:14]
                row['vm'] = hostname[7:14]
                row['host'] = hostname
                row['item'] = item
                # logger.debug('hostname: %s', row['hostname'])
                #data = self.broker.call_method('get_data_for_host', row['hostname'])
                # logger.debug('data: %s', data)
                #if data is not None:
                    # logger.debug('data: %s', data['Rolle'])
                #    row['role_index'] = data['Rolle']

                row['fk'] = self.count
                #row['title_new'] = '%s %s' % (item, hostname)
                for key, anp_item in attrs_new_pat_d.items():
                    # logger.debug('check-item: %s -- key: %s, anp_item: %s', item, key, anp_item)
                    if 'all_items' in attrs_new_pat_d[key].keys():
                        pattern = attrs_new_pat_d[key]['all_items']
                    else:
                        pattern = anp_item.get(item, {} )
                    # logger.debug('pattern: %s', pattern)
                    result_substd = string.Template(pattern).substitute(row)
                    # logger.debug('result_substd: %s', result_substd)
                    row[key] = result_substd
                    self.count += 1
                self.count_suc += 1
                ldf = len(df)
                df.loc[ldf] = row

        self.stats_report(name='30_generate_wanted_table_hs'+group_name_new)
        lg.debug('len df: %s', len(df))
        self.buffer_names_d['wanted'][group_name_new] = group_name_new
        self.df_d['wanted'][group_name_new] = df.sort_values(by='hostname')  # df.fillna('')

        if 'wanted_as_dummy_entries' in self.cfg_kp_logic_ctrl['work']:
            copy_of_df = self.df_d['wanted'][group_name_new].copy()
            self.df_d['wanted_hs_as_dummy'][group_name_new] = self.add_necessary_cols_to_df(copy_of_df)
            self.buffer_names_d['wanted_hs_as_dummy'][group_name_new] = group_name_new


    def group_generate_wanted_table(self, group_obj_old, group_name_new, case_name, group_logic):
        fieldnames = self.frame_fields['wanted_table']
        df = pd.DataFrame(columns=fieldnames)
        generate_wanted = group_logic.get('generate_wanted', True)
        if generate_wanted == False:
            self.df_d['wanted'][case_name] = df
            return

        items = group_logic.get('items')
        # lg.debug('items: %s', items)
        # itemmap = group_logic.get('map') #get or {}
        app = group_logic.get('app')
        dst = group_logic.get('dst')
        group_path_new = [group_name_new]
        lg.debug('case_name: %s => group_path_new: %s', case_name, group_path_new)
        # dst is a optional destination subgroup
        age_pattern = group_logic.get('age')
        age_template = string.Template(age_pattern)
        sub_all = group_logic.get('sub_all')
        attrs_new_pat_d = group_logic.get('attrs_new_pat_d')
        #lg.debug('attrs_new_pat_d: %s', attrs_new_pat_d)
        behoerde = ''
        self.stats_init()
        for crit in self.cfg_age['env']:
            lvlrow = {
                'crit': crit,
                'app': app,
                'group_new': group_name_new,
                'group_old': group_obj_old.name,
                'status': 'NEW',
            }
            for item in items:
                row = lvlrow.copy()
                for sub in sub_all:
                    if group_logic['loop_gericht']:
                        behoerde = sub
                    # logger.debug('sub: %s', sub)
                    roledict = {'app': app, 'behoerde': behoerde, 'crit': crit}
                    role = age_template.substitute(roledict)
                    # logger.debug('role: %s', role)
                    hostname_list = self.broker.call_method('get_data_for_one', role)
                    if hostname_list:
                        row['hostname'] = hostname_list[0] # XXX this is not exact, info is lost
                        row['vm'] = row['hostname'][7:]
                    else:
                        row['hostname'] = 'host-NA'
                        row['vm'] = 'vmname-NA'
                    # logger.debug('hostname_list: %s', hostname_list)
                    row['hostname_list'] = hostname_list
                    row['behoerde'] = sub  # XXX use behoerde here
                    row['item'] = item
                    # row['mapped'] = itemmap.get(item, item)
                    row['fk'] = self.count
                    row['role_index'] = '%s_%s' % (role, item)
                    # logger.debug('role_index: %s', row['role_index'])
                    #row['title_new'] = '%s %s' % (crit, item)
                    for key, anp_item in attrs_new_pat_d.items():
                        #logger.debug('check-item: %s -- key: %s', item, key)
                        if 'all_items' in attrs_new_pat_d[key].keys():
                            pattern = attrs_new_pat_d[key]['all_items']
                        else:
                            pattern = anp_item.get(item, '' )
                        row[key] = string.Template(pattern).substitute(row)
                        #row[key+'_new'] = string.Template(pattern).substitute(row)

                    # for bmarks_eip
                    if group_logic.get('use_subgroups', None):
                        group_path_new = [group_name_new, sub]
                    else:
                        if dst:
                            group_path_new = [group_name_new, dst]

                    # logger.debug('group_path_new: %s', group_path_new)
                    row['group_path_new'] = group_path_new
                    self.count += 1
                    self.count_suc += 1
                    ldf = len(df)
                    df.loc[ldf] = row
        self.stats_report(name='30_generate_wanted_table_'+case_name)
        # df_wanted = self.df_wanted[case_name].copy()
        # Apply the function to each row and create a new column
        # XXX outcomment for debugging the other call

        # self.stats_init()
        # 03-17 , calc role_index in loop
        # df['role_index'] = df.fillna('').apply(self.calc_role_index, sig='', axis=1)
        # self.stats_report(name='33_wanted_calc_roleindex_'+case_name)

        # df_wanted = self.df_wanted[case_name].copy()
        self.df_d['wanted'][case_name] = df.sort_values(by='role_index')  # df.fillna('')
        self.buffer_names_d['wanted'][case_name] = case_name
        lg.debug('len df wanted: %s', len(self.df_d['wanted'][case_name]))

        if 'wanted_as_dummy_entries' in self.cfg_kp_logic_ctrl['work']:
            copy_of_df = self.df_d['wanted'][case_name].copy()
            self.df_d['wanted_as_dummy'][case_name] = self.add_necessary_cols_to_df(copy_of_df)
            # lg.debug(self.df_d['wanted_as_dummy'][case_name].head())
            self.buffer_names_d['wanted_as_dummy'][case_name] = case_name


    # to be used with DataFrame.apply() or directly
    # checking if all sig fields are defined and combine to role_index
    def calc_role_index(self, row, sig='', sig_item='', caller=None) -> str:
        if caller:
            pass
            # logger.debug('caller: %s ', caller)
        if 'role_index' in row:
            if row['role_index'] != '':
                logger.debug('alrdy set: %s - hn=%s', row['role_index'], row['sig_hostname'])
                return row['role_index']
        # xxx use template here
        item_key = sig_item + 'item'
        req_fields = [(sig+var) for var in self.cfg_kp_process_fields['kp_si_fields']]
        # logger.debug("%s_%s_%s_%s", row[req_fields[0]], row[req_fields[1]], row[req_fields[2]], row[item_key])

        if 'sig_app' in row and row['sig_app'] == 'eKP':
            # later added, for eKP no behoerde is needed
            if row[req_fields[1]] != 'FachG':
                logger.debug('LAST non FachG')
                row[req_fields[1]] = 'FachG'

        # Check if all fields are defined, only then it returns a useful role index
        all_defined, ok = False, False
        if all(((row[field] is not None) and (row[field] != '')) for field in req_fields):
            all_defined = True
        # logger.debug(row.index)

        self.stats_init()
        role_index = "%s_%s_%s_%s" % (row[req_fields[0]], row[req_fields[1]], row[req_fields[2]], row[item_key])
        if all_defined or ok:
            logger.debug('====== role_i set to : %s ============', role_index)
            self.count_suc += 1
            return role_index
        # if not all parts are defined the role_index should be empty string
        else:
            self.count_err += 1
            # logger.debug('role_index NOT set - %s', role_index)
            return ''
        self.stats_report(name='33_calc_ri')


    def group_ri_by_hostname(self, group_name_old):
        """ find Role by hostname, then concat SI Rolle and Item """

        id_field_name = self.cfg_si.get('id_field_name', 'abc_id')
        df = self.df_d['entr_old_tag_unknown_removed'][group_name_old].copy()

        for i, row in df.iterrows():
            if not row['sig_hostname']:
                # logger.warning('sig_hostname is empty - title is %s', row['title_old'])
                continue

            data = self.broker.call_method('get_data_for_host', row['sig_hostname'])
            if data == {}:
                logger.warning('data is empty for %s', row['sig_hostname'])
                data2 = self.broker2.call_method('get_data_for_host', row['sig_hostname'])
                logger.debug('data2: %s', data2)
                if data2 == {}:
                    logger.error('data and data2 is empty for %s', row['sig_hostname'])
                    continue
                # mark data as unused hosts
                row['status'] = 'NOT IN USE'
                # from here use data2 as data
                data = data2

            logger.debug('data:  %s', data[id_field_name])
            # marker = 'N.A.'
            if row['role_index'] != '':
                pass
                # logger.debug('role_index already set: %s', row['role_index'])
            else:
                # logger.debug('sig-Item: %s', row['sig_item'])
                if data[id_field_name] and row['sig_item']:
                    # the simple RI concatenation
                    ri_new = '%s_%s' % (data[id_field_name], row['sig_item'])
                    logger.debug('RI set to: %s', ri_new)
                    if len(ri_new.split('_')) == 3:
                        logger.debug(row)
                    row['role_index'] = ri_new
                    note = 'RI-hn'
                    if row['status_info'] != '':
                        note = row['status_info'] + ', ' + note
                    row['status_info'] = note
                else:
                    # XXX pass is suff,
                    row['role_index'] = '' # marker
            # logger.debug('role_index: %s', row['role_index'])

            df.loc[i] = row

#         self.prep_debug_table(df, group_name_new, 'ri_by_hostname', attrs_groups=['kp_same'])
        # Update the current data
        self.df_d['entr_old_tag_unknown_removed'][group_name_old] = df

    def group_add_ri_to_oldentries(self, group_name_old):
        #lg.debug(self.df_d['entr_old_tag_unknown_removed'].keys())
        # XXX filter for existing role index entries first
        df = self.df_d['entr_old_tag_unknown_removed'][group_name_old].copy()

        for index, row in self.df_d['entr_old_tag_unknown_removed'][group_name_old].iterrows():
            # row['sig_app'] = role_prefix
            role_index = self.calc_role_index(row, sig='sig_', sig_item='sig_', caller='add_ri')  # LEVEL 2
            if len(role_index.split('_')) == 3:
                logger.debug(row)
            if role_index != '':
                logger.debug('set RI=%s', role_index)
                self.df_d['entr_old_tag_unknown_removed'][group_name_old].at[index, 'role_index'] = role_index

                note = 'RI-cc'
                if row['status_info'] != '':
                    note = row['status_info'] + ', ' + note
                self.df_d['entr_old_tag_unknown_removed'][group_name_old].at[index, 'status_info'] = note

            else:
                pass
                #lg.debug('got empty role_index, not updating')
    """
    def leftover(self, case_name):
        logic = self.cfg_kp_wanted_logic[case_name]
        group_name_old = logic['group_name']['old']
        self.df_d['entries_old_tagged'][group_name_old]
        mask_ri_set = self.df_d['entries_old_tagged'][group_name_old]['role_index'] != ''
        leftover = self.df_d['entries_old_tagged'][group_name_old][mask]
        num_defined = len(leftover)
        lg.debug('num_defined: %s', num_defined)
    """

    def group_match_by_hostname(self, group_name_old, group_name_new, case_name):
        # lg.debug('Entering. group_name_old: %s, group_name_new: %s, case_name: %s',
        #         group_name_old, group_name_new, case_name)
        df_old = self.df_d['entr_old_tag_unknown_removed'][group_name_old].copy()
        df_old['hostname'] = df_old['sig_hostname']
        df_wanted = self.df_d['wanted'][case_name].copy()
        # XXX why assign this right here?
        # df_old['hostname'] = df_filtered['sig_hostname']
        wanted_index = 'hostname'
        # wanted_index = 'role_index'

        #df_old.set_index(wanted_index, inplace=True)
        #df_wanted.set_index(wanted_index, inplace=True)
        #df_wanted.reset_index(inplace=True)
        df_merged = pd.merge(df_old, df_wanted, on='hostname', how='inner')

        lg.debug('len(df_d[merged]): %s', len(df_merged))
        self.df_d['merged'][case_name] = df_merged.fillna('')
        self.df_d['for_insert'][case_name] = df_merged.fillna('')
        self.buffer_names_d['merged'][case_name] = case_name
        self.buffer_names_d['for_insert'][case_name] = case_name

    # XXX by_role_index rename
    def group_match_by_serverlist_tags(self, group_name_old, group_name_new, case_name):
        """ filter all rows from df_entries where all kp_si_fields are not None and not 'Sandbox'
        and merge with df_wanted on the same fields """
        lg.debug('Entering. group_name_old: %s, group_name_new: %s, case_name: %s',
                 group_name_old, group_name_new, case_name)
        ## now continue with copies from the dataframes
        kp_pf = self.cfg_kp_process_fields
        cols_to_drop = ['sig_item', 'sig_app', 'sig_behoerde', 'sig_crit', 'sig_url', 'item']
        df_old = self.df_d['entr_old_tag_unknown_removed'][group_name_old].copy().drop(columns=cols_to_drop)

        self.prep_debug_table(df_old.copy(), group_name_old, 'msit_df_filtered', attrs=['pk'])
        cols_to_drop = ['app']
        df_wanted = self.df_d['wanted'][case_name].copy().drop(columns=cols_to_drop)

        role_index = 'role_index'
        # XXX just for now set to string
        df_old['role_index'] = df_old['role_index'].astype(str)
        df_old.set_index(role_index, inplace=True)
        df_old.reset_index(inplace=True)
        # df_old = df_old[self.frame_fields['entries_for_merge_table']]

        df_wanted.set_index(role_index, inplace=True)
        df_wanted.reset_index(inplace=True)
        # df_wanted = df_wanted[self.frame_fields['debug_w_table']]
        #df_merged = df_wanted.merge(df_old, on=['role_index'], how='inner') # , suffixes=('_old', '_wt'))
        df_merged = df_old.merge(df_wanted, on='role_index', how='inner')  # , suffixes=('_old', '_wt'))
        lg.debug('len(df_merged): %s', len(df_merged))
        self.df_d['merged'][case_name] = df_merged.fillna('')
        self.buffer_names_d['merged'][case_name] = case_name
        self.df_d['for_insert'][case_name] = df_merged.fillna('')
        self.buffer_names_d['for_insert'][case_name] = case_name

        # Perform the merge operation with an outer join
        df_m_outer = df_old.merge(df_wanted, on=['role_index'], how='outer', suffixes=('_old', '_wt'))
        # Mark rows in df_old that do not have a match in df_wanted
        df_m_outer['status_info'] = df_m_outer.apply(lambda row: 'No Match' if row['role_index']=='' else 'Matched', axis=1)
        # Optionally, filter out the rows that were not matched
        df_not_matched = df_m_outer[df_m_outer['status_info'] == 'No Match']
        # lg.debug(self.df_d['merged'][case_name].head())
        #df_debug = self.prep_debug_table(df_not_matched)
        self.prep_debug_table(df_m_outer, case_name, 'msit_df_m_outer')
        #lg.debug('columns of df_debug: %s', df_debug.columns)
        self.df_d['progress_wanted'][case_name] = df_m_outer
        self.buffer_names_d['progress_wanted'][case_name] = case_name

    # XXX is this unnnedded?
    # we just use title_new and username_new from wanted frame
    def calc_and_update_title_and_username(self, group_name_old, group_name_new, case_name, group_logic):
        lg.debug('Entering. group_name_old: %s, group_name_new: %s, case_name: %s',
                 group_name_old, group_name_new, case_name)
        self.stats_init()

        df = self.df_d['merged'][case_name].copy()

        attrs_new_pat_d = group_logic.get('attrs_new_pat_d')
        lg.debug('attrs_new_pat_d: %s', attrs_new_pat_d)
        username_new_pat = attrs_new_pat_d.get('username_new')
        title_new_pat = attrs_new_pat_d.get('title_new')
        if title_new_pat is None:
            lg.error('title_new pattern is None')
            return

        #logger.debug('title_new_pat: %s', title_new_pat)
        df_up = pd.DataFrame(columns=self.frame_fields['entry_update_table'])

        for i, row in df.iterrows():
            uprow = {}
            row_d = row.to_dict()
            logger.debug('row_d: %s %s %s', row_d['item'], row['username_old'], row['title_old'])
            logger.debug(row_d)
            # logger.debug('row[title_new]: %s', row['title_new'])
            #row_d['item'] = row_d['sig_item']  # XXX assumption ?

            item = row_d['item']

            if 'all_items' in title_new_pat:
                pat_all = title_new_pat['all_items']
            if item in title_new_pat:
                pat = title_new_pat[item]
            else:
                pat = pat_all
            template_text = string.Template(pat)
            uprow['title_new'] = template_text.substitute(row_d)

            # logger.debug('item: %s', item)
            if username_new_pat.get(item) is None:
                uprow['username_new'] = ""
            else:
                if 'all_items' in username_new_pat:
                    pat_all = username_new_pat['all_items']
                if item in username_new_pat:
                    pat = username_new_pat[item]
                else:
                    pat = pat_all
                template_text = string.Template(username_new_pat[item])
                uprow['username_new'] = template_text.substitute(row_d)

            # logger.debug('i: %s, uprow: %s', i, uprow)
            df_up.loc[i] = uprow
            self.count += 1


        lg.debug('len(df): %s, len(df_up): %s', len(df), len(df_up))
        df.update(df_up)
        # lg.debug('df.head(): %s', df[['title_new', 'username_new']].head())
        self.df_d['for_insert'][case_name].update(df)
        self.stats_report(name='40_calc_title_and_username_'+case_name)


    # used for sandbox and unknown - > status_term
    # ATTENTION: runs every case, for same group_name_old is checked multiple times
    # so all subsequent runs wont find any sb/unk in entr_old_tag_unknown_removed
    def allgroups_wanted_loop_rest(self, status_term):  # group_list
        group_list = self.cfg_kp_logic_ctrl_groups.get('loop_crit', []) + self.cfg_kp_logic_ctrl_groups.get('loop_hostspecific', [])

        if group_list is None:
            group_list = []
        # lg.debug('group_lists: %s', group_list)
        for case_name in group_list:
            logic = self.cfg_kp_wanted_logic[case_name]
            group_name_old = logic['group_name']['old']
            group_name_new = logic['group_name']['new']

            df = self.df_d['entries_old_tagged'][group_name_old].copy()
            df_chosen = df[ df['status']==status_term ]
            lg.debug('%s of %s are %s', len(df_chosen), len(df), status_term)

            # save old index
            index_old = df_chosen.index.copy()

            if 'loop_'+status_term+'_drop_from_old' in self.cfg_kp_logic_ctrl['work']:
                lg.debug('Dropping %s entries from %s', len(index_old), group_name_old)
                self.df_d['entries_old_tagged'][group_name_old].drop(index=index_old, inplace=True)

            df_chosen = df_chosen.reset_index()
            df_chosen['group_path_new'] = str( [group_name_new, status_term.capitalize()] )

            self.df_d[status_term][case_name] = df_chosen
            self.buffer_names_d[status_term][case_name] = case_name

