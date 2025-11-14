import sys
# print(sys.path)
from flowpy.utils import setup_logger
import logging

logger = setup_logger(__name__, __name__+'.log', level=logging.DEBUG)
lg = setup_logger(__name__+'_2', __name__+'_2.log')


class TreeReorderBuilderBase:

    # XXX class YamlConfigSupport:
    # auto create attributes or dict of attrs with same name,


    def prep_debug_table(self, df, case_name, dname, attrs=None, attrs_groups=None):
        lg.debug('dname(tkey): %s, case_name: %s, len(df): %s', dname, case_name, len(df))
        # XXX copy df here or at caller?
        # lg.debug('df.columns:')
        # lg.debug(df.columns)

        if attrs is None:
            attrs = []
        if attrs_groups is None:
            attrs_groups = []
        attrs_default = self.frame_fields['debug_table']
        # lg.debug('attrs_default: %s', attrs_default)
        attrs = attrs_default + attrs

        for a_group in attrs_groups:
            group_attrs = self.cfg_kp_process_fields.get(a_group+'_fields', [])
            # lg.debug('group_attrs: %s', group_attrs)
            attrs += group_attrs

        # deduplicate, use for development
        seen = set()
        attrs_final = [x for x in attrs if not (x in seen or seen.add(x))]
        # lg.debug('attrs: %s', attrs)
        # lg.debug('attrs_final: %s', attrs_final)

        for attr in attrs_final:
            if attr not in df.columns:
                lg.debug('attr: %s not in df.columns', attr)
                attrs.remove(attr)

        df = df[attrs]
        # lg.debug(df.head(3))
        if 'sort' in self.cfg_kp_process_fields['debug_table'].keys():
            df = df.sort_values(by=self.cfg_kp_process_fields['debug_table']['sort'])

        self.df_d[dname][case_name] = df
        self.buffer_names_d[dname][case_name] = case_name


    def write_all(self):
        # XXX to specialized:
        # XXX move to treeReorderBase
        self.progress_table_output_drop_fields = self.cfg_kp_process_fields['progress_table_output_drop_fields']
        lg.info('### Writing to excel ###')
        self.generic_write_all()
