
from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')

import re


# this class should be similar to FieldOps, but it works on rows directly
# we do not use any entry object here anymore

class FieldOperations:

    def check_row_for_prominent_terms(self, row):
        """ in rows there are certain terms at typical places,
            like in the title or the notes, ie. the behoerde or the criticality
        """
        self.config_d = self.context['config_d']
        field_search_spec   = self.config_d['kp_term_attr_field_search_spec']['field_search_spec']
        field_variations    = self.config_d['kp_term_attr_field_variations']['field_variations']
        field_search        = self.config_d['kp_term_attr_field_search']['field_search']

        kp_process_fields = self.config_d['kp_process_fields']
        attrs_all = kp_process_fields['kp_pure_fields'] + \
                    kp_process_fields['kp_same_fields']

        if 'password' in attrs_all:
            attrs_all.remove('password')
        status_d = {
            'status': None,
            'status_info': '',
        }
        age_sig = {
            'hostname': None,
            'item': None,
            'app': None,
            'crit': None,
            'behoerde': None,
            'url': None,
        }
        ### here submethods
        self.do_search_spec(field_search_spec, row, age_sig)
        self.do_variations(field_variations, row, age_sig, attrs_all)
        self.do_search(field_search, age_sig, status_d, attrs_all, row)

        #logger.debug('age_sig: %s', age_sig)
        result = {}
        # prepare transfer
        for key, d_item in age_sig.items():
            # what condition was here? XXX
            if True:
                result['sig_'+key] = d_item
        for key, d_item in status_d.items():
            result[key] = d_item

        #logger.debug('result: %s', result)
        row.update( result )
        #logger.debug('row: %s', row)
        return row  #age_sig



    def do_search_spec(self, field_search_spec, row, age_sig):
            for item in field_search_spec:
                # logger.debug('item: %s', item['name'])
                method, argu = next(iter(item['how'].items()))
                if method == 're':
                    cpat = re.compile(argu)
                    logger.debug('method: %s, cpat: %s', method, cpat)
                    # we want to keep in memory all found vals
                    # if there are more than one, we log a warning
                    # if there is only one, we keep it
                    where = item['where']
                    val = row[where]
                    if val is None:
                        # logger.debug('val is None')
                        val = ''
                    else:
                        m = cpat.findall(val)
                        if m:
                            logger.debug('SS val: %s, m: %s', val, m)
                            age_sig[item['finds']] = m[0]

    def do_variations(self, field_variations, row, age_sig, attrs_all):
            for attr in attrs_all:
                text = str(row[attr])
                logger.debug('attr: %s - text: %s', attr, text)
                if text is None or text=='':
                    continue

                # Check for possible variations of field values
                for fv_key, fv_item in field_variations.items():
                    for aspect_key, aspect_item in fv_item.items():
                        # leave key alone, it is the canonical name
                        #all_vars = [aspect_key] + aspect_item
                        all_vars = aspect_item
                        # we need real regexp in the values
                        for term in all_vars:
                            cterm = re.compile(term)
                            # logger.debug('cterm: %s', cterm)
                            m = cterm.findall(text)
                            if m:
                                logger.debug('VV %s: %s ', text, term)
                                # if there was already a value from other term found before,
                                # dont replace vorProd with Prod # XXX solve reg exp in yaml issue
                                if age_sig[fv_key] is None:
                                    age_sig[fv_key] = term
                                else:
                                    pass
                                    #logger.warning('Coll %s: %s %s %s', attr, text, term, age_sig[fv_key])
                                age_sig[fv_key] = aspect_key


        # Search all values for these strings
        #for fs_key, fs_item in field_search.items():
        # logger.debug('fs_key: %s, fs_item: %s', fs_key, fs_item)
    def do_search(self, field_search, age_sig, status_d, attrs_all, row):
        for attr in attrs_all:
            text = str(row[attr])

            for fs_item in field_search:
                name = fs_item['name']

                if fs_item['read'] == 're':
                    m = None
                    cpat = re.compile(fs_item['args'])
                    m = cpat.findall(text)
                    if m:
                        age_sig[name] = m[0]
                        # logger.debug('fs_key: %s - text: %s, m: %s', fs_key, text, m)

                if fs_item['read'] == 'enum':
                    act = fs_item['act']
                    for term in fs_item['args']:
                        m = re.findall(term, text)
                        if m:
                            logger.debug('DS text: %s, m: %s', text, m)
                            if 'set_sig' in act.keys():
                                age_sig[act['set_sig']] = m[0]
                            if 'mark'  in act.keys():
                                # logger.debug('== mark: %s', act['mark'])
                                status_d['status'] = act['mark']
                                status_d['status_info'] = status_d['status_info'] + ' ' + m[0][:8]
                            """
                            if 'modify' in act.keys():
                                field = act['modify']['field']
                                val =  act['modify']['val']
                                age_sig[field] = val
                            """
                            # one match is sufficient
                            continue
                            # XXX modifying row here is a no-go!

