from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')

import re
#from flashtext import KeywordProcessor


# this class should be similar to FieldOps, but it works on rows directly
# we do not use any entry object here anymore
def log_sig(msg, age_sig):
    # Logge nur die Einträge, deren Wert nicht None ist
    filtered = {k: v for k, v in age_sig.items() if v is not None}
    logger.debug('%s %s', msg, filtered)

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
            'status': 'NEW',
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
        #logger.debug('row: %s', row)
        logger.debug('')
        #input('Processing row: %s' %(row['title']))

        logger.debug('==== %s |%s |%s |%s', row['title'], row['username'], row['url'], row['notes'][:20])
        ### here submethods
        self.do_search_spec(field_search_spec, row, age_sig)
        #log_sig('age_sig ss:', age_sig)
        age_sig_ss = age_sig.copy()

        self.do_variations(field_variations, row, age_sig, attrs_all)
        #log_sig('age_sig fv:', age_sig)
        age_sig_fv = age_sig.copy()

        self.do_search(field_search, age_sig, status_d, attrs_all, row)
        log_sig('age_sig sr:', age_sig)
        age_sig_sr = age_sig.copy()



        result = {}
        # prepare transfer
        for key, d_item in age_sig.items():
            if d_item is not None:
                result['sig_'+key] = d_item

        for key, d_item in status_d.items():
            #if d_item not in ['NEW', '']:
            result[key] = d_item

        row.update( result )

        result_kp = self.check_synonyms(row)
        #logger.debug('result_kp: %s', result_kp)

        #row.update( result_kp )

        #logger.debug('row: %s', row)
        return row

    # XXX url not yet found
    def do_search_spec(self, field_search_spec, row, age_sig):
            for item in field_search_spec:
                # logger.debug('item: %s', item['name'])
                method, argu = next(iter(item['how'].items()))
                if method == 're':
                    cpat = re.compile(argu)
                    #logger.debug('method: %s, cpat: %s', method, cpat)
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
                            #logger.debug('SS val: %s, m: %s', val, m)
                            age_sig[item['finds']] = m[0]

    def do_variations(self, field_variations, row, age_sig, attrs_all):
        #input(' %s -  %s' % (row['title'], row['username']))
        for attr in attrs_all:
            text = str(row[attr])
            if text is None or text=='':
                continue

            #logger.debug('====== attr: %s - text: %s', attr, text)
            # Check for possible variations of field values
            for fv_key, fv_item in field_variations.items():
                for aspect_key, aspect_item in fv_item.items():
                    # leave key alone, it is the canonical name
                    all_vars = aspect_item
                    for term in all_vars:
                        # we need real regexp in the values
                        cterm = re.compile(term)
                        # logger.debug('cterm: %s', cterm)
                        m = cterm.findall(text)
                        if m:
                            #logger.debug('VV %s| %s: %s ', attr, text, term)
                            # if there was already a value from other term found before,
                            # dont replace vorProd with Prod # XXX solve reg exp in yaml issue
                            if age_sig[fv_key] is None:
                                age_sig[fv_key] = term
                            else:
                                if term == age_sig[fv_key]:
                                    pass
                                    # same term found again, ok
                                else:
                                    pass
                                    #logger.warning('Coll %s: %s', attr, text)
                                    #logger.warning('term: %s, age_sig[t]: %s', term, age_sig[fv_key])
                            # remember finding
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
                            #logger.debug('DS text: %s, m: %s', text, m)
                            if 'set_sig' in act.keys():
                                age_sig[act['set_sig']] = m[0]
                            if 'mark'  in act.keys():
                                # logger.debug('== mark: %s', act['mark'])
                                status_d['status'] = act['mark']
                                status_d['status_info'] = status_d['status_info'] + ' ' + m[0][:8]
                            if 'modify' in act.keys():
                                #logger.debug('== mark: %s', act['modify'])
                                field = act['modify']['field']
                                val =  act['modify']['val']
                                status_d[field] = val
                            """
                            """
                            # one match is sufficient
                            continue
                            # XXX modifying row here is a no-go!


    def init_keyword_processors(self):
        # Initialisiere keyword_processors aus YAML, falls nicht vorhanden
        import yaml
        from flashtext import KeywordProcessor
        self.kproc_d = {}
        with open("/home/flow/dev_flow/ypipe_apps/data_master/ypipe_test/kp_flashtext_variations.yml", "r") as f:
            field_variations = yaml.safe_load(f)

        for aspect, aspect_items in field_variations.items():
            self.kproc_d[aspect] = {}
            for canonical, synonyms in aspect_items.items():
                kp = KeywordProcessor()
                #kp.add_non_word_boundary('')
                for syn in synonyms:
                    kp.add_keyword(syn, clean_name=canonical)
                self.kproc_d[aspect][canonical] = kp
                #logger.debug("syn: '%s': %s", canonical, kp.get_all_keywords())


    def check_synonyms(self, row):
        # keyword_processors muss im Task vorab initialisiert werden
        if not hasattr(self, 'kproc_d'):
            self.init_keyword_processors()

        result = {}
        tmp = {}
        #logger.debug('')
        #logger.debug('==== %s |%s |%s |%s', row['title'], row['username'], row['url'], row['notes'][:20])
        for attr in self.attrs_all:
            text = str(row[attr])
            if text is None or text=='':
                    continue

            for aspect, aspect_items in self.kproc_d.items():
                for canonical, kp in aspect_items.items():
                    found = kp.extract_keywords(text)
                    if found:
                        #logger.debug("FlashText found for field '%s': %s", field, found)
                        sig = "sig_"+aspect
                        result[sig] = found[0]
                        if sig not in tmp:
                            tmp[sig] = []
                        if canonical not in tmp[sig]:
                            #logger.debug("'%s': %s", canonical, found)
                            tmp[sig].append( canonical )

        #logger.debug('tmp: %s',  tmp)
        for sig, can_list in tmp.items():
            if len(can_list) == 1:
                result[sig] = can_list[0]
            else:
                result[sig] = can_list[0]
                logger.warning("Multi found for %s: %s", sig, can_list)
        #logger.debug('result: %s', result)
        return result