# step: lib_yldpipe.group_ri_by_sitags
        # in:
        #   - entries_old_tagged
        #   - group_name_old
        # out:
        #   - entries_old_tagged

# own steps have min one frame (declaration: frame_name) for in and min. one for out
# and what else is common / required / useful?

class TrbCraft:
    def filter_by_si_fields_defined(self, df):
        ca = ((df['app'] != None) & df['app'].notna())
        cb = ((df['behoerde'] != None) & df['behoerde'].notna())
        cc = ((df['crit'] != 'Sandbox') & df['crit'].notna())
        # citem = ((df['item'] != None) & df['item'].notna())
        citem = ((df['sig_item'] != None) & df['sig_item'].notna())
        return df[ca & cb & cc & citem]


    # XXX another generate logic needed for looping only hosts of an app, for Alf serviceuser ie.
    def filter_row_by_si_fields_defined(self, row, sig='', sig_item=''):
        logger.debug('row: %s', row)
        ca = (row[sig + 'app'] != None) & (row[sig + 'app'] != '')
        cb = (row[sig + 'behoerde'] != None) & (row[sig + 'behoerde'] != '')
        cc = ((row[sig + 'crit'] != None) & (row[sig + 'crit'] != '') & (row[sig + 'crit'] != 'Sandbox'))
        item_key = sig_item + 'item'
        logger.debug('item_key: %s', item_key)
        citem = (row[item_key] != None) & (row[item_key] != '')
        return (ca & cb & cc & citem)
    # XXX another generate logic needed for looping only hosts of an app, for Alf serviceuser ie.

    def faker(self, _):
        length = random.randint(6, 10)
        letters_and_digits = string.ascii_letters + string.digits
        random_string = ''.join(random.choice(letters_and_digits) for i in range(length))
        return random_string

    def add_necessary_cols_to_df(self, df):
        import ast
        kp_pf = self.cfg_kp_process_fields
        lg.debug(kp_pf['kp_types_d']['datetime'])
        fieldnames = kp_pf['kp_pure_fields'] + kp_pf['kp_same_fields'] + kp_pf['kp_extra_fields']
        cols = df.columns
        #for col in df.columns:
        #    df[col] = df[col].astype(str)
        for fieldname in fieldnames:
            if fieldname not in cols:
                # lg.debug('fieldname: %s was not in cols -- ADDED', fieldname)
                if fieldname in kp_pf['kp_types_d']['datetime']:
                    lg.debug('fieldname: %s is a datetime field', fieldname)
                    #df[fieldname] = pd.Timestamp('2026-01-01').tz_localize(None)
                    #df[fieldname] = pd.Timestamp('2026-01-01').tz_convert(None)
                    df[fieldname] = ''  # for writer type is not relevant
                elif fieldname == 'group_path_new':
                    pass
                else:
                    df[fieldname] = ''
                    # df[fieldname] = df[fieldname].apply(self.faker)
        df['title'] = df['title'].apply(self.faker)
        df['username'] = df['username'].apply(self.faker)
        return df
