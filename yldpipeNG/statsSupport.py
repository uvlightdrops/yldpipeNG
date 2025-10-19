import pandas as pd
from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')


class StatsSupport:
    df_d = {}
    df_d['stats'] = {}
    buffer_names_d = {}
    frame_fields = {}
    results = {}

    def stats_df_init(self):
        self.df_d['stats']['report'] = pd.DataFrame(columns=self.frame_fields['stats_table'])
        self.buffer_names_d['stats']['report'] = 'report'

    def stats_init(self):
        self.count = 0
        self.count_suc = 0
        self.count_err = 0
        self.count_crit = 0
        self.results = {
            'crit': [],
            'err': [],
        }

    def stats_report(self, name=''):
        logger.info("------  ------    STATS REPORT: %s", name)
        logger.info("self.count : %s", self.count)
        logger.info("self.count_suc: %s", self.count_suc)
        logger.info("self.count_crit: %s", self.count_crit)
        logger.info("self.count_err: %s", self.count_err)
        ldf = len(self.df_d['stats']['report'])
        logger.debug('len(self.df_d[stats][report]): %s', ldf)
        self.df_d['stats']['report'].loc[ldf] = {
            'name': name,
            'count': self.count,
            'count_suc': self.count_suc,
            'count_err': self.count_err,
            'count_crit': self.count_crit,
        }
