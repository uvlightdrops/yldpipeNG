import re  # , ipaddress
import validators
from flowpy.utils import setup_logger

logfn2 = __name__ + '.log'
logger = setup_logger(__name__, logfn2)

# TODO = '# ??'
TODO = ''
# MISSING = "--> fehlend"
MISSING = ""


class TransformFunc:
    unique_list = {}
    config_dir = ''
    count_replaced = 0

    def gener(self, val, mf, f):
        # meta func? anD =serial of 2:n subfunctions piped
        cmd_d = list(mf.items())
        cmd = list(mf.keys())[0]
        # logger.info("f is %s", f)
        logger.debug("mf: %s", mf)
        logger.debug("val: %s", val)
        logger.info("cmd: %s", cmd)
        logger.debug("cmd_d: %s", cmd_d)
        r = val  # in case no condition matches
        if cmd == 'anD':
            r = self.anD(val, mf, f)
        else:
            # single func
            if cmd in ['tr', 'enum', 'replace', 're']:
                tmp = list(mf.values())[0]
                r = eval('self.' + cmd + '(val, tmp, f)')
            else:
                #if cmd == 're':
                #    # p = re.compile(cmd)
                #    if not re.match(cmd_d[0][1], str(val)):  # cast to str, if pandas int
                #        r = TODO + val
                # XXX move validations to own validation class
                if cmd == 'val':
                    r = self.validate(val, cmd_d[0][1], f)
                if cmd == 'collect':
                    if val not in self.unique_list[f]:
                        self.unique_list[f].append(val)
        logger.debug("result r: %s", r)
        return r

    def re(self, val, mf, f):
        if not re.match(mf, str(val)):  # cast to str, if pandas int
            val = TODO + val
            logger.debug("re failed for val: %s", val)
        return val

    def anD(self, val, mf, f):
        cmds = mf['anD'].items()
        for cmd, args in cmds:
            res = eval('self.' + cmd + '(val, args, f)')
            val = res
        return val

    def typE(self, val, mf, f):
        if isinstance(val, mf[0]):
            return val
        else:
            logger.error('wrong type %s', val)
            return TODO + val


    def stR(self, val, mf, f):
        res = getattr(val, mf[0])()
        return res

    def validate(self, val, mf, f):
        val = str(val)
        if val == '':
            return TODO + MISSING
        check = getattr(validators, mf)(val)
        if check is True:
            return val
        return TODO + val

    def tr(self, val, mf, f):
        func, arg = mf
        # logger.info('val is %s', val)
        #logger.debug('val is %,  type(val) is %s', val, type(val))
        ev = getattr(val, func)(arg)
        res = ev[0]  # hardcoded
        return res

    def enum(self, val, mf, f):
        if val == '':
            return TODO + MISSING
        if mf == ['file']:  # allowed values are in extra text/yml file
            fn = self.config_dir + '/vals_' + f + '.yml'
            fl = open(fn, 'r')
            af = fl.read().splitlines()
        else:
            af = mf
        if val in af:  # existing value is allowed, keep it
            return val
        return TODO + val

    def replace(self, val, mf, f):
        # print(mf)
        if val == mf[0]:
            self.count_replaced += 1
            return mf[1]
        return val
