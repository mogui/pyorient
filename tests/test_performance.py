import unittest
import pyorient
import random
import string
import math
import time
import pstats
import cProfile
import traceback
import getpass
import sys
if sys.version_info[0] <= 2:
    import StringIO
else:
    import io as StringIO


"""
    Measure performance.  optionally profile.
    
    crafts SQL to create vertices with lots of data and then profiles command execution.
    Response from server includes the original data, which we use to verify correctness.
    
    Performance info printed to stdout.  use "nosetests -s" to see stdout.
"""

ENABLE_PROFILE = False


def banner(msg, width=40):
    """print big banner so we can find test results easily"""
    print('')
    print('#'*width)
    print('# '+msg)
    print('#'*width)
    
    
class PerfMon:
    """wrap cProfile to make profiling trivial"""

    def __init__(self):
        self.pr = cProfile.Profile()
        self.start_time = None
        self.stop_time = None

    def start(self):
        self.start_time = time.time()
        if ENABLE_PROFILE:
            self.pr.enable()
        
    def stop(self):
        if ENABLE_PROFILE:
            self.pr.disable()
        self.stop_time = time.time()
        
    def report(self):
        print('execution time: %f seconds' % (self.stop_time - self.start_time))
        if ENABLE_PROFILE:
            s = StringIO.StringIO()
            ps = pstats.Stats(self.pr, stream=s).sort_stats(profileSort)
            ps.print_stats()
            self.pr.clear()
            print(s.getvalue())
    
    
class PerformanceTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(PerformanceTestCase, self).__init__(*args, **kwargs)
        self.client = None

    def setUp(self):

        self.client = pyorient.OrientDB("localhost", 2424)
        self.client.connect("root", "root")

        db_name = "test_tr"
        try:
            self.client.db_drop(db_name)
        except pyorient.PyOrientCommandException as e:
            print(e)
        finally:
            self.client.db_create(db_name, pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_MEMORY)

    def test_number_performance(self):
        banner('Number Performance')

        min_range = -1000
        max_range = 1000
        max_exp = 35
        min_exp = -35

        # some integers:
        data_in = list(range(min_range, max_range))
        # random floats:
        data_in += [random.random()*x for x in range(min_range, max_range)]
        # small floats:
        data_in += [math.pow(10, min_exp*random.random()) for x in range(min_range, max_range)]
        # big floats:
        data_in += [math.pow(10, max_exp*random.random()) for x in range(min_range, max_range)]

        # format so ODB can parse exponents (123e+4 -> 123E4)
        data_in_str = str(data_in).replace('e', 'E').replace('+', '')

        # profile command execution:
        perf = PerfMon()
        perf.start()
        result = self.client.command('create vertex set data = %s' % data_in_str)
        perf.stop()
        perf.report()

        data_out = result[0].data

        # check length:
        if len(data_in) == len(data_out):
            print('PASS: data length matches.  len(data)=%d' % len(data_in))
        else:
            print('FAIL: data length does not match!  len(dataIn)=%d  len(dataOut)=%d' % (len(data_in), len(data_out)))
        assert len(data_in) == len(data_out)
        
        def isclose(a, b, rel_tol=1e-09, abs_tol=0.0):
            # http://stackoverflow.com/questions/5595425/what-is-the-best-way-to-compare-floats-for-almost-equality-in-python
            return abs(a-b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)
        
        # check values:
        fail_count = 0
        for i in range(len(data_in)):
            if not isclose(data_in[i], float(data_out[i])):
                fail_count += 1
                print('dataIn[%d] != dataOut[%d]  (%f != %f)' % (i, i, data_in[i], data_out[i]))
        if fail_count > 0:
            print('FAIL: %d incorrect number values!' % fail_count)
        else:
            print('PASS: all number values matched.')
        assert fail_count == 0
    
    def test_map_performance(self):
        banner('Map performance')

        map_len = 8000
    
        input_map = {}
        for k in range(map_len):
            input_map[str(k)] = k

        # profile command execution:
        perf = PerfMon()
        perf.start()
        result = self.client.command('create vertex set map = %s' % input_map)
        perf.stop()
        perf.report()
    
        output_map = result[0].map
    
        # check length:
        if len(input_map) == len(output_map):
            print('PASS: map length matches.  len(map)=%d' % len(input_map))
        else:
            print('FAIL: map length does not match!  len(inputMap)=%d  len(outputMap)=%d' %
                  (len(input_map), len(output_map)))
        assert len(input_map) == len(output_map)
        
        # check values:
        mismatch_count = 0
        for k in input_map:
            try:
                if input_map[k] != output_map[k]:
                    print('input %s:%s != output %s:%s' % (str(k), str(input_map[k]), str(k), str(output_map[k])))
                    mismatch_count += 1
            except:
                mismatch_count += 1
                # print(traceback.format_exc())
        if mismatch_count > 0:
            print('FAIL: %d incorrect map values!' % mismatch_count)
        else:
            print('PASS: all map values matched.')
        assert mismatch_count == 0
        
    def test_rid_performance(self):
        banner('RID performance')

        num_rids_to_make = 10
        num_rids_to_test = 8000

        # make some vertices and keep list of their RIDs
        rids = []
        for k in range(num_rids_to_make):
            result = self.client.command('create vertex set value = %f' % random.random())
            rids.append(result[0]._rid)

        # make big list of RIDs
        rids_in = []
        # ridsIn.append('#-1:0') #test a negative cluster ID
        # ridsIn.append('#123:456') #test multi-digit cluster ID and record ID
        while len(rids_in) < num_rids_to_test:
            rids_in.append(rids[random.randint(0, num_rids_to_make-1)])  # add more RIDs randomly from set created above
        rids_in_str = '[' + ', '.join(rids_in) + ']'
        
        # profile command execution:
        perf = PerfMon()
        perf.start()
        result = self.client.command('create vertex set list = %s' % rids_in_str)
        perf.stop()
        perf.report()
        
        rids_out = result[0].list
    
        # check length:
        if len(rids_in) == len(rids_out):
            print('PASS: rid list length matches.  len(rids)=%d' % len(rids_in))
        else:
            print('FAIL: rid list length mismatch!  len(ridsIn)=%d  len(ridsOut)=%d' % (len(rids_in), len(rids_out)))
        assert len(rids_in) == len(rids_out)
        
        # check values:
        mismatch_count = 0
        for k in range(num_rids_to_test):
            if str(rids_in[k]) != str(rids_out[k]):
                print('rids do not match! ridsIn[%s]:%s != ridsOut[%s]:%s' %
                      (str(k), str(rids_in[k]), str(k), str(rids_out[k])))
                mismatch_count += 1
        if mismatch_count > 0:
            print('FAIL: %d incorrect RID values!' % mismatch_count)
        else:
            print('PASS: all RID values matched.')
        assert mismatch_count == 0
    
    def test_string_performance(self):
        banner('String performance')

        num_strs = 200  # how many total strings to make
        str_len = 1000  # how long to make randomly generated strings
        strs_in = []    # array of strings to send into ODB
        while len(strs_in) < num_strs:
            rand_str = ''.join(random.choice(string.printable) for _ in range(str_len))
            rand_str = rand_str.replace("'", '"')   # quotes are not working.  :(
            rand_str = rand_str.replace('\\', '/')  # backslashes are also not working.  :(
            strs_in.append(rand_str)
        
        # format as array of quoted strings with escapes for: backslash, quote, newline, and return
        strs_in_formatted = '['+', '.join("'%s'" % (
            s.replace('\\', '\\\\').replace("'", "\\'").replace('\n', '\\n').replace('\r', '\\r')) for s in strs_in)+']'
    
        # profile command execution:
        perf = PerfMon()
        perf.start()
        result = self.client.command('create vertex set text = %s' % strs_in_formatted)
        perf.stop()
        perf.report()
    
        strs_out = result[0].text
    
        # check length:
        if len(strs_in) == len(strs_out):
            print('PASS: string list length matches.  len(strs)=%d' % len(strs_in))
        else:
            print('FAIL: string list length does not match!  len(strsIn)=%d  len(strsOut)=%d' %
                  (len(strs_in), len(strs_out)))
        assert len(strs_in) == len(strs_out)
        
        # check values:
        mismatch_count = 0
        for k in range(num_strs):
            if str(strs_in[k]) != str(strs_out[k]):
                print('strings do not match! strsIn[%s]:%s != strsOut[%s]:%s' %
                      (str(k), str(strs_in[k]), str(k), str(strs_out[k])))
                mismatch_count += 1
        if mismatch_count > 0:
            print('FAIL: %d incorrect string values!' % mismatch_count)
        else:
            print('PASS: all string values matched.')
        assert mismatch_count == 0
