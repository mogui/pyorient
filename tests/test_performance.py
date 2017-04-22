import base64
import cProfile
import datetime
import getpass
import math
import os
import pstats
import pyorient
import random
import string
import sys
import time
import traceback
import unittest
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
PROFILE_SORT = 'tottime'


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
            ps = pstats.Stats(self.pr, stream=s).sort_stats(PROFILE_SORT)
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

        min_range = -2000
        max_range = 2000
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
        data_in_str = repr(data_in).replace('e', 'E').replace('+', '')

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
        
        def is_close(a, b, rel_tol=1e-09, abs_tol=0.0):
            # http://stackoverflow.com/questions/5595425/what-is-the-best-way-to-compare-floats-for-almost-equality-in-python
            return abs(a-b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)
        
        # check values:
        fail_count = 0
        for i in range(len(data_in)):
            if not is_close(data_in[i], float(data_out[i])):
                fail_count += 1
                print('dataIn[%d] != dataOut[%d]  (%f != %f)' % (i, i, data_in[i], data_out[i]))
        if fail_count > 0:
            print('FAIL: %d incorrect number values!' % fail_count)
        else:
            print('PASS: all number values matched.')
        assert fail_count == 0
    
    def test_map_performance(self):
        banner('Map performance')

        map_len = 16000
    
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
            except KeyError:
                print('output is missing key "%s"' % str(k))
                mismatch_count += 1
        if mismatch_count > 0:
            print('FAIL: %d incorrect map values!' % mismatch_count)
        else:
            print('PASS: all map values matched.')
        assert mismatch_count == 0
        
    def test_rid_performance(self):
        banner('RID performance')

        num_rids_to_make = 10
        num_rids_to_test = 16000

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

        num_strs = 1000  # how many total strings to make
        str_len = 200  # how long to make randomly generated strings
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
            print('FAIL: string list length does not match!  len(strs_in)=%d  len(strs_out)=%d' %
                  (len(strs_in), len(strs_out)))
        assert len(strs_in) == len(strs_out)
        
        # check values:
        mismatch_count = 0
        for k in range(num_strs):
            if str(strs_in[k]) != str(strs_out[k]):
                print('strings do not match! strs_in[%s]:%s != strs_out[%s]:%s' %
                      (str(k), str(strs_in[k]), str(k), str(strs_out[k])))
                mismatch_count += 1
        if mismatch_count > 0:
            print('FAIL: %d incorrect string values!' % mismatch_count)
        else:
            print('PASS: all string values matched.')
        assert mismatch_count == 0

    def test_binary_data_performance(self):
        banner('Binary-datatype performance')
        
        BINARY_LEN = 1024 * 80
        
        # make some random binary data to use
        rawdata = os.urandom(BINARY_LEN)
        b64data = str(base64.b64encode(rawdata).decode('utf-8'))
        
        # make a class with a binary field to store the data
        self.client.command('create class test_binarydata')
        self.client.command('create property test_binarydata.bin BINARY')
        
        # command to store data
        cmd = 'insert into test_binarydata set bin = decode("%s","base64")' % b64data
        
        # profile command execution:
        perf = PerfMon()
        perf.start()
        result = self.client.command(cmd)[0]
        perf.stop()
        perf.report()
        
        # confirm we got correct data back
        assert base64.b64decode(result.oRecordData['bin']) == rawdata
        
    def test_multi_datatype_performance(self):
        banner('Multi-datatype performance')
        
        ITTERATIONS = 10  # small values like 10 are fine for code coverage, larger
                          # values like 100 show performance more clearly
        MAX_COLLECTION_SIZE = 3
    
        # some RIDs to test link datatype
        rids = []
        for k in range(MAX_COLLECTION_SIZE):
            result = self.client.command('create vertex set value = %f' % random.random())
            rids.append(result[0]._rid)
        
        # some data to test binary datatype
        rawdata = b'Hello\x00Binary\x00World\x00!'
        b64data = str(base64.b64encode(rawdata).decode('utf-8'))
        
        # embedded vertex to use
        embeddedv_s = '{"@type":"d","@class":"v", "name":"bob", "number":42, "nada":Null}'  # what we send to OrientDB
        embeddedv_d = {'nada': None, 'number': 42, 'o_class': 'V', 'name': 'bob'}  # what we get back
        embeddedv_list = [embeddedv_s for i in range(MAX_COLLECTION_SIZE)]
        
        # supported types from OrientDB 2.1:
        #
        # BOOLEAN	SHORT	DATE	BYTE
        # INTEGER	LONG	STRING	LINK
        # DOUBLE	FLOAT	BINARY	EMBEDDED
        # EMBEDDEDLIST	EMBEDDEDSET	EMBEDDEDMAP
        # LINKLIST	LINKSET	LINKMAP
        
        # class with lots of types
        self.client.command('create class test_datatypes')
        for i in range(ITTERATIONS):
            self.client.command('create property test_datatypes.true%d BOOLEAN' % i)
            self.client.command('create property test_datatypes.false%d BOOLEAN' % i)
            self.client.command('create property test_datatypes.short%d SHORT' % i)
            self.client.command('create property test_datatypes.date%d DATE' % i)
            self.client.command('create property test_datatypes.datetime%d DATETIME' % i)
            self.client.command('create property test_datatypes.byte%d BYTE' % i)
            self.client.command('create property test_datatypes.int%d INTEGER' % i)
            self.client.command('create property test_datatypes.long%d LONG' % i)
            self.client.command('create property test_datatypes.string%d STRING' % i)
            self.client.command('create property test_datatypes.link%d LINK' % i)
            self.client.command('create property test_datatypes.double%d DOUBLE' % i)
            self.client.command('create property test_datatypes.float%d FLOAT' % i)
            self.client.command('create property test_datatypes.binary%d BINARY' % i)
            self.client.command('create property test_datatypes.embedded%d EMBEDDED v' % i)
            self.client.command('create property test_datatypes.embeddedlist%d EMBEDDEDLIST v' % i)
            self.client.command('create property test_datatypes.embeddedset%d EMBEDDEDSET v' % i)
            self.client.command('create property test_datatypes.embeddedmap%d EMBEDDEDMAP v' % i)
            self.client.command('create property test_datatypes.linklist%d LINKLIST' % i)
            self.client.command('create property test_datatypes.linkset%d LINKSET' % i)
            self.client.command('create property test_datatypes.linkmap%d LINKMAP' % i)
            
        # self.client.command(batch) # above setup is very slow, but putting into a batch hangs :P 
        
        # massive command to insert all datatypes ITTERATIONS number of times
        cmd = 'insert into test_datatypes set '
        for i in range(ITTERATIONS):
            cmd += 'null%d = NULL, ' % i
            cmd += 'true%d = True, ' % i
            cmd += 'false%d = False, ' % i
            cmd += 'short%d = 123, ' % i
            cmd += 'date%d = date("2000-01-01 00:00:00"), ' % i
            cmd += 'datetime%d = date("2000-01-01 12:34:56"), ' % i
            cmd += 'byte%d = 123, ' % i
            cmd += 'int%d = 123, ' % i
            cmd += 'long%d = 123, ' % i
            cmd += 'string%d = "helloworld", ' % i
            cmd += 'link%d = %s, ' % (i, rids[0])
            cmd += 'double%d = 456.789, ' % i
            cmd += 'float%d = 456.789, ' % i
            cmd += 'binary%d = decode("%s","base64"), ' % (i, b64data)
            cmd += 'emptylist%d = [], ' % i
            cmd += 'list%d = [111,222,333,444,555,666,777,888,999], ' % i
            cmd += 'emptyset%d = set(NULL), ' % i  # orientdb set function requires at least one arg
            cmd += 'set%d = set(1111,2222,3333,1111,2222), ' % i
            cmd += 'embeddedlist%d = [%s], ' % (i, ','.join(embeddedv_list[:i % MAX_COLLECTION_SIZE]))
            cmd += 'embeddedset%d = set(%s), ' % (i, ','.join(embeddedv_list[:max(i % MAX_COLLECTION_SIZE, 1)]))
            cmd += 'embeddedmap%d = { ' % i
            for j in range(i % MAX_COLLECTION_SIZE):
                cmd += '"entry%d":%s,' % (j, embeddedv_list[j])
            cmd = cmd[:-1]  # strip final ','
            cmd += '}, '
            cmd += 'linklist%d = [%s], ' % (i, ','.join(rids[:i % MAX_COLLECTION_SIZE]))
            cmd += 'linkset%d = set(%s), ' % (i, ','.join(rids[:max(i % MAX_COLLECTION_SIZE, 1)]))
            cmd += 'linkmap%d = { ' % i
            for j in range(i % MAX_COLLECTION_SIZE):
                cmd += '"entry%d":%s,' % (j, rids[j])
            cmd = cmd[:-1]  # strip final ','
            cmd += '}, '
            
        # profile command execution:
        perf = PerfMon()
        perf.start()
        result = self.client.command(cmd[:-2])[0]
        perf.stop()
        perf.report()
        
        test_datetime = datetime.datetime.strptime('2000-01-01 12:34:56', '%Y-%m-%d %H:%M:%S')
        test_date = test_datetime.date()
        
        for i in range(ITTERATIONS):
            assert result.oRecordData['true%d' % i] is True
            assert result.oRecordData['false%d' % i] is False
            assert result.oRecordData['null%d' % i] is None
            assert result.oRecordData['int%d' % i] == 123
            assert result.oRecordData['float%d' % i] == 456.789
            assert result.oRecordData['date%d' % i] == test_date
            assert result.oRecordData['datetime%d' % i] == test_datetime
            assert result.oRecordData['string%d' % i] == 'helloworld'
            assert base64.b64decode(result.oRecordData['binary%d' % i]) == rawdata
            assert result.oRecordData['emptylist%d' % i] == []
            assert result.oRecordData['list%d' % i] == [111, 222, 333, 444, 555, 666, 777, 888, 999]
            assert result.oRecordData['emptyset%d' % i] is None  # not a list or set?
            assert set(result.oRecordData['set%d' % i]) == {1111, 2222, 3333, 1111, 2222}  # pyorient returns list
            
            assert type(result.oRecordData['embeddedlist%d' % i]) is list
            assert len(result.oRecordData['embeddedlist%d' % i]) == i % MAX_COLLECTION_SIZE
            for j in range(i % MAX_COLLECTION_SIZE):
                assert result.oRecordData['embeddedlist%d' % i][j] == embeddedv_d
                
            assert type(result.oRecordData['embeddedset%d' % i]) is list  # pyorient returns list and not set
            assert len(result.oRecordData['embeddedset%d' % i]) == max(i % MAX_COLLECTION_SIZE, 1)
            for j in range(i % MAX_COLLECTION_SIZE):
                assert result.oRecordData['embeddedset%d' % i][j] == embeddedv_d
            
            assert type(result.oRecordData['embeddedmap%d' % i]) is dict
            assert len(result.oRecordData['embeddedmap%d' % i]) == i % MAX_COLLECTION_SIZE
            for j in range(i % MAX_COLLECTION_SIZE):
                assert result.oRecordData['embeddedmap%d' % i]['entry%d' % j] == embeddedv_d
            
            assert type(result.oRecordData['linklist%d' % i]) is list
            assert len(result.oRecordData['linklist%d' % i]) == i % MAX_COLLECTION_SIZE
            for j in range(i % MAX_COLLECTION_SIZE):
                assert str(result.oRecordData['linklist%d' % i][j]) == rids[j]
                
            assert type(result.oRecordData['linkset%d' % i]) is list  # pyorient returns list and not set
            assert len(result.oRecordData['linkset%d' % i]) == max(i % MAX_COLLECTION_SIZE, 1)
            result_rids = [str(link) for link in result.oRecordData['linkset%d' % i]]
            for j in range(max(i % MAX_COLLECTION_SIZE, 1)):
                assert rids[j] in result_rids
                
            assert type(result.oRecordData['linkmap%d' % i]) is dict
            assert len(result.oRecordData['linkmap%d' % i]) == i % MAX_COLLECTION_SIZE
            for j in range(i % MAX_COLLECTION_SIZE):
                assert str(result.oRecordData['linkmap%d' % i]['entry%d' % j]) == rids[j]
