import os
from collections import defaultdict
import re
import operator
import sys

'''
This script takes a txt log as input, output configurations of each carrier-freq pair
September 2020
Zizheng Liu

Usage:
python3 collectConfigs/clctCfg.py intputlog.txt > output.txt

Updates:
suppress printing with less than min_cnt; 
print report type only with type_only; merge by freq only with no_cid;
set filter on meas state without pcell meas with no_filter;
'''
# signs of identifying cells: 'RRCREQ', 'Handoff' and 'RRCREEST.*from'
# Assumption: configuration under the same frequency should be the same or
# similar when its neighbour cells remain unchanged.

def logNameFromLine(l):
    return l.replace('###', '').replace('[new log] ', '').replace('end', '').strip()

def deviceFromLogName(logName):
    return logName.split('/')[-1].split('_')[4].strip()

def carrierFromLogName(logName):
    return logName.split('/')[-1].replace('.mi3log', '').split('_')[-1].strip()

def getDevCar(l):
    logName = logNameFromLine(l)
    device = deviceFromLogName(logName)
    carrier = carrierFromLogName(logName)
    return '_'.join([device, carrier])

def fileName2dtHms(filename):
    find = re.compile(r'diag_log_[0-9]{8}_[0-9]{6}_')
    result = find.search(filename)
    # time_cmp = result[0].replace('diag_log_', '').strip('_')
    time_cmp = result.group().replace('diag_log_', '').strip('_')
    dt_hms = int(time_cmp.split('_')[0]) * 1000000 + int(time_cmp.split('_')[1])
    return dt_hms

def sortByTime(inputFile):
    f = open(inputFile, 'r')
    lines = f.readlines()
    log_flag, current_dev_car = False, ''
    dev_car_dict_time_list = defaultdict(list) # {dev_car: [time1, time2, ...]}
    dev_car_time_dict_path = {}
    '''
    Extract log information including device, carrier and time
    '''
    for l in lines:
        if '###[new log]' in l:
            current_dev_car = getDevCar(l)
            log_flag = True
        elif '###end' in l and log_flag:
            try:
                this_dev_car = getDevCar(l)
                logName = logNameFromLine(l)
                if this_dev_car == current_dev_car:
                    log_dt_hms = fileName2dtHms(logName)
                    dev_car_dict_time_list[this_dev_car].append(log_dt_hms)
                    dev_car_time_dict_path[this_dev_car + str(log_dt_hms)] = logName
                else:
                    # print('\t'.join([current_dev_car, this_dev_car, 'Wrong order']))
                    pass
            except:
                pass
            log_flag, current_dev_car = False, ''
    '''
    Sort logs of each device_carrier pair by time
    '''
    dev_car_dict_path_list = defaultdict(list)
    for k, time_list in dev_car_dict_time_list.items():
        for t in sorted(time_list):
            dev_car_dict_path_list[k].append(dev_car_time_dict_path[k + str(t)])
    return dev_car_dict_path_list

def segLog(tfile):
    f = open(tfile, 'r')
    lines = f.readlines()
    segDict = defaultdict(str)
    in_log_flag = False
    current_log = ''
    for l in lines:
        if '###[new log]' in l:
            in_log_flag = True
            current_log = l.replace('###', '').replace('[new log] ', '').strip()
            #print(current_log)
        elif '###end' in l and in_log_flag:
            segDict[current_log] += l
            in_log_flag = False
            current_log = ''
        if in_log_flag and current_log:
            segDict[current_log] += l
    return segDict          

def getFreq(logline, keyword, no_cid = False):
    if keyword == 'rrcreq':
        if no_cid:
            return logline.split(' ')[4].split('-')[0]
        else:
            return logline.split(' ')[4]
    if keyword == 'rrcreest':
        if no_cid:
            return logline.split(' ')[5].split('-')[0]
        else:
            return logline.split(' ')[5]
    if keyword == 'handoff':
        cid_freq = logline.split(' ')[-1]
        cid = cid_freq.split('(')[0]
        freq = cid_freq.split(',')[-1].replace(')', '')
        if no_cid:
            return freq
        else:
            return freq + '-' + cid

def checkFreqChange(logline, next_lines):
    freq = None
    ptn = r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+ Measurement report [0-9]{1} a[3,4,5]{1} .*' #e.g., 2019-05-15 19:54:55.032182 Measurement report 1 a3
    result = re.compile(ptn).match(logline)
    if result:
        for n in next_lines:
            if ' Handoff to ' in n:
                freq = getFreq(n, 'handoff')
                break
    if ' RRCREQ ' in logline:
        freq = getFreq(logline, 'rrcreq')
        # print(freq)
    elif ' RRCREEST ' in logline and 'LOG' not in logline:
        freq = getFreq(logline, 'rrcreest')
        # print(freq)
    elif ' Handoff to ' in logline:
        freq = getFreq(logline, 'handoff')
    return freq

def parseObj(logline):
    freq = logline.split(' ')[2]
    cell = logline.split(' ')[4]
    if cell == 'None':
        return freq + '_' + 'P'
    else:
        return freq + '_' + 'S'

def parseRpt(logline, nextline, type_only = False):
    htss = logline.split(' ')[2]
    ttt = logline.split(' ')[3]
    tp = nextline.split(' ')[0]
    ofst = nextline.split(' ')[1] + '_' + nextline.split(' ')[2]
    if not type_only: 
        return 'type: ' + tp + ', thr/ofst: ' + ofst + \
            ', hetersis: ' + htss + ', time_to_trigger: ' + ttt
    else:
        return 'type: ' + tp

def mapObjRpt(freq_id_dict, report_dict, logline):
    ids = logline.split('(')[-1]
    freq_id = ids.split(',')[0].strip('\' ')
    rpt_id = ids.split(',')[-1].strip('\') ')
    try:
        return report_dict[rpt_id].replace('type', freq_id_dict[freq_id])
    except:
        return None      

def extractConfig(segDict, dev_car_seq, no_filter = True):
    counter = {} # carrier: frequency: measstate: cnt
    for dev_car, loglist in dev_car_seq.items():
        # print(dev_car)
        car = dev_car.split('_')[-1]
        if car not in counter:
            counter[car] = {}
        measstate = set()
        freq = ''
        freq_id_dict = {} # e.g., {'1': '9820_P', '2': '2175_S'}
        report_dict = {} # e.g., {'1': 'type: a1, thr/ofst: 3.0 None, hetersis: 1.0, tt_trigger: 80'}
        obj_flag, rpt_flag, map_flag = False, False, False
        one_meas_state = set()
        for l in loglist:
            logcontent = segDict[l]
            logcontent = logcontent.split('\n')

            last_meas_state = set()

            for i in range(0, len(logcontent)):
                logline = logcontent[i]
                next_lines = logcontent[i: i + 20]
                if checkFreqChange(logline, next_lines):
                    freq = checkFreqChange(logline, next_lines)
                if freq == '':
                    continue
                if 'LteMeasObjectEutra ' in logline:
                    obj_flag = True
                    obj_id = logline.split(' ')[1]
                    freq_id_dict[obj_id] = parseObj(logline)
                if obj_flag and 'LteReportConfig ' in logline:
                    rpt_flag = True
                    nextline = logcontent[i+1]
                    rpt_id = logline.split(' ')[1]
                    report_dict[rpt_id] = parseRpt(logline, nextline)
                    # print(parseRpt(logline, nextline))
                if rpt_flag and 'MeasObj ' in logline:
                    map_flag = True
                    meas_cfg_entry = mapObjRpt(freq_id_dict, report_dict, logline)
                    if meas_cfg_entry:
                        one_meas_state.add(meas_cfg_entry)
                if map_flag and 'MeasObj ' not in logline:
                    # one_meas_state.add(l)
                    one_meas_state = frozenset(one_meas_state)
                    #print(one_meas_state)
                    if freq not in counter[car]:
                        counter[car][freq] = {}

                    added_meas = set()
                    added_meas = one_meas_state.difference(last_meas_state)

                    removed_meas = set()
                    removed_meas = last_meas_state.difference(one_meas_state)

                    delta_state = [added_meas, removed_meas]
                    delta_state_union = added_meas.union(removed_meas)

                    if one_meas_state:
                        pcell_included = False
                        for meas_cfg_entry in one_meas_state:
                            if freq in meas_cfg_entry:
                                pcell_included = True
                        if pcell_included or no_filter: # filter meas with serving cell
                            if len(delta_state_union) > 0:
                                if delta_state_union not in counter[car][freq]:
                                    counter[car][freq][delta_state_union] = [delta_state, 0]
                                counter[car][freq][delta_state_union][1] += 1
                    freq_id_dict = {} # e.g., {'1': '9820_P', '2': '2175_S'}
                    report_dict = {} # e.g., {'1': 'type: a1, thr/ofst: 3.0 None, hetersis: 1.0, tt_trigger: 80'}
                    obj_flag, rpt_flag, map_flag = False, False, False
                    last_meas_state = one_meas_state.copy()
                    one_meas_state = set()
    return counter

def printer(counter, min_cnt = 0):
    for car, freq_deltastate_count in counter.items():
        print('Carrier: ' + car)
        # if car == '310410':
        for freq, deltastate_cnt in freq_deltastate_count.items():
            print('\tFrequency: ' + freq, 'Cnt:', len(deltastate_cnt))
            for k,v in sorted(deltastate_cnt.items(), key=lambda item: item[1], reverse=True):
            # for measstate, cnt in dict(sorted(measstate_cnt.items(), key=operator.itemgetter(1), reverse=True)).items():
                if v[1] > min_cnt:
                    print('\t\tMeasurement delta state count: ' + str(v[1]))
                    print('\t\t\tAdded delta state: ')
                    for cfg in v[0][0]:
                        print('\t\t\t\t' + cfg)
                    print('\t\t\tRemoved delta state: ')
                    for cfg in v[0][1]:
                        print('\t\t\t\t' + cfg)


if __name__ == "__main__":

    inputFile = sys.argv[1]

    '''
    Divid the logs by (device, carrier) pair and sort logs in each 
    (device, carrier) pair by time.
    
    device_seq: dict {device_carrier: [log1_path, log2_path, ...]} logs here are sorted by time
    '''
    dev_car_seq = sortByTime(inputFile)
    # for dev_car, loglist in dev_car_seq.items():
        # print(dev_car, end='\t')
    
    '''
    Store log data in a dictionary
    '''
    print('Loading logs...')
    segDict = segLog(inputFile)

    '''
    Scan logs, extract configs
    '''
    print('Scanning logs, collecting configs...')
    counter = extractConfig(segDict, dev_car_seq)

    '''
    Print the result
    '''
    printer(counter)





