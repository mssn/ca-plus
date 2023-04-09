import collections
import os
import sys
import math
import datetime
from collections import OrderedDict

input_file = sys.argv[1]
output_path = sys.argv[2]

def check_missed_4g(state_list, max_freq_list):
    missed_freq_list = []
    for item in state_list:
        flag = 1
        for freq, v in item.items():
            if freq not in max_freq_list:
                flag = 0
                break 
        if not flag:
            break
        for freq, v in max_freq_list.items():
            if freq not in item:
                missed_freq_list.append(freq)

    return missed_freq_list

pcell_str = ""
added_scell_freq_list = {}
removed_freq_list = {}

scell_freq_list = {}

missed_list = {}
state_list = []
max_freq_num = 0
max_freq_list = {}

total_dsm_num = 0

op = None

with open(input_file, 'r', encoding='utf-8-sig') as lines:
    for line in lines:
        
        if "Carrier" in line:
            items = line.strip().split(' ')
            if len(items) >= 2:
                op = items[1]
            else:
                op = None

        if "Frequency" in line:
            if len(state_list) > 1:
                missed_4g_freq_list = check_missed_4g(state_list, max_freq_list)
                if len(missed_4g_freq_list) > 0:
                    missed_list[pcell_str] = [missed_4g_freq_list, op]

            items = line.strip().split(' ')
            pcell_str = items[1]

            state_list = []
            scell_freq_list = {}
            added_scell_freq_list = {}
            removed_scell_freq_list = {}
            max_freq_list = {}
            max_freq_num = 0
        
        if "Measurement delta state count" in line:

            if len(added_scell_freq_list) > 0 and len(removed_freq_list) == 0:
                state_list.append(added_scell_freq_list)
                freq_num = len(added_scell_freq_list)
                if freq_num > max_freq_num:
                    max_freq_num = freq_num
                    max_freq_list = added_scell_freq_list

            added_flag = 0
            removed_flag = 0

            added_scell_freq_list = {}
            removed_freq_list = {}
            
        if "Added delta state" in line:
            added_flag = 1
            removed_flag = 0
        elif "Removed delta state" in line:
            removed_flag = 1
            added_flag = 0
            
        if "_S" in line:
            items = line.strip().split(': ')
            freq = items[0]
            event = items[1].strip(', thr/ofst')
            thres = items[2].strip(', hetersis')
            
            if added_flag == 1:
                added_scell_freq_list[freq] = 1
                scell_freq_list[freq] = 1

            if removed_flag == 1:
                removed_freq_list[freq] = 1

        if "_P" in line:
            items = line.strip().split(': ')
            freq = items[0]
            event = items[1].strip(', thr/ofst')
            thres = items[2].strip(', hetersis')

            if removed_flag == 1:
                removed_freq_list[freq] = 1
                
#print(a1a2_loop_dict)
#print(total_dsm_num)
                
p = output_path + "/" + "misconfig_missed_4g.csv"
fout = open(p, 'w')
fout.write('operator,pcell,freq\n')
for k1, v1 in missed_list.items():
    for item in v1[0]:
        if v1[1]:
            line = str(v1[1]) + ',' + str(k1) + ',' + str(item)
            fout.write(line + '\n')
fout.close()
    
