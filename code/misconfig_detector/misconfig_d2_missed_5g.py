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
missed_list = {}

total_dsm_num = 0

op = None

freq_list = {
    '5330': 1,
    '2050': 1,
    '39874': 1,
    '40072': 1
}

with open(input_file, 'r', encoding='utf-8-sig') as lines:
    for line in lines:
        
        if "Carrier" in line:
            items = line.strip().split(' ')
            if len(items) >= 2:
                op = items[1]
            else:
                op = None

        if "Frequency" in line:

            items = line.strip().split(' ')
            pcell_str = items[1]
            pcell_freq = (pcell_str.split('-'))[0]

            if pcell_freq in freq_list:
                missed_list[pcell_str] = op
                
#print(a1a2_loop_dict)
#print(total_dsm_num)
                
p = output_path + "/" + "misconfig_missed_5g.csv"
fout = open(p, 'w')
fout.write('operator,pcell\n')
for k1, v1 in missed_list.items():
    line = str(v1) + ',' + str(k1)
    fout.write(line + '\n')
fout.close()
    
