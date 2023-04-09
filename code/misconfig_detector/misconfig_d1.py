import collections
import os
import sys
import math
import datetime
from collections import OrderedDict

input_file = sys.argv[1]
output_path = sys.argv[2]

def check_a1a2_loop(added_meas, removed_meas):
    loop_list = {}
    for freq, v in added_meas.items():
        if freq in removed_meas:
            thres_list_a1 = []
            thres_list_a2 = []
            if 'a1' in added_meas[freq] and 'a2' in removed_meas[freq]:
                thres_list_a1 = added_meas[freq]['a1']
                thres_list_a2 = removed_meas[freq]['a2']
            if 'a2' in added_meas[freq] and 'a1' in removed_meas[freq]:
                thres_list_a1 = removed_meas[freq]['a1']
                thres_list_a2 = added_meas[freq]['a2']
            for thres_a1 in thres_list_a1:
                for thres_a2 in thres_list_a2:
                    threshold_a1 = float(thres_a1.strip('_Non'))
                    threshold_a2 = float(thres_a2.strip('_Non'))
                    
                    if threshold_a1 < threshold_a2:
                        if (threshold_a1 <= -40 and threshold_a2 <= -40) or (threshold_a1 > -40 and threshold_a2 > -40):
                            if freq not in loop_list:
                                loop_list[freq] = {}
                            threshold_str = str(threshold_a1) + '|' + str(threshold_a2)
                            if threshold_str not in loop_list[freq]:
                                loop_list[freq][threshold_str] = [threshold_a1, threshold_a2, 0]
                            loop_list[freq][threshold_str][2] += 1
    return loop_list

def check_b1a2_loop(added_meas, removed_meas):
    loop_list = {}
    for freq, v in added_meas.items():
        if freq in removed_meas:
            thres_list_b1 = []
            thres_list_a2 = []
            if 'b1_n' in added_meas[freq] and 'a2' in removed_meas[freq]:
                thres_list_b1 = added_meas[freq]['b1_n']
                thres_list_a2 = removed_meas[freq]['a2']
            if 'a2' in added_meas[freq] and 'b1_n' in removed_meas[freq]:
                thres_list_b1 = removed_meas[freq]['b1_n']
                thres_list_a2 = added_meas[freq]['a2']
            for thres_b1 in thres_list_b1:
                for thres_a2 in thres_list_a2:
                    threshold_b1 = float(thres_b1.strip('_Non'))
                    threshold_a2 = float(thres_a2.strip('_Non'))
                    
                    if threshold_b1 < threshold_a2:
                        if (threshold_b1 <= -40 and threshold_a2 <= -40) or (threshold_b1 > -40 and threshold_a2 > -40):
                            if freq not in loop_list:
                                loop_list[freq] = {}
                            threshold_str = str(threshold_b1) + '|' + str(threshold_a2)
                            if threshold_str not in loop_list[freq]:
                                loop_list[freq][threshold_str] = [threshold_b1, threshold_a2, 0]
                            loop_list[freq][threshold_str][2] += 1
    
    return loop_list

a1a2_loop_dict = {}
b1a2_loop_dict = {}
pcell_str = ""
added_meas = {}
removed_meas = {}

total_dsm_num = 0

with open(input_file, 'r', encoding='utf-8-sig') as lines:
    for line in lines:
        
        if "Frequency" in line:
            items = line.strip().split(' ')
            pcell_str = items[1]
            added_meas = {}
            removed_meas = {}
            delta_config_list = []
        
        if "Measurement delta state count" in line:
            #print(added_meas)
            #print(removed_meas)
            a1a2_loop_list = check_a1a2_loop(added_meas, removed_meas)
            b1a2_loop_list = check_b1a2_loop(added_meas, removed_meas)
            
            if len(a1a2_loop_list) > 0:
                #print(line)
                print(a1a2_loop_list)
                a1a2_loop_dict[pcell_str] = a1a2_loop_list
            
            if len(b1a2_loop_list) > 0:
                #print(pcell_str)
                print(b1a2_loop_list)
                b1a2_loop_dict[pcell_str] = b1a2_loop_list
            
            total_dsm_num += 1
            
            added_meas = {}
            removed_meas = {}
            added_flag = 0
            removed_flag = 0
            a1a2_loop_list = {}
            b1a2_loop_list = {}
            
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
                if freq not in added_meas:
                    added_meas[freq] = {}
                if event not in added_meas[freq]:
                    added_meas[freq][event] = []
                added_meas[freq][event].append(thres)
            elif removed_flag == 1:
                if freq not in removed_meas:
                    removed_meas[freq] = {}
                if event not in removed_meas[freq]:
                    removed_meas[freq][event] = []
                removed_meas[freq][event].append(thres)
                
#print(a1a2_loop_dict)
#print(total_dsm_num)
                
p = output_path + "/" + "misconfig_a1a2.csv"
fout = open(p, 'w')
fout.write('pcell,freq,thres_a1,thres_a2,count\n')
for k1, v1 in a1a2_loop_dict.items():
    for k2, v2 in v1.items():
        for k3, v3 in v2.items():
            line = str(k1) + ',' + str(k2) + ',' + str(v3[0]) + ',' + str(v3[1]) + ',' + str(v3[2])
            fout.write(line + '\n')
fout.close()

p = output_path + "/" + "misconfig_b1a2.csv"
fout = open(p, 'w')
fout.write('pcell,freq,thres_b1,thres_a2,count\n')
for k1, v1 in b1a2_loop_dict.items():
    for k2, v2 in v1.items():
        for k3, v3 in v2.items():
            line = str(k1) + ',' + str(k2) + ',' + str(v3[0]) + ',' + str(v3[1]) + ',' + str(v3[2])
            fout.write(line + '\n')
fout.close()
    
