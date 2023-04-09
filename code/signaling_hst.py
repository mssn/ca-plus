#!/usr/bin/python

import collections
import numpy as np
import os, sys
import pickle
import re
import ast

import random

enb_to_cell = {}
cell_to_enb = {}

infer_time = 100
infer_delta = 20

def get_colocated_cells(cell, timestamp):
	eid = get_enb_id(cell, timestamp)
	if not eid or eid[:5] not in enb_to_cell:
		return set()

	return enb_to_cell[eid[:5]]

def get_enb_id(cell, timestamp):
	if cell not in cell_to_enb:
		return ""

	for eid, lst in cell_to_enb[cell].items():
		for t in lst:
			if abs(float(t) - float(timestamp)) <= 30:
				return eid 

	return ""

def is_good_cell_report(config):
	return (config['event_type'] == 'a3' and config['offset'] >= 0) or (config['event_type'] == 'a4') or (config['event_type'] == 'a5' and config['threshold1'] <= config['threshold2'])

def get_eca_signaling(intra_report, inter_report, measured_intra, measured_inter):
	good_cells = get_good_cells(intra_report, inter_report)
	good_cells_num = len(good_cells)

	# print(intra_report,inter_report,good_cells)

	# group good cells
	bs_to_freq = {}
	index = 0

	removed = {x:False for x in good_cells}
	
	for cell in good_cells:
		if removed[cell]:
			continue

		t = good_cells[cell]
		bs_to_freq[index] = set([cell[1]])

		colocated = get_colocated_cells(cell,t)
		for c1 in colocated:
			for i in range(-2,3):
				for c2 in good_cells:
					if removed[c2]:
						continue
					if c1[0] + i == c2[0]:
						bs_to_freq[index].add(c2[1])
						removed[c2] = True

		index += 1
		removed[cell] = True

	# print("intermediate:", good_cells, bs_to_freq)

	intra_freq = set([x[1] for x in measured_intra])
	intra_covered = set()
	for bs,st in bs_to_freq.items():
		if len(st.intersection(intra_freq)) > 0:
			intra_covered.add(bs)

	if len(intra_covered) == len(bs_to_freq):
		return len(good_cells), len(set([x[1] for x in good_cells])), 1

	freq_to_access = set()

	remaining = set(bs_to_freq.keys())
	if remaining.intersection(intra_covered):
		freq_to_access.add('intra')

	remaining = remaining.difference(intra_covered)

	inter_covered = {}
	for f in set([x[1] for x in measured_inter]):
		for bs,st in bs_to_freq.items():
			if f in st:
				if f not in inter_covered:
					inter_covered[f] = set()
				inter_covered[f].add(bs)

	while remaining:
		max_freq, max_num = None,0
		for f, st in inter_covered.items():
			num = len(remaining.intersection(st))
			if num > max_num:
				max_freq,max_num = f,num

		if not max_freq:
			# freq_to_access = inter_report.keys()
			for bs in remaining:
				freq_to_access = freq_to_access.union(bs_to_freq[bs])
			break

		else:
			remaining = remaining.difference(inter_covered[max_freq])
			del inter_covered[max_freq]
			freq_to_access.add(max_freq)

	return len(good_cells), len(set([x[1] for x in good_cells])), len(freq_to_access)

	# intra_covered = set()
	# inter_covered = {}

	# for c, t in good_cells.items():
	# 	colocated = get_colocated_cells(c,t)
	# 	for c1 in colocated:
	# 		for i in range(-2,3):
	# 			for c2 in measured_intra:
	# 				if c1[0] + i == c2[0]:
	# 					intra_covered.add(c)
	# 			for c2 in measured_inter:
	# 				if c1[0] + i == c2[0]:
	# 					if c2[1] not in inter_covered:
	# 						inter_covered[c2[1]] = set()
	# 					inter_covered[c2[1]].add(c)

	# # print("intermediate",good_cells, intra_covered, inter_covered)

	# if len(intra_covered) == len(good_cells):
	# 	return 0, len(good_cells)

	# remaining = set(good_cells.keys()).difference(intra_covered)

	# freq_to_access = []
	# while remaining and len(freq_to_access) < len(inter_report):
	# 	max_freq, max_num = None,0
	# 	for f, st in inter_covered.items():
	# 		num = len(remaining.intersection(st))
	# 		if num > max_num:
	# 			max_freq,max_num = f,num

	# 	if not max_freq:
	# 		freq_to_access = inter_report.keys()

	# 	else:
	# 		remaining = remaining.difference(inter_covered[max_freq])
	# 		del inter_covered[max_freq]
	# 		freq_to_access.append(max_freq)

	# # print("results",freq_to_access)

	# return len(freq_to_access), len(good_cells)


def get_good_cells(intra, inter):
	good_cells = {}

	for r in intra:
		cell = (int(r['report_pid']), int(['report_freq']))
		good_cells[cell] = float(r['timestamp'])

		if cell in cell_to_enb:
			global_id = ""
			for eid, lst in cell_to_enb[cell].items():
				for t in lst:
					if abs(float(t) - float(r['timestamp'])) <= 30:
						global_id = eid 
						break
				if global_id:
					break

			if global_id:
				enb = global_id[:5]
				if enb in enb_to_cell:
					for cell2 in enb_to_cell[enb]:
						if int(cell2[1]) != cell[1]:
							good_cells[cell2] = float(r['timestamp'])
							# good_cells.add(cell2)

	for _,reports in inter.items():
		for r in reports:
			cell = (int(r['report_pid']), int(r['report_freq']))
			good_cells[cell] = float(r['timestamp'])

			if cell in cell_to_enb:
				global_id = ""
				for eid, lst in cell_to_enb[cell].items():
					for t in lst:
						if abs(float(t) - float(r['timestamp'])) <= 30:
							global_id = eid 
							break
					if global_id:
						break

				if global_id:
					enb = global_id[:5]
					if enb in enb_to_cell:
						for cell2 in enb_to_cell[enb]:
							if int(cell2[1]) != cell[1]:
								good_cells[cell2] = float(r['timestamp'])
							# good_cells.add(cell2)

	return good_cells

def process_signaling(file):
	global infer_time, infer_delta

	cur_pid, cur_freq = None, None

	intra_report = []
	inter_report = {}

	# reported_good_intra = set()
	# reported_good_inter = {}

	measured_intra = {}
	measured_inter = {}

	meas_gap = 40

	# no_impact = 0
	no_impact_list = []

	fcnt = 0 
	with open(file, 'r') as lines:
		for line in lines:
			# if "[File Name]" in line:
			# 	fcnt += 1
			# 	if fcnt > 100:
			# 		break

			# 1564478320.010058 [measurementReport] reportNeighborCell 182 1825 {'event_type': 'a3', 'offset': 1.5, 'hyst': 1.5, 'reportInterval': 'ms480', 'reportAmount': 'infinity', 'timeToTrigger': 'ms160', 'triggerQuantity': 'rsrp', 'report_id': '1'} serving: -17.0 -103 target: -12.5 -97 181 1825
			if "[measurementReport] reportNeighborCell" in line:
				start, end = line.find('{'), line.rfind('}')

				tmp_lst = line[end+1:].strip().split(' ')
				c_pid, c_freq = int(tmp_lst[-2]), int(tmp_lst[-1])

				config = ast.literal_eval(line[start:end+1])
				if is_good_cell_report(config):
					items = line[:start].strip().split(' ')
					try:
						m_pid, m_freq = int(items[-2]), int(items[-1])
					except:
						continue
						
					new_report = {'timestamp':items[0], 'config':config, 'report_pid':m_pid, 'report_freq':m_freq}
					if m_freq == c_freq:
						# reported_good_intra.add((m_pid,m_freq))
						if intra_report and intra_report[0]['timestamp'] != items[0]: 
							del intra_report[:]
							intra_report.append(new_report)
						measured_intra[(m_pid,m_freq)] = float(items[0])

					else:
						if m_freq in inter_report and inter_report[m_freq][0]['timestamp'] != items[0]:
							del inter_report[m_freq][:]
						if m_freq not in inter_report:
							inter_report[m_freq] = []
						inter_report[m_freq].append(new_report)

						measured_inter[(m_pid,m_freq)] = float(items[0])

			# 1564478309.220078 [servingCellMeas] PCell 148 1825 148 1825 -109.5625 -21.125
			if "[servingCellMeas]" in line:
				items = line.strip().split()
				measured_intra[(int(items[3]), int(items[4]))] = float(items[0])

			# 1564482364.335012 [connectedIntraNeighbor] PCell 294 1850 296 1850 -99.5 -17.6875
			if "[connectedIntraNeighbor]" in line:
				items = line.strip().split()
				try:
					measured_intra[(int(items[3]), int(items[4]))] = float(items[0])
				except:
					pass

				try:
					measured_intra[(int(items[5]), int(items[6]))] = float(items[0])
				except:
					pass

			# 1564482365.955753 [connectedInterNeighbor] 294 1850 166 2452 -126.6875 -18.75
			if "[connectedInterNeighbor]" in line:
				items = line.strip().split()
				m_pid, m_freq = int(items[4]), int(items[5])
				measured_inter[(m_pid,m_freq)] = float(items[0])

			# 1564485048.907050 [LTE_RRC_OTA_Packet] MeasGap 0
			if "[LTE_RRC_OTA_Packet] MeasGap" in line:
				index = int(line.strip().split()[-1])
				if index == 1:
					meas_gap = 80

			# 1564485029.112901 [activeHandoff] 485 1650 122 1850 {'event_type': 'a3', 'offset': 1.5, 'hyst': 1.5, 'reportInterval': 'ms1024', 'reportAmount': 'r1', 'timeToTrigger': 'ms160', 'triggerQuantity': 'rsrp', 'report_id': '1'} serving: -14.5 -108, target: -10.5 -101
			if "[activeHandoff]" in line or "[idleHandoff]" in line:
				items = line.strip().split()

				old_pci, old_freq = int(items[2]), int(items[3])
				new_pci, new_freq = int(items[4]), int(items[5])

				if inter_report or intra_report:
					# final_time = 0 
					# total_cell_cnt = len(intra_report)
					# if intra_report:
					# 	intra_ttt = int(intra_report[0]['config']['timeToTrigger'][2:])
					# 	final_time = random.gauss(intra_ttt * 1.1, intra_ttt * 0.05)

					# ttt_lst = []
					# for f in inter_report:
					# 	ttt_lst.append(int(inter_report[f][0]['config']['timeToTrigger'][2:]))
					# 	total_cell_cnt += len(inter_report[f])

					# ttt_lst.sort(reverse=True)
					# for i in range(len(ttt_lst)):
					# 	# final_time = max(final_time, (i+1)*meas_gap + ttt_lst[i])
					# 	final_time = max(final_time, (i+1)*meas_gap + random.gauss(ttt_lst[i] * 1.1, ttt_lst[i]*0.05))

					# final_time /= total_cell_cnt

					# eca_freq_num, good_cell_num = get_eca_time(intra_report, inter_report, measured_intra, measured_inter)

					# if (eca_freq_num > 0 or good_cell_num > total_cell_cnt or inter_report): # otherwise, it would be no change
					# 	eca_time = eca_freq_num * meas_gap + random.gauss(infer_time, infer_delta)
					# 	# todo 
					# 	print("change", final_time, eca_time/good_cell_num)

					# else:
					# 	print("no-change", final_time, final_time)

					good_cell_num, good_cell_freq_num, eca_freq_num = get_eca_signaling(intra_report, inter_report, measured_intra, measured_inter)
					# get_eca_signaling(intra_report, inter_report, measured_intra, measured_inter)
					print(good_cell_freq_num, eca_freq_num)

				measured_intra.clear()
				measured_inter.clear()

				del intra_report[:]
				inter_report.clear()

				meas_gap = 40


			# 1564484976.996855 [idleHandoff] 357 1850 359 1850
			# if "[idleHandoff]" in line:
			# 	items = line.strip().split()
			# 	old_pci, old_freq = int(items[2]), int(items[3])
			# 	new_pci, new_freq = int(items[4]), int(items[5])
				
			# 	measured_intra.clear()
			# 	measured_inter.clear()

			# 	del intra_report[:]
			# 	inter_report.clear()

			# 	meas_gap = 40

			if "RRCConnectionSetupComplete" in line or "RRCConnectionReestablishmentComplete" in line: 

				measured_intra.clear()
				measured_inter.clear()

				del intra_report[:]
				inter_report.clear()

				meas_gap = 40


if __name__ == '__main__':
	i = 0
	with open(sys.argv[2],'r') as lines:
		for line in lines:
			if i == 0:
				enb_to_cell = ast.literal_eval(line)
			if i == 1:
				cell_to_enb = ast.literal_eval(line)
			i += 1
		# for line in lines:
		# 	# (255, 1825):875f9b00:1564482779.434268,e30333e0:1564483192.773989
		# 	start = line.find(':')

		# 	tmp = line[1:start-1].split(', ')
		# 	pci, freq = int(tmp[0]), int(tmp[1])
		# 	cell = (pci,freq)

		# 	items = line[start+1:].strip().split(',')
		# 	for item in items:
		# 		tmp = item.split(':')
		# 		enb, ts = tmp[0], float(tmp[1])

		# 		if cell not in cell_to_enb:
		# 			cell_to_enb[cell] = {}
		# 		cell_to_enb[cell][enb] = ts 

		# 		if enb[:5] not in enb_to_cell:
		# 			enb_to_cell[enb[:5]] = set()
		# 		enb_to_cell[enb[:5]].add(cell)

	# print(len(enb_to_cell), len(cell_to_enb))

	process_signaling(sys.argv[1])

