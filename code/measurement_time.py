#!/usr/bin/python

import pandas as pd
import sys, os
import ast 
import random

unknown_channel = set([])

op = ''

def get_freq_to_meas(cells):
	remaining = set([x[1] for x in cells])

	freq_to_bs = {}
	for freq, pci in cells:
		if freq not in freq_to_bs:
			freq_to_bs[freq] = set()
		freq_to_bs[freq].add(pci)

	freq_cnt = len(freq_to_bs)
	# print(remaining, freq_to_bs)

	# print(remaining, freq_to_bs)

	cnt = 0
	meas_freq = [] 
	while remaining:
		max_num,max_freq = 0,None

		for f, st in freq_to_bs.items():
			if len(remaining.intersection(st)) > max_num:
				max_num,max_freq = len(remaining.intersection(st)), f
				# print(max_num,max_freq,remaining.intersection(st))

		# try:
			
		# except:
		# 	print(freq_to_bs, max_freq, remaining, cells)
		# 	sys.exit(-1)
		remaining = remaining.difference(freq_to_bs[max_freq])
		cnt += 1
		meas_freq.append(max_freq)
		del freq_to_bs[max_freq]

	return freq_cnt, meas_freq 

def get_meas_time(df):
	print(len(df))
	df = df[df.measurement_time != 'None']
	df = df.astype({'measurement_time': 'float64'}, copy=False)
	print(len(df))

	for area, group in df.groupby(by=["region"]):
		print(area, len(group))

		# improved, no_impact = 0,0

		for _, row in group.iterrows():
			org_time, reported_5g_cell = float(row['measurement_time']), int(row['measured_5g_cell_num'])
			good_cell_dict = ast.literal_eval(row['good_5g_cell_list'])

			# {'499-174270': -80.0, '873-174270': -79.0, '902-174270': -91.0, '985-174270': -87.0, '193-2256663': -108.5, '193-2261661': -108.0, '571-2256663': 'Unknown', '571-2258329': 'Unknown', '571-2259995': 'Unknown', '571-2261661': 'Unknown', '193-2258329': 'Unknown', '193-2259995': 'Unknown'}
			# print(type(good_cells), good_cells)
			# break

			good_cell_list = sorted([(int(x.split('-')[1]),int(x.split('-')[0])) for x in good_cell_dict])

			if reported_5g_cell == 0 or not good_cell_list:
				continue

			org_cnt, eca_cnt = get_freq_to_meas(good_cell_list)
			eca_cnt = len(eca_cnt)

			if org_cnt == eca_cnt:
				print(org_time/reported_5g_cell, org_time/len(good_cell_list))
				# no_impact += 1
			else:
				print(org_time/reported_5g_cell, (eca_cnt * 80 + random.gauss(100,20)) / (1000 * len(good_cell_list)))

			# print(good_cell_list, freq_to_meas)
			# break


# def get_ctrl_msg_num(cells):
# 	freq_cnt, eca_measured = get_freq_to_meas(cells)

# 	remaining_bs = set([x[1] for x in cells])

# 	freq_to_bs = {}
# 	for f,c in cells:
# 		if f not in freq_to_bs:
# 			freq_to_bs[f] = set()
# 		freq_to_bs[f].add(c)

# 	cnt = 0
# 	while remaining_bs:
# 		max_f, max_num = None,0
# 		for f in eca_measured:
# 			if len(remaining_bs.intersection(freq_to_bs[f])) > max_num:
# 				max_f,max_num = f, len(remaining_bs.intersection(freq_to_bs[f]))

# 		remaining_bs.difference(freq_to_bs[max_f])
# 		cnt += 1
# 		del freq_to_bs[max_f]

# 	return freq_cnt, cnt

def get_signaling(df):
	print(len(df))
	df = df[df.measurement_time != 'None']
	df = df.astype({'measurement_time': 'float64'}, copy=False)
	print(len(df))

	for area, group in df.groupby(by=["region"]):
		print(area, len(group))
		for _, row in group.iterrows():
			good_cell_dict = ast.literal_eval(row['good_5g_cell_list'])
			good_cell_list = sorted([(int(x.split('-')[1]),int(x.split('-')[0])) for x in good_cell_dict])

			legacy_msg_num, eca_num = get_freq_to_meas(good_cell_list)
			eca_num = len(eca_num)

			print(legacy_msg_num, eca_num)

def lte_bandwidth_att(f):
	freq_to_bandwidth = {675:15, 850:20, 1025:5, 5110:10, 5330:10, 9820:10, 9840:5, 
	52740:20, 52940:20, 52941:20, 53139:20, 53140:20, 53141:20, 53339:20, 53341:20, 53539:20,
	66486:10, 66661:5, 66686:10, 66936:10, 67086:10}

	if int(f) not in freq_to_bandwidth:
		unknown_channel.add(int(f))
		return 0
	return freq_to_bandwidth[int(f)]

def lte_bandwidth_tmo(f):
	freq_to_bandwidth = {677:15, 1123:15, 1200:10, 1275:15, 1475:5, 5035:5, 39874:20, 40072:20, 66736:10, 66811:15, 67011:5}

	if int(f) not in freq_to_bandwidth:
		unknown_channel.add(int(f))
		return 0
	return freq_to_bandwidth[int(f)]

def lte_bandwidth_vrz(f):
	freq_to_bandwidth = {677:15, 925:5, 1000:20, 1050:10, 1075:10, 1100:10, 1123:10, 1300:10, 1550:10, 2050:20, 2100:20, 2350:10, 2561:10,5035:10, 5230:10, 66536:20, 66586:10, 66811:10, 66836:10, 66911:10, 67011:5, 67086:10}

	if int(f) > 40000 and int(f) < 60000:
		return 10
	elif int(f) in freq_to_bandwidth:
		return freq_to_bandwidth[int(f)]
	else:
		unknown_channel.add(int(f))
		return 0

def lte_bandwidth(f):
	if f == "None":
		return 0

	if op.startswith('a') or op.startswith('A'):
		return lte_bandwidth_att(f)

	if op.startswith('t') or op.startswith('T'):
		return lte_bandwidth_tmo(f)

	if op.startswith('v') or op.startswith('V'):
		return lte_bandwidth_vrz(f)


def nr_bandwidth_att(f):
	if int(f) < 1e6:
		return 5
	return 100

def nr_bandwidth_tmo(f):
	freq_to_bandwidth_nr = {125290:20, 125330:15, 519630:100, 521550:80}
	if int(f) not in freq_to_bandwidth_nr:
		# print(f)
		unknown_channel.add(int(f))
		return 0
	return freq_to_bandwidth_nr[int(f)]

def nr_bandwidth_vrz(f):
	if int(f) < 1e6:
		return 10
	return 100
	

def nr_bandwidth(f):
	if f == "None":
		return 0

	if op.startswith('a') or op.startswith('A'):
		return nr_bandwidth_att(f)

	if op.startswith('t') or op.startswith('T'):
		return nr_bandwidth_tmo(f)

	if op.startswith('v') or op.startswith('V'):
		return nr_bandwidth_vrz(f)


def get_bandwidth(df):
	df = df[(df.pcell_cid != 'None') & (df.pcell_freq != "None") & (df.first_gps != "None")]
	# df = df.astype({'measurement_time': 'float64'}, copy=False)
	print(len(df))

	for area, group in df.groupby(by=["region"]):
		print(area, len(group))

		cur_pid, cur_freq = None, None
		cur_grid = ""
		results = []
		for _, row in group.iterrows():
			pid,freq = int(row['pcell_cid']), int(row['pcell_freq'])
			grid = row['first_gps']

			if (freq,pid) != (cur_freq,cur_pid) or cur_grid != grid:
				# get the previous results
				if results:
					results.sort(reverse=True)
					print(results[0][0],results[0][1],results[0][2])
					del results[:]

			channel_width = (lte_bandwidth(row['pcell_freq']) + lte_bandwidth(row['scell1_freq'])) + lte_bandwidth(row['scell2_freq']) + lte_bandwidth(row['scell3_freq'])

			# 5G bandwidth
			channel_width += (nr_bandwidth(row['nrcell_freq']) + nr_bandwidth(row['nr_scell1_freq']) + nr_bandwidth(row['nr_scell2_freq']) + nr_bandwidth(row['nr_scell3_freq']))

			# optimal width
			# 2-675-769-174270-2-52740-2-52941-2-53139-None-None-None-None-None-None
			items = row['best_5g_cell_set'].split('-')
			optimal_width = lte_bandwidth(items[1]) + lte_bandwidth(items[5]) + lte_bandwidth(items[7]) + lte_bandwidth(items[9]) + nr_bandwidth(items[3]) + nr_bandwidth(items[11]) + nr_bandwidth(items[13]) + nr_bandwidth(items[15])

			results.append((channel_width,optimal_width,grid))

			cur_pid,cur_freq,cur_grid = pid,freq,grid

		results.sort(reverse=True)
		print(results[0][0],results[0][1],results[0][2])


if __name__ == '__main__':
	df = pd.read_csv(sys.argv[1])
	op = sys.argv[2]

	print(df.dtypes)

	get_meas_time(df)

	# get_signaling(df)

	# get_bandwidth(df)

	print(unknown_channel)
