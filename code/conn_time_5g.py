#!/usr/bin/python

import pandas as pd
import sys, os
import ast 
import random

unknown_channel = set([])

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

def get_conn_time(df):
	print(len(df))
	df = df[df.measurement_time != 'None']
	df = df.astype({'measurement_time': 'float64'}, copy=False)
	print(len(df))

	for area, group in df.groupby(by=["region"]):
		print(area, len(group))

		# improved, no_impact = 0,0

		for _, row in group.iterrows():
			org_time, reported_5g_cell = float(row['measurement_time']), int(row['measured_5g_cell_num'])

			if reported_5g_cell == 0:
				continue

			reported_5g_cell_dict = ast.literal_eval(row['measured_5g_cell_list'])
			reported_5g_cell_list = sorted([(int(x.split('-')[1]),int(x.split('-')[0])) for x in reported_5g_cell_dict])

			org_cnt, eca_cnt = get_freq_to_meas(reported_5g_cell_list)
			eca_cnt = len(eca_cnt)
			print(org_time - (eca_cnt * 80 + random.gauss(8.2,2))/1000)


if __name__ == '__main__':
	df = pd.read_csv(sys.argv[1])

	print(df.dtypes)

	get_conn_time(df)

	print(unknown_channel)
