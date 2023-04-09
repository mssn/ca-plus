import sys, os 
import pandas as pd 
import re
import random
import logging


cellcet_keys = ['pcell_freq', 'pcell_cid', 'nrcell_freq', 'nrcell_cid', 'scell1_freq', 'scell1_cid', 'scell2_freq', 'scell2_cid', 'scell3_freq', 'scell3_cid', 'nrscell1_freq', 'nrscell1_cid', 'nrscell2_freq', 'nrscell2_cid', 'nrscell3_freq', 'nrscell3_cid']

def map_tag_to_index(tag):
	index = -1
	try:
		index = cellcet_keys.index(tag)
	except:
		pass
	return index

def get_grid_cellset_perf_dict(df):
	result_dict = {}
	for grid, group in df.groupby(by=['grid_lat','grid_lon']):
		result_dict[grid] = {}
		for _, row in group.iterrows():
			cellset = tuple([row[x] for x in cellcet_keys])
			if cellset not in result_dict[grid]:
				result_dict[grid][cellset] = {'tput': 0, 'count': 0}
			new_cnt = result_dict[grid][cellset]['count'] + row['thput_sample']
			new_tput = (result_dict[grid][cellset]['count'] * result_dict[grid][cellset]['tput'] + row['thput_avg'] * row['thput_sample']) / new_cnt			
			result_dict[grid][cellset]['tput'] = new_tput
			result_dict[grid][cellset]['count'] = new_cnt
	return result_dict


def get_grid_cell_rss_dict(df):
	result_dict = {}
	for grid, grp in df.groupby(by=['grid_lat', 'grid_lon']):
		result_dict[grid] = {} 

		for _, row in grp.iterrows():
			result_dict[grid][(row['freq'], row['cid'])] = {
				# 'rsrp_avg':row['rsrp_avg'],
				# 'rsrp_std':row['rsrp_std'],
				# 'rsrp_0':row['rsrp_0'],
				# 'rsrp_10':row['rsrp_10'],
				'rsrp_25':float(row['rsrp_25']),
				'rsrp_50':float(row['rsrp_50']),
				'rsrp_75':float(row['rsrp_75']),
				# 'rsrp_90':row['rsrp_90'],
				# 'rsrp_100':row['rsrp_100'],
				# 'rsrq_avg':float(row['rsrq_avg']),
				# 'rsrq_std':row['rsrq_std'],
				# 'rsrq_0':row['rsrq_0'],
				# 'rsrq_10':row['rsrq_10'],
				# 'rsrq_25':float(row['rsrq_25']),
				# 'rsrq_50':float(row['rsrq_50']),
				# 'rsrq_75':float(row['rsrq_75']),
				# 'rsrq_90':row['rsrq_90'],
				# 'rsrq_100':row['rsrq_100'],
			}
	return result_dict

def lte_bandwidth(f):
	if f == "None":
		return 0

	freq_to_bandwidth = {677:15, 1123:15, 1200:10, 1275:15, 1475:5, 5035:5, 39874:20, 40072:20, 66736:10, 66811:15, 67011:5}

	if int(f) not in freq_to_bandwidth:
		unknown_channel.add(int(f))
		return 0
	return freq_to_bandwidth[int(f)]

def nr_bandwidth(f):
	if f == "None":
		return 0

	freq_to_bandwidth_nr = {125290:20, 125330:15, 519630:100, 521550:80}
	if int(f) not in freq_to_bandwidth_nr:
		# print(f)
		unknown_channel.add(int(f))
		return 0
	return freq_to_bandwidth_nr[int(f)]


not_found_count = 0
na_count = 0

lte_delta = 30

grid_cell_rss_dict = {}
grid_nr_scell_config_dict = {}

false_cnt = 0

def is_row_maintain_no_grid(row):
	global false_cnt, lte_delta
	grid = (row['grid_lat'], row['grid_lon'])
	pcell = (row['pcell_freq'], row['pcell_cid'])
	all_lte_cell_good = True
	for x, y in [('scell1_freq', 'scell1_cid'), 
		('scell2_freq', 'scell2_cid'), 
		('scell3_freq', 'scell3_cid')]:
		x, y = row[x], row[y]
		if x == 'None':
			continue

		thresh = -119

		lte_rsrp = -140
		try:
			lte_rsrp = float(grid_cell_rss_dict[grid][(x,y)]['rsrp_50'])
		except:
			print(grid,x,y)
			pass

		if lte_rsrp <= thresh:
			lte_rsrp = thresh + random.randint(1,10) - 0.5

		if lte_rsrp <= thresh + lte_delta:
			all_lte_cell_good = False
			false_cnt += 1
			# print(lte_rsrp, thresh + lte_delta)
			# break
		else:
			print(lte_rsrp)

	return all_lte_cell_good


if __name__ == '__main__':
	# Step 1: Load per-grid cellset throughput
	dtype_dict = {'grid_lat':'object','grid_lon':'object'}
	dtype_dict.update({x:'object' for x in cellcet_keys})
	perf_df = pd.read_csv(sys.argv[1], index_col=False, dtype = dtype_dict)
	print(len(perf_df))
	perf_df = perf_df[(perf_df.thput_sample > 3) & (perf_df.thput_avg > 1) & (perf_df.pcell_cid != 'None') & (perf_df.pcell_freq != 'None') & (perf_df.grid_lon != 'None')] # 
	print(len(perf_df))
	perf_df = perf_df.rename(columns={"grid_lat": "grid_lon", "grid_lon": "grid_lat"})

	# Step 2: Load per-grid cell rss
	df = pd.read_csv(sys.argv[2], index_col=False, dtype = {'grid_lat':'object','grid_lon':'object', 'freq':'object', 'cid':'object'})
	print(len(df))
	df = df[(df['rat'] == 'lte') & (df['rsrp_50'] != 'None') & (df['sample'] > 3)]
	print(len(df))
	grid_cell_rss_dict = get_grid_cell_rss_dict(df)

	# Step 4: Emulation based on perf_df
	results = []

	perf_df['is_maintain'] = perf_df.apply(lambda row: is_row_maintain_no_grid(row), axis = 1)
	print(perf_df['is_maintain'].value_counts())

	# perf_df = perf_df[perf_df['is_maintain'] == True]
	
	for grid, group in perf_df.groupby(by=['grid_lat','grid_lon']):

		filter_group = group[group.is_maintain == True]

		bw, new_tput = 0, 0
		for _, row in filter_group.iterrows():

			lte_width = (lte_bandwidth(row['pcell_freq']) + lte_bandwidth(row['scell1_freq'])) + lte_bandwidth(row['scell2_freq']) + lte_bandwidth(row['scell3_freq'])

			nr_width = (nr_bandwidth(row['nrcell_freq']) + nr_bandwidth(row['nrscell1_freq']) + nr_bandwidth(row['nrscell2_freq']) + nr_bandwidth(row['nrscell3_freq']))

			if (lte_width+nr_width, row['thput_avg']) > (bw, new_tput):
				bw, new_tput = lte_width+nr_width, row['thput_avg']


		# apply filter, and find avg and opt for each pcell
		pcell_perf_dict = {}
		for pcell, sub_group in filter_group.groupby(by=['pcell_freq', 'pcell_cid']):
			pcell_perf_dict[pcell] = sum(sub_group['thput_avg'] * sub_group['thput_sample']) / sum(sub_group['thput_sample'])

		for _, row in group.iterrows():
			pcell = (row['pcell_freq'], row['pcell_cid'])

			if pcell not in pcell_perf_dict:
				na_count += 1 #TODO: ignore such case as a tentative solution
				continue

			legacy_perf = row['thput_avg'] if row['is_maintain'] else pcell_perf_dict[pcell]

			results.append([grid[0], grid[1], legacy_perf, new_tput])

	new_df = pd.DataFrame(results, columns=['grid_lat','grid_lon',
		'legacy','ca++'])
	print(len(new_df))
	print('na_count', na_count)
	print('false_cnt', false_cnt)
	# print('no_change_count', no_change_count)

	new_df.to_csv(sys.argv[3], index=False)
