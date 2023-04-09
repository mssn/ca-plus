import sys, os 
import pandas as pd 
import re
import random
import logging


cellcet_keys = ['pcell_freq', 'pcell_cid', 'nrcell_freq', 'nrcell_cid', 'scell1_freq', 'scell1_cid', 'scell2_freq', 'scell2_cid', 'scell3_freq', 'scell3_cid', 'nrscell1_freq', 'nrscell1_cid', 'nrscell2_freq', 'nrscell2_cid', 'nrscell3_freq', 'nrscell3_cid']


def lte_bandwidth(f):
	if f == "None":
		return 0

	freq_to_bandwidth = {677:15, 925:5, 1000:20, 1050:10, 1075:10, 1100:10, 1123:10, 1300:10, 1550:10, 2050:20, 2100:20, 2350:10, 2561:10,5035:10, 5230:10, 66536:20, 66586:10, 66811:10, 66836:10, 66911:10, 67011:5, 67086:10}

	if int(f) > 40000 and int(f) < 60000:
		return 10
	elif int(f) in freq_to_bandwidth:
		return freq_to_bandwidth[int(f)]
	else:
		unknown_channel.add(int(f))
		return 0


def nr_bandwidth(f):
	if f == "None":
		return 0
	if int(f) < 1e6:
		return 10
	return 100

# def get_grid_pcell_weighted_avg_dict(df):
# 	result_dict = {}
# 	for grid, group in df.groupby(by=['grid_lat','grid_lon']):
# 		result_dict[grid] = {}
# 		for cell, sub_group in group.groupby(by=['pcell_freq', 'pcell_cid']):
# 			weighted_avg_perf = sum(sub_group['thput_avg']*sub_group['thput_sample']) / sum(sub_group['thput_sample'])
# 			tmp = {
# 				'avg':weighted_avg_perf,  
# 				# 'avg_LTE+Sub6':0, 
# 				# 'avg_LTE':0,
# 				'avg_NO-NW': 0,
# 				# 'avg_LTE+MW':0,
# 				'best':max(sub_group['thput_avg']),
# 				# 'best_LTE+Sub6':0, 
# 				# 'best_LTE':0, 
# 				'best_NO-NW': 0,
# 				# 'best_LTE+MW':0, 
# 			}

# 			# for tag, sub_sub_group in sub_group.groupby(by=['cell_set_type']):
# 			# 	tmp['avg_'+tag] = sum(sub_sub_group['thput_avg']*sub_sub_group['thput_sample']) / sum(sub_sub_group['thput_sample'])
# 			# 	tmp['best_'+tag] = max(sub_sub_group['thput_avg'])

# 			no_mw_rows = sub_group[sub_group.cell_set_type != 'LTE+MW']
# 			if len(no_mw_rows):
# 				tmp['avg_NO-NW'] = sum(no_mw_rows['thput_avg']*no_mw_rows['thput_sample']) / sum(no_mw_rows['thput_sample'])
# 				tmp['best_NO-NW'] = max(no_mw_rows['thput_avg'])

# 			result_dict[grid][cell] = tmp
# 	return result_dict


def get_grid_pcell_weighted_avg_dict(df):
	result_dict = {}
	for grid, group in df.groupby(by=['grid_lat','grid_lon']):
		result_dict[grid] = {}
		for cell, sub_group in group.groupby(by=['pcell_freq', 'pcell_cid']):
			weighted_avg_perf = sum(sub_group['thput_avg']*sub_group['thput_sample']) / sum(sub_group['thput_sample'])
			tmp = {
				'avg':weighted_avg_perf,
				'best':max(sub_group['thput_avg']), 
				'avg_NO-NW': 0,
				'best_NO-NW': 0,
			}

			no_mw_rows = sub_group[sub_group.cell_set_type != 'LTE+MW']
			if len(no_mw_rows) > 0:
				tmp['avg_NO-NW'] = sum(no_mw_rows['thput_avg']*no_mw_rows['thput_sample']) / sum(no_mw_rows['thput_sample'])

			wide_bw, wide_bw_tput = 0, 0
			for _, row in sub_group.iterrows():
				total_bw = lte_bandwidth(row['pcell_freq']) + lte_bandwidth(row['scell1_freq']) + lte_bandwidth(row['scell2_freq']) + lte_bandwidth(row['scell3_freq']) + nr_bandwidth(row['nrcell_freq']) + nr_bandwidth(row['nrscell1_freq']) + nr_bandwidth(row['nrscell2_freq']) + nr_bandwidth(row['nrscell3_freq'])
				if total_bw > wide_bw:
					wide_bw = total_bw
					wide_bw_tput = row['thput_avg']

			tmp['best_NO-NW'] = wide_bw_tput
			tmp['best_bw'] = wide_bw

			result_dict[grid][cell] = tmp

	return result_dict


def get_grid_cell_rss_dict(df):
	result_dict = {}
	for grid, grp in df.groupby(by=['grid_lat', 'grid_lon']):
		result_dict[grid] = {}
		# tmp = {}
		for _, row in grp.iterrows():
			result_dict[grid][(row['freq'], row['cid'])] = {
				# 'rsrp_avg':row['rsrp_avg'],
				# 'rsrp_std':row['rsrp_std'],
				# 'rsrp_0':row['rsrp_0'],
				# 'rsrp_10':row['rsrp_10'],
				# 'rsrp_25':row['rsrp_25'],
				# 'rsrp_50':row['rsrp_50'],
				# 'rsrp_75':row['rsrp_75'],
				# 'rsrp_90':row['rsrp_90'],
				# 'rsrp_100':row['rsrp_100'],
				'rsrq_avg':float(row['rsrq_avg']),
				# 'rsrq_std':row['rsrq_std'],
				# 'rsrq_0':row['rsrq_0'],
				# 'rsrq_10':row['rsrq_10'],
				'rsrq_25':float(row['rsrq_25']),
				'rsrq_50':float(row['rsrq_50']),
				'rsrq_75':float(row['rsrq_75']),
				# 'rsrq_90':row['rsrq_90'],
				# 'rsrq_100':row['rsrq_100'],
			}
	return result_dict


def get_grid_cellset_source_config(df):
	result_dict = {}
	for grid, grp in df.groupby(by=['grid_lat', 'grid_lon']):
		result_dict[grid] = {}
		for cellset, sub_grp in grp.groupby(by=cellcet_keys):
			result_dict[grid][cellset] = []
			for _, row in sub_grp.iterrows():
				item = {'src_pcell_cid': row['src_pcell_cid'], 
					'src_pcell_freq': row['src_pcell_freq'],
					'a3_inter': False}
					# 'a2':-17,
					# 'a3_intra':3,
					# 'a3_inter':5,
					# 'a5':[]}
				try:
					cfg_str = re.findall(r'\[(.*?)\]', row['src_pcell_cfg'])
				except:
					print(row['src_pcell_cfg'])
					sys.exit(-1)

				for cfg in cfg_str:
					#'66661/a2/-19.5/'
					freq, event, th1, th2 = cfg.split('/')
					# freq = int(freq)
					# th1 = float(th1)

					# # get a2
					# if 'a2' == event and freq == row['src_pcell_freq']:
					# 	item['a2'] = th1 if item['a2'] is None else max(item['a2'], th1)

					# # get a3_intra
					# if 'a3' == event and freq == row['src_pcell_freq']:
					# 	item['a3_intra'] = th1

					# get a3_inter
					if 'a3' == event and freq != row['src_pcell_freq']:
						item['a3_inter'] = True

					# if 'a5' == event:
					# 	item['a5'].append(freq) # assume a5 candidates eligible


				result_dict[grid][cellset].append(item)
	return result_dict

not_found_count = 0
no_change_count = 0

if __name__ == '__main__':
	# Step 1: Load per-grid cellset throughput
	dtype_dict = {'grid_lat':'object','grid_lon':'object'}
	dtype_dict.update({x:'object' for x in cellcet_keys})
	perf_df = pd.read_csv(sys.argv[1], index_col=False, dtype = dtype_dict)
	# print(perf_df.dtypes)
	print(len(perf_df))
	perf_df = perf_df[(perf_df.thput_sample > 3) & (perf_df.thput_avg > 1) & (perf_df.pcell_cid != 'None') & (perf_df.pcell_freq != 'None') & (perf_df.grid_lon != 'None')] # 
	print(len(perf_df))
	perf_df = perf_df.rename(columns={"grid_lat": "grid_lon", "grid_lon": "grid_lat"})
	#	Get weighted average throughput for each pcell
	grid_pcell_weighted_avg = get_grid_pcell_weighted_avg_dict(perf_df)

	# Step 2: Load per-grid cell rss
	df = pd.read_csv(sys.argv[2], index_col=False, dtype = {'grid_lat':'object','grid_lon':'object', 'freq':'object', 'cid':'object'})
	print(len(df))
	# print(df.dtypes)
	df = df[(df['sample'] > 3) & (df['rsrq_25'] != 'None') & (df['rsrq_avg'] != 'None')]
	print(len(df))
	grid_cell_rss_dict = get_grid_cell_rss_dict(df)

	# Step 3: Load source cell distribution and config
	df = pd.read_csv(sys.argv[3], index_col=False, dtype = dtype_dict)
	# print(df.dtypes)
	print(len(df))
	df = df[(df.src_pcell_cfg.notna()) & (df.avail_sample > 3)]
	print(len(df))
	# grid_cellset_source_config_dict = get_grid_cellset_source_config(df)

	# Step 4: Emulation based on perf_df
	# a2, a3_intra, a3_inter = -17, 3, 5

	# a3_intra -= 3
	# a3_inter -= 3

	# a2 -= 2

	results = []
	for grid, group in perf_df.groupby(by=['grid_lat','grid_lon']):

		best_thput = {'LTE+Sub6':0, 'LTE':0, 'LTE+MW':0}
		for tag, grp in group.groupby(by=['cell_set_type']):
			best_thput[tag] = max(grp['thput_avg'])

		if max(best_thput['LTE+Sub6'], best_thput['LTE']) == 0: # ignore locations with mw only observed
			continue

		# break down each cell set based on its source PCell
		for _, row in group.iterrows():
			pcell = (row['pcell_freq'], row['pcell_cid'])
			if row['cell_set_type'] != 'LTE+MW':
				legacy_perf = row['thput_avg']
			else:
				legacy_perf = grid_pcell_weighted_avg[grid][pcell]['avg_NO-NW'] 

			sorted_perf_by_bw = sorted(list(grid_pcell_weighted_avg[grid].values()), key=lambda x:x['best_bw'], reverse=True)
			new_tput = sorted_perf_by_bw[0]['best_NO-NW']

			results.append([grid[0],grid[1],legacy_perf, new_tput])

	new_df = pd.DataFrame(results, columns=['grid_lat','grid_lon',
		'legacy','capp'])
	print(len(new_df))
	# print('not_found_count', not_found_count)
	# print('no_change_count', no_change_count)

	new_df.to_csv(sys.argv[4], index=False)
