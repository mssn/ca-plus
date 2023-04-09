import sys, os 
import pandas as pd 
import re
import random
import logging


cellcet_keys = ['pcell_freq', 'pcell_cid', 'nrcell_freq', 'nrcell_cid', 'scell1_freq', 'scell1_cid', 'scell2_freq', 'scell2_cid', 'scell3_freq', 'scell3_cid', 'nrscell1_freq', 'nrscell1_cid', 'nrscell2_freq', 'nrscell2_cid', 'nrscell3_freq', 'nrscell3_cid']


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
				'avg_no_nr': 0,
				'best_no_nr': 0,
			}

			no_mw_rows = sub_group[sub_group.cell_set_type == 'LTE']
			if len(no_mw_rows) > 0:
				tmp['avg_no_nr'] = sum(no_mw_rows['thput_avg']*no_mw_rows['thput_sample']) / sum(no_mw_rows['thput_sample'])

			wide_bw, wide_bw_tput = 0, 0
			for _, row in no_mw_rows.iterrows():
				total_bw = lte_bandwidth(row['pcell_freq']) + lte_bandwidth(row['scell1_freq']) + lte_bandwidth(row['scell2_freq']) + lte_bandwidth(row['scell3_freq'])
				if (total_bw, row['thput_avg']) > (wide_bw, wide_bw_tput):
					wide_bw = total_bw
					wide_bw_tput = row['thput_avg']

			tmp['best_no_nr'] = wide_bw_tput
			tmp['best_bw'] = wide_bw

			result_dict[grid][cell] = tmp

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


	results = []
	for grid, group in perf_df.groupby(by=['grid_lat','grid_lon']):

		best_thput = {'LTE+Sub6':0, 'LTE':0, 'LTE+MW':0}
		for tag, grp in group.groupby(by=['cell_set_type']):
			best_thput[tag] = max(grp['thput_avg'])

		if best_thput['LTE'] == 0: # ignore locations with mw only observed
			continue

		# break down each cell set based on its source PCell
		for _, row in group.iterrows():
			pcell = (row['pcell_freq'], row['pcell_cid'])
			if grid_pcell_weighted_avg[grid][pcell]['avg_no_nr'] == 0:
				continue
				
			if row['cell_set_type'] == 'LTE':
				legacy_perf = row['thput_avg']
			else:
				legacy_perf = grid_pcell_weighted_avg[grid][pcell]['avg_no_nr'] 

			sorted_perf_by_bw = sorted(list(grid_pcell_weighted_avg[grid].values()), key=lambda x:x['best_bw'], reverse=True)
			new_tput = sorted_perf_by_bw[0]['best_no_nr']

			results.append([grid[0],grid[1],legacy_perf, new_tput])


	new_df = pd.DataFrame(results, columns=['grid_lat', 'grid_lon', 'legacy', 'ca++'])
	print(len(new_df))
	# print('not_found_count', not_found_count)
	# print('no_change_count', no_change_count)

	new_df.to_csv(sys.argv[2], index=False)
