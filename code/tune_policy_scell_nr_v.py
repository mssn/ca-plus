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
		# tmp = {}
		for _, row in grp.iterrows():
			result_dict[grid][(row['freq'], row['cid'])] = {
				# 'rsrp_avg':row['rsrp_avg'],
				# 'rsrp_std':row['rsrp_std'],
				# 'rsrp_0':row['rsrp_0'],
				# 'rsrp_10':row['rsrp_10'],
				'rsrp_25':row['rsrp_25'],
				'rsrp_50':row['rsrp_50'],
				'rsrp_75':row['rsrp_75'],
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


def helper_dump_scell_config(df):
	freq_dump_dict = {}
	for _, row in df.iterrows():
		pcell_freq = row['pcell_freq']
		try:
			cfg_lst = re.findall(r'\[(.*?)\]', row['dst_pcell_cfg']) # src_pcell_cfg dst_pcell_cfg
		except:
			continue

		for cfg in cfg_lst:
			freq, event, th1, th2 = cfg.split('/')

			# collect lte scell threahold
			if freq != pcell_freq and event in ['a1', 'a2']:
				if freq not in freq_dump_dict:
					freq_dump_dict[freq] = set([])
				freq_dump_dict[freq].add(th1)

			# collect nr scell threshold
			if int(freq) > 1000000:
				if freq not in freq_dump_dict:
					freq_dump_dict[freq] = set([])
				freq_dump_dict[freq].add((event, th1))
	return freq_dump_dict


def get_grid_nr_scell_config_dict(df):
	result_dict = {}
	for grid, grp in df.groupby(by=['grid_lat', 'grid_lon']):
		if grid not in result_dict:
			result_dict[grid] = {}
		for _, row in grp.iterrows():
			pcell = (row['pcell_freq'], row['pcell_cid'])
			try:
				cfg_lst = re.findall(r'\[(.*?)\]', row['dst_pcell_cfg']) # src_pcell_cfg dst_pcell_cfg
			except:
				continue

			for cfg in cfg_lst:
				freq, event, th1, th2 = cfg.split('/')

				# collect nr scell threshold
				if int(freq) > 1000000:
					if pcell not in result_dict[grid]:
						result_dict[grid][pcell] = {}
					result_dict[grid][pcell][freq] = float(th1)

	return result_dict


# def get_grid_scell_config(df):
# 	result_dict = {}
# 	for grid, grp in df.groupby(by=['grid_lat', 'grid_lon']):
# 		result_dict[grid] = {}
# 		for cellset, sub_grp in grp.groupby(by=cellcet_keys):
# 			result_dict[grid][cellset] = []
# 			for _, row in sub_grp.iterrows():
# 				item = {'src_pcell_cid': row['src_pcell_cid'], 
# 					'src_pcell_freq': row['src_pcell_freq'],
# 					'a3_inter': False}
# 					# 'a2':-17,
# 					# 'a3_intra':3,
# 					# 'a3_inter':5,
# 					# 'a5':[]}
# 				try:
# 					cfg_str = re.findall(r'\[(.*?)\]', row['src_pcell_cfg'])
# 				except:
# 					print(row['src_pcell_cfg'])
# 					sys.exit(-1)

# 				for cfg in cfg_str:
# 					#'66661/a2/-19.5/'
# 					freq, event, th1, th2 = cfg.split('/')
# 					# freq = int(freq)
# 					# th1 = float(th1)

# 					# # get a2
# 					# if 'a2' == event and freq == row['src_pcell_freq']:
# 					# 	item['a2'] = th1 if item['a2'] is None else max(item['a2'], th1)

# 					# # get a3_intra
# 					# if 'a3' == event and freq == row['src_pcell_freq']:
# 					# 	item['a3_intra'] = th1

# 					# get a3_inter
# 					if 'a3' == event and freq != row['src_pcell_freq']:
# 						item['a3_inter'] = True

# 					# if 'a5' == event:
# 					# 	item['a5'].append(freq) # assume a5 candidates eligible


# 				result_dict[grid][cellset].append(item)
# 	return result_dict

not_found_count = 0
na_count = 0

b1_nr_delta = 10

grid_cell_rss_dict = {}
grid_nr_scell_config_dict = {}

false_cnt = 0

def is_row_maintain_no_grid(row):
	global false_cnt
	grid = (row['grid_lat'], row['grid_lon'])
	pcell = (row['pcell_freq'], row['pcell_cid'])
	all_nr_cell_good = True
	for x, y in [('nrcell_freq', 'nrcell_cid'), 
		('nrscell1_freq', 'nrscell1_cid'), 
		('nrscell2_freq', 'nrscell2_cid'), 
		('nrscell3_freq', 'nrscell3_cid')]:
		x, y = row[x], row[y]
		thresh = 0
		try:
			thresh = grid_nr_scell_config_dict[grid][pcell][x]
		except:
			continue

		nr_rsrp = thresh + 0.5
		try:
			# nr_rsrp = max(float(grid_cell_rss_dict[grid][(x,y)]['rsrp_50']), nr_rsrp) #v1
			nr_rsrp = max(float(grid_cell_rss_dict[grid][(x,y)]['rsrp_50']), nr_rsrp) #v2
			# print(grid, x, y, float(grid_cell_rss_dict[grid][(x,y)]['rsrp_50']), thresh)
		except:
			# print(grid, x, y)
			pass

		if nr_rsrp <= thresh + b1_nr_delta:
			all_nr_cell_good = False
			# print(grid, x, y, nr_rsrp, thresh)
			false_cnt += 1
			break

	return all_nr_cell_good


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
	# grid_pcell_weighted_avg = get_grid_cellset_perf_dict(perf_df)

	# Step 2: Load per-grid cell rss
	df = pd.read_csv(sys.argv[2], index_col=False, dtype = {'grid_lat':'object','grid_lon':'object', 'freq':'object', 'cid':'object'})
	print(len(df))
	# print(df.dtypes)
	df = df[(df['rat'] == 'mw') & (df['sample'] >= 1) & (df['rsrp_25'] != 'None') & (df['rsrp_50'] != 'None')]
	print(len(df))
	grid_cell_rss_dict = get_grid_cell_rss_dict(df)

	# Step 3: Load nr scell config; lte scell has unified config: a2, -18.5
	df = pd.read_csv(sys.argv[3], index_col=False, dtype = dtype_dict)
	# print(df.dtypes)
	print(len(df))
	df = df[(df.src_pcell_cfg.notna()) & (df.avail_sample > 3)]
	print(len(df))
	grid_nr_scell_config_dict = get_grid_nr_scell_config_dict(df)

	# dump_res = helper_dump_scell_config(df)
	# for x in dump_res:
	# 	print(x, dump_res[x])
	

	# Step 4: Emulation based on perf_df
	#	change a3
	results = []

	perf_df['is_maintain'] = perf_df.apply(lambda row: is_row_maintain_no_grid(row), axis = 1)
	print(perf_df['is_maintain'].value_counts())

	# perf_df = perf_df[perf_df['is_maintain'] == True]
	
	for grid, group in perf_df.groupby(by=['grid_lat','grid_lon']):

		filter_group = group[group.is_maintain == True]

		best_thput = {'LTE+Sub6':0, 'LTE':0, 'LTE+MW':0}
		for tag, grp in filter_group.groupby(by=['cell_set_type']):
			best_thput[tag] = max(grp['thput_avg'])
		if best_thput['LTE+MW'] > 0:
			new_tput = best_thput['LTE+MW']
		else:
			new_tput = max(best_thput['LTE+Sub6'], best_thput['LTE'])

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
			# legacy_perf = row['thput_avg']
			# opt_perf = pcell_perf_dict[pcell]['best_mw'] if pcell_perf_dict[pcell]['best_mw'] > 0 else pcell_perf_dict[pcell]['best_no_mw']
			opt_perf = new_tput

			results.append([grid[0], grid[1], legacy_perf, opt_perf])

	new_df = pd.DataFrame(results, columns=['grid_lat','grid_lon',
		'legacy','ca++'])
	print(len(new_df))
	print('na_count', na_count)
	print('false_cnt', false_cnt)
	# print('no_change_count', no_change_count)

	new_df.to_csv(sys.argv[4], index=False)
