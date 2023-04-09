import sys, os 
import pandas as pd 
import re
import random
import logging


cellcet_keys = ['pcell_freq', 'pcell_cid', 'nrcell_freq', 'nrcell_cid', 'scell1_freq', 'scell1_cid', 'scell2_freq', 'scell2_cid', 'scell3_freq', 'scell3_cid', 'nrscell1_freq', 'nrscell1_cid', 'nrscell2_freq', 'nrscell2_cid', 'nrscell3_freq', 'nrscell3_cid']


def get_grid_pcell_weighted_avg_dict(df):
	result_dict = {}
	for grid, group in df.groupby(by=['grid_lat','grid_lon']):
		result_dict[grid] = {}
		for cell, sub_group in group.groupby(by=['pcell_freq', 'pcell_cid']):
			weighted_avg_perf = sum(sub_group['thput_avg']*sub_group['thput_sample']) / sum(sub_group['thput_sample'])
			tmp = {
				'avg':weighted_avg_perf,  
				# 'avg_LTE+Sub6':0, 
				# 'avg_LTE':0,
				'avg_NO-NW': 0,
				'avg_LTE+MW':0,
				'best':max(sub_group['thput_avg']),
				# 'best_LTE+Sub6':0, 
				# 'best_LTE':0, 
				'best_NO-NW': 0,
				'best_LTE+MW':0, 
			}

			for tag, sub_sub_group in sub_group.groupby(by=['cell_set_type']):
				tmp['avg_'+tag] = sum(sub_sub_group['thput_avg']*sub_sub_group['thput_sample']) / sum(sub_sub_group['thput_sample'])
				tmp['best_'+tag] = max(sub_sub_group['thput_avg'])

			no_mw_rows = sub_group[sub_group.cell_set_type != 'LTE+MW']
			if len(no_mw_rows):
				tmp['avg_NO-NW'] = sum(no_mw_rows['thput_avg']*no_mw_rows['thput_sample']) / sum(no_mw_rows['thput_sample'])
				tmp['best_NO-NW'] = max(no_mw_rows['thput_avg'])

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
	print(perf_df.dtypes)
	print(len(perf_df))
	perf_df = perf_df[(perf_df.thput_sample > 3) & (perf_df.thput_avg > 1) & (perf_df.pcell_cid != 'None') & (perf_df.pcell_freq != 'None') & (perf_df.grid_lon != 'None')] # 
	print(len(perf_df))
	perf_df = perf_df.rename(columns={"grid_lat": "grid_lon", "grid_lon": "grid_lat"})
	#	Get weighted average throughput for each pcell
	grid_pcell_weighted_avg = get_grid_pcell_weighted_avg_dict(perf_df)

	# Step 2: Load per-grid cell rss
	df = pd.read_csv(sys.argv[2], index_col=False, dtype = {'grid_lat':'object','grid_lon':'object', 'freq':'object', 'cid':'object'})
	print(len(df))
	print(df.dtypes)
	df = df[(df['sample'] > 3) & (df['rsrq_25'] != 'None') & (df['rsrq_avg'] != 'None')]
	print(len(df))
	grid_cell_rss_dict = get_grid_cell_rss_dict(df)

	# Step 3: Load source cell distribution and config
	df = pd.read_csv(sys.argv[3], index_col=False, dtype = dtype_dict)
	print(df.dtypes)
	print(len(df))
	df = df[(df.src_pcell_cfg.notna()) & (df.avail_sample > 3)]
	print(len(df))
	grid_cellset_source_config_dict = get_grid_cellset_source_config(df)

	# if ('39.7795', '-86.1675') in grid_cellset_source_config_dict:
	# 	print('grid yes')
	# 	if ('850', '206', 'None', 'None', '52740', '71', 'None', 'None', 'None', 'None', 'None', 'None', 'None', 'None', 'None', 'None') in grid_cellset_source_config_dict[('39.7795', '-86.1675')]:
	# 		print('cell set yes')
	# print(grid_cellset_source_config_dict)

	# sys.exit(0)

	# Step 4: Emulation based on perf_df
	#	change a3
	results = []
	a2, a3_intra, a3_inter = -17, 3, 5

	a3_intra -= 2
	a3_inter -= 2

	# a2 -= 2

	for grid, group in perf_df.groupby(by=['grid_lat','grid_lon']):
		# break down each cell set based on its source PCell
		best_thput = {'LTE+Sub6':0, 'LTE':0, 'LTE+MW':0}
		for tag, grp in group.groupby(by=['cell_set_type']):
			best_thput[tag] = max(grp['thput_avg'])

		for _, row in group.iterrows():
			# first select LTE+MW if available
			if best_thput['LTE+MW'] > 0:
				new_tput = best_thput['LTE+MW']
			else:
				new_tput = max(best_thput['LTE+Sub6'], best_thput['LTE'])

			old_dest_freq, old_dest_cid = row['pcell_freq'], row['pcell_cid']
			old_dest_cell = (old_dest_freq, old_dest_cid)

			cellset = tuple([row[x] for x in cellcet_keys])

			if grid not in grid_cellset_source_config_dict or cellset not in grid_cellset_source_config_dict[grid]:
				logging.warning(f"No breakdown! {grid} {cellset}")
				not_found_count += 1
				results.append([grid[0],grid[1],row['thput_avg'],new_tput])
				no_change_count += 1
				continue

			cfg_lst = grid_cellset_source_config_dict[grid][cellset]
			legacy_perf, capp_perf, src_cell_cnt = 0, 0, len(cfg_lst)
			for src in cfg_lst:
				src_freq, src_cid = src['src_pcell_freq'], src['src_pcell_cid']
				src_cell = (src_freq, src_cid)
				
				# src rsrq
				try:
					src_rsrq = float(grid_cell_rss_dict[grid][src_cell]['rsrq_25'])
				except:
					logging.warning("unknown rss cell " + str(grid) + " " + str(src_cell))
					src_rsrq = -17 - 0.5 if src['a3_inter'] else -17 + 0.5

				if src['a3_inter']:
					src_rsrq = min(src_rsrq, -17 - 0.5)

				inter_on = src_rsrq < a2

				# find all eligible candidates
				candidate_per_freq_perf = {} # freq: [cid, rsrq, perf_avg, perf_mw]
				# first ad the original destination
				delta = 3 if old_dest_freq == src_cell[0] else 5
				try:
					org_dest_rsrq = max(grid_cell_rss_dict[grid][old_dest_cell]['rsrq_75'], src_rsrq + delta + 0.5)
				except:
					print('UNKNOWN RSS', grid, old_dest_cell)
					org_dest_rsrq = src_rsrq + delta + 0.5

				# find optimized perf of the old dest cell
				delta = a3_intra if old_dest_freq == src_cell[0] else a3_inter
				if org_dest_rsrq + delta > src_rsrq:
					candidate_per_freq_perf[old_dest_freq] = [(
						old_dest_cid, 
						org_dest_rsrq, 
						row['thput_avg'], 
						grid_pcell_weighted_avg[grid][old_dest_cell]['best_LTE+MW'],
						grid_pcell_weighted_avg[grid][old_dest_cell]['best_NO-NW'],
						True)]

				# find all candidates based on a2/a3 criteria
				for candidate, perf in grid_pcell_weighted_avg[grid].items():
					freq, cid = candidate

					if candidate == old_dest_cell:
						continue

					legacy_cand = (inter_on or freq == src_cell[0])
					# if freq != src_cell[0] and not inter_on:
					# 	continue

					delta = 3 if freq == src_cell[0] else 5
					try:
						rsrq = grid_cell_rss_dict[grid][candidate]['rsrq_25']
					except:
						print('UNKNOWN RSS', grid, candidate)
						rsrq = src_rsrq + delta + 0.5
						# sys.exit(-1)
					rsrq = max(rsrq, src_rsrq + delta + 0.5)

					delta = a3_intra if freq == src_cell[0] else a3_inter
					if rsrq > src_rsrq + delta:
						if freq not in candidate_per_freq_perf:
							candidate_per_freq_perf[freq] = []
						candidate_per_freq_perf[freq].append((
							cid, 
							rsrq, 
							perf['avg'], 
							perf['best_LTE+MW'], 
							perf['best_NO-NW'], 
							legacy_cand))

				if len(candidate_per_freq_perf) == 0:
					src_cell_cnt -= 1
					continue

				# new cell by legacy
				# use random to show the order
				keys = [x for x in candidate_per_freq_perf if candidate_per_freq_perf[x][-1]]
				key = random.choice(keys)
				items = candidate_per_freq_perf[key]
				if key == old_dest_freq:
					legacy_perf += [x[2] for x in items if x[0] == old_dest_cid][0]
				else:
					items.sort(key=lambda x:x[1],reverse=True)
					legacy_perf += items[0][2]

				# new cell by ca++
				opt_perf_nw, opt_perf_no_nw = 0, 0
				for _, cell_lst in candidate_per_freq_perf.items():
					opt_perf_nw = max(opt_perf_nw, max([x[3] for x in cell_lst]))
					opt_perf_no_nw = max(opt_perf_no_nw, max([x[4] for x in cell_lst]))
				capp_perf += opt_perf_nw if opt_perf_nw > 0 else opt_perf_no_nw

			if src_cell_cnt == 0:
				results.append([grid[0],grid[1],row['thput_avg'],new_tput])
				no_change_count += 1
			else:
				results.append([grid[0],grid[1],legacy_perf/src_cell_cnt, capp_perf/src_cell_cnt])

	new_df = pd.DataFrame(results, columns=['grid_lat','grid_lon',
		'legacy_c','capp_c'])
	print(len(new_df))
	print('not_found_count', not_found_count)
	print('no_change_count', no_change_count)

	new_df.to_csv(sys.argv[4], index=False)


