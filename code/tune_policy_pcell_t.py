import sys, os 
import pandas as pd 
import re
import random
import logging


cellcet_keys = ['pcell_freq', 'pcell_cid', 'nrcell_freq', 'nrcell_cid', 'scell1_freq', 'scell1_cid', 'scell2_freq', 'scell2_cid', 'scell3_freq', 'scell3_cid', 'nrscell1_freq', 'nrscell1_cid', 'nrscell2_freq', 'nrscell2_cid', 'nrscell3_freq', 'nrscell3_cid']

# config_a2_dict = {
# 	'677': -119,
# 	'66736': -119,
# 	'67011': -119,
# 	'66836': -116
# }

config_select_dict = {
	# '677': {'a3':[5], 'a5':[-109, -106, -112,]},
	'677': {'a5':[-109, -106, -112,]},
	'2250': {'a3':[5]},
	'5035': {'a3':[15]},
	'39874': {'a5': [-111]},
	'66736': {'a5':[-109]},
	'67011': {'a5': [-109]},
	'40072': {'a5':[-140]}
}

# 66736 {('66736', 'a5', '-110', '-109'), ('66736', 'a3', '5.0', ''), ('a2', '-119')}
# 677 {('677', 'a3', '5.0', ''), ('677', 'a5', '-107', '-106'), ('a2', '-119'), ('677', 'a5', '-110', '-109'), ('67011', 'a5', '-110', '-109')}
# 67011 {('67011', 'a5', '-110', '-109'), ('67011', 'a3', '5.0', ''), ('a2', '-119')}
# 39874 {('677', 'a5', '-43', '-140'), ('39874', 'a3', '3.0', ''), ('677', 'a5', '-112', '-112'), ('2250', 'a5', '-43', '-140'), ('5035', 'a3', '15.0', ''), ('67011', 'a3', '3.0', ''), ('2250', 'a3', '3.0', ''), ('68911', 'a5', '-121', '-115'), ('39874', 'a5', '-112', '-111'), ('67011', 'a5', '-43', '-140')}
# 40072 {('2250', 'a5', '-43', '-140'), ('677', 'a5', '-43', '-140'), ('67011', 'a5', '-43', '-140')}

unknown_channel = set([])


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


def get_grid_pcell_weighted_avg_dict(df):
	result_dict = {}
	for grid, group in df.groupby(by=['grid_lat','grid_lon']):
		result_dict[grid] = {}
		for cell, sub_group in group.groupby(by=['pcell_freq', 'pcell_cid']):
			weighted_avg_perf = sum(sub_group['thput_avg']*sub_group['thput_sample']) / sum(sub_group['thput_sample'])

			avg_lte = 0
			lte_rows = sub_group[sub_group.cell_set_type == 'LTE']
			if len(lte_rows) > 0:
				avg_lte = sum(lte_rows['thput_avg'] * lte_rows['thput_sample']) / sum(lte_rows['thput_sample'])

			widest, widest_lte, best, best_lte = 0, 0, 0, 0
			for _, row in sub_group.iterrows():
				lte_width = (lte_bandwidth(row['pcell_freq']) + lte_bandwidth(row['scell1_freq'])) + lte_bandwidth(row['scell2_freq']) + lte_bandwidth(row['scell3_freq'])
				nr_width = (nr_bandwidth(row['nrcell_freq']) + nr_bandwidth(row['nrscell1_freq']) + nr_bandwidth(row['nrscell2_freq']) + nr_bandwidth(row['nrscell3_freq']))

				if (lte_width+nr_width, row['thput_avg']) > (widest, best):
					widest, best = lte_width+nr_width, row['thput_avg']

				if nr_width == 0 and (lte_width, row['thput_avg']) > (widest_lte, best_lte):
					widest_lte, best_lte = lte_width, row['thput_avg']

			result_dict[grid][cell] = {
				'avg': weighted_avg_perf,
				'widest_bw': widest,
				'widest_perf': best,
				# 'avg_lte': avg_lte,
				# 'widest_bw_lte': widest_lte,
				# 'widest_perf_lte': best_lte,
			}

	return result_dict


# def get_grid_cellset_perf(df):
# 	result_dict = {}
# 	for grid, group in df.groupby(by=['grid_lat','grid_lon']):
# 		result_dict[grid] = {}
# 		for cellset, sub_group in group.groupby(by=cellcet_keys):
			

			

# 			result_dict[grid][cell] = {'perf':max(sub_group['thput_avg']), 'bandwidth': lte_width+nr_width, 'lte_bw':lte_width, 'nr_bw':nr_width}

# 	return result_dict


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
				# 'rsrq_25':row['rsrq_25'],
				# 'rsrq_50':row['rsrq_50'],
				# 'rsrq_75':row['rsrq_75'],
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
					'inter_on': False,
					'targets':{} }
				try:
					cfg_str = re.findall(r'\[(.*?)\]', row['src_pcell_cfg'])
				except:
					print(row['src_pcell_cfg'])
					sys.exit(-1)

				for cfg in cfg_str:
					#'66661/a2/-19.5/'
					freq, event, th1, th2 = cfg.split('/')

					# get a3_inter
					if event in ['a3', 'a4', 'a5']:
						if freq != row['src_pcell_freq']:
							item['inter_on'] = True

						item['targets'][freq] = (event, float(th1)) if event == 'a3' else (event, float(th2))

				result_dict[grid][cellset].append(item)

	return result_dict

not_found_count = 0
no_change_count = 0

gain, loss, same = 0, 0, 0

if __name__ == '__main__':
	# Step 1: Load per-grid cellset throughput
	dtype_dict = {'grid_lat':'object','grid_lon':'object'}
	dtype_dict.update({x:'object' for x in cellcet_keys})
	perf_df = pd.read_csv(sys.argv[1], index_col=False, dtype = dtype_dict)
	print(len(perf_df))
	perf_df = perf_df[(perf_df.thput_sample > 3) & (perf_df.thput_avg > 1) & (perf_df.pcell_cid != 'None') & (perf_df.pcell_freq != 'None') & (perf_df.grid_lon != 'None')] # 
	print(len(perf_df))
	perf_df = perf_df.rename(columns={"grid_lat": "grid_lon", "grid_lon": "grid_lat"})
	#	Get weighted average throughput for each pcell
	grid_pcell_weighted_avg = get_grid_pcell_weighted_avg_dict(perf_df)

	# grid_cellset_perf = get_grid_cellset_perf(df)

	# Step 2: Load per-grid cell rss
	df = pd.read_csv(sys.argv[2], index_col=False, dtype = {'grid_lat':'object','grid_lon':'object', 'freq':'object', 'cid':'object'})
	print(len(df))
	df = df[(df['sample'] > 3) & (df['rsrp_50'] != 'None')]
	print(len(df))
	grid_cell_rss_dict = get_grid_cell_rss_dict(df)
	print('grid_cell_rss_dict', len(grid_cell_rss_dict))

	# Step 3: Load source cell distribution and config
	df = pd.read_csv(sys.argv[3], index_col=False, dtype = dtype_dict)
	# print(df.dtypes)
	print(len(df))
	df = df[(df.src_pcell_cid != 'None') & (df.src_pcell_cfg.notna()) & (df.avail_sample > 3)]
	print(len(df))
	grid_cellset_source_config_dict = get_grid_cellset_source_config(df)
	print('grid_cellset_source_config_dict', len(grid_cellset_source_config_dict))

	# print(grid_cellset_source_config_dict)

	# sys.exit(0)

	a2 = -119 #rsrp

	results = []

	a3_delta = 0
	a2_delta = 0
	a5_delta = 30

	tmp_freqs = set([])

	found, not_found = 0, 0

	for grid, group in perf_df.groupby(by=['grid_lat','grid_lon']):
		# break down each cell set based on its source PCell
		# best_thput = {'LTE+Sub6':0, 'LTE':0, 'LTE+MW':0}
		# for tag, grp in group.groupby(by=['cell_set_type']):
		# 	best_thput[tag] = max(grp['thput_avg'])

		# if best_thput['LTE+MW'] > 0:
		# 	new_tput = best_thput['LTE+MW']
		# else:
		# 	new_tput = max(best_thput['LTE+Sub6'], best_thput['LTE'])

		bw, new_tput = 0, 0
		for _,row in group.iterrows():
			# pid,freq = int(row['pcell_cid']), int(row['pcell_freq'])

			lte_width = (lte_bandwidth(row['pcell_freq']) + lte_bandwidth(row['scell1_freq'])) + lte_bandwidth(row['scell2_freq']) + lte_bandwidth(row['scell3_freq'])

			nr_width = (nr_bandwidth(row['nrcell_freq']) + nr_bandwidth(row['nrscell1_freq']) + nr_bandwidth(row['nrscell2_freq']) + nr_bandwidth(row['nrscell3_freq']))

			if (lte_width+nr_width, row['thput_avg']) > (bw, new_tput):
				bw, new_tput = lte_width+nr_width, row['thput_avg']

		for _, row in group.iterrows():

			# first select LTE+MW if available

			old_dest_freq, old_dest_cid = row['pcell_freq'], row['pcell_cid']
			old_dest_cell = (old_dest_freq, old_dest_cid)

			cellset = tuple([row[x] for x in cellcet_keys])

			if grid not in grid_cellset_source_config_dict or cellset not in grid_cellset_source_config_dict[grid]:
				# logging.warning(f"No breakdown! {grid} {cellset}")
				not_found_count += 1
				results.append([grid[0],grid[1],row['thput_avg'],new_tput])
				continue

			cfg_lst = grid_cellset_source_config_dict[grid][cellset]
			legacy_perf, capp_perf, src_cell_cnt = 0, 0, len(cfg_lst)
			capp_lst = []
			for src in cfg_lst:
				src_freq, src_cid = src['src_pcell_freq'], src['src_pcell_cid']
				src_cell = (src_freq, src_cid)

				src_rsrp = -119
				try:
					src_rsrp = float(grid_cell_rss_dict[grid][src_cell]['rsrp_50'])
					# src_rsrq = float(grid_cell_rss_dict[grid][src_cell]['rsrq_50'])
				except:
					# print(grid, src_cell)
					src_cell_cnt -= 1
					continue # ignore src cells whose rss unavailable

				# print('here!')
				
				# src rsrp -> a2 based on rsrp
				if src['inter_on'] and src_rsrp >= a2:
					src_rsrp = a2 - random.randint(1,3)
					# src_rsrp = min(src_rsrp, a2 - random.randint(1,3))

				a2_update = a2 + a2_delta
				inter_on = src_rsrp < a2_update

				# find all eligible candidates
				candidate_per_freq_perf = {} 
				
				for candidate, perf in grid_pcell_weighted_avg[grid].items():
					freq, cid = candidate

					avg_perf = row['thput_avg'] if candidate == old_dest_cell else perf['avg']

					add_cell = False

					legacy_cand = inter_on or freq == src_freq

					target_cfg = None
					if freq in src['targets'] and src['targets'][freq][0] in ['a3','a5']:
						target_cfg = src['targets'][freq]
						found += 1
					elif freq in config_select_dict:
						options = config_select_dict[freq]
						event_type, event_thresh_lst = random.choice(list(options.items()))
						event_thresh = random.choice(event_thresh_lst)
						target_cfg = (event_type, event_thresh)
						# print(target_cfg)
						not_found += 1

					# simplify the logic: if config or rss unavailable, add it
					if target_cfg is None:
						unknown_channel.add(freq)
						continue 

					# get candidate rsrp
					a5, a3 = 0, 0
					try:
						a5 = float(target_cfg[1])
						a3 = float(target_cfg[1])
						# print(a5, a3)
					except:
						# print(target_cfg)
						pass

					standard_rsrp = a5 + 1 if target_cfg[0] == 'a5' else src_rsrp + a3 + 1

					candidate_rsrp = standard_rsrp

					try:
						candidate_rsrp = float(grid_cell_rss_dict[grid][candidate]['rsrp_50'])
						if candidate == old_dest_cell: 
							candidate_rsrp = max(candidate_rsrp, standard_rsrp)
					except:
						pass

					if (target_cfg[0] == 'a5' and candidate_rsrp > a5 + a5_delta) or (target_cfg[0] == 'a3' and candidate_rsrp > src_rsrp + a3 + a3_delta):
					# if add_cell:
						if freq not in candidate_per_freq_perf:
							candidate_per_freq_perf[freq] = []

						candidate_per_freq_perf[freq].append((
							cid, 
							avg_perf, 
							perf['widest_bw'], 
							perf['widest_perf'], 
							legacy_cand)
						)

					# else:
					# 	print(target_cfg, candidate_rsrp, a5+a5_delta, src_rsrp, a3+a3_delta)


				# print(len(grid_pcell_weighted_avg[grid]), len(candidate_per_freq_perf))


				if len(candidate_per_freq_perf) == 0:
					src_cell_cnt -= 1
					continue

				# print(candidate_per_freq_perf)

				# new cell by legacy
				# use random to show the order
				eligible_candidates = {}
				for key, lst in candidate_per_freq_perf.items():
					new_lst = []
					for x in lst:
						if x[-1]:
							new_lst.append(x)
					if new_lst:
						eligible_candidates[key] = new_lst

				if not eligible_candidates:
					src_cell_cnt -= 1
					continue

				key = random.choice(list(eligible_candidates.keys()))
				items = eligible_candidates[key]
				item = random.choice(items)

				selection_perf = item[1]
				if key == old_dest_freq:
					try:
						selection_perf = [x[1] for x in items if x[0] == old_dest_cid][0] 
					except:
						selection_perf = item[1]

				legacy_perf += selection_perf

				# new cell by ca++
				best_bw, best_bw_perf = 0, 0
				for _, cell_lst in candidate_per_freq_perf.items():
					for x in cell_lst:
						if (best_bw, best_bw_perf) < (x[2], x[3]):
							best_bw, best_bw_perf = x[2], x[3]

				capp_lst.append(best_bw_perf)
				capp_perf += best_bw_perf
				# if best_bw_perf == 0:
				# 	print(candidate_per_freq_perf)

			# if src_cell_cnt == 0 or legacy_perf == 0:
			# 	results.append([grid[0],grid[1],row['thput_avg'],new_tput])
			# else:
			# 	results.append([grid[0],grid[1],legacy_perf/src_cell_cnt, capp_perf/src_cell_cnt])
			# print(capp_lst, src_cell_cnt)
			
			if src_cell_cnt > 0:
				curr_perf = capp_perf/src_cell_cnt
				results.append([grid[0],grid[1],legacy_perf/src_cell_cnt, curr_perf])

				if curr_perf > new_tput:
					gain += 1
				elif curr_perf < new_tput:
					loss += 1
				else:
					same += 1
			else:
				pass
				# results.append([grid[0],grid[1],row['thput_avg'],new_tput])

	new_df = pd.DataFrame(results, columns=['grid_lat','grid_lon', 'legacy','ca++'])
	print(len(new_df))
	# print('not_found_count', not_found_count)

	# print('target', found, not_found)

	# print(tmp_freqs)

	# print(unknown_channel)
	print('gain',gain,'loss',loss, 'same', same)

	new_df.to_csv(sys.argv[4], index=False)

# a3+5: 971; gain 53 loss 122 same 549

# a3-5: 1673; gain 60 loss 178 same 1188

# a2-10: 1665; gain 83 loss 260 same 1075

# a5-10: 1666; gain 86 loss 267 same 1066

# a5+30: 1258; gain 132 loss 307 same 572
