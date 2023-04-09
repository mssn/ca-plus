import sys, os 
import pandas as pd 
import re
import random
import logging


cellcet_keys = ['pcell_freq', 'pcell_cid', 'nrcell_freq', 'nrcell_cid', 'scell1_freq', 'scell1_cid', 'scell2_freq', 'scell2_cid', 'scell3_freq', 'scell3_cid', 'nrscell1_freq', 'nrscell1_cid', 'nrscell2_freq', 'nrscell2_cid', 'nrscell3_freq', 'nrscell3_cid']

a3_a5_dict = {}
a2_dict = {}

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

			mw_rows = sub_group[sub_group.cell_set_type == 'LTE+MW']
			if len(mw_rows) > 0:
				tmp['avg_LTE+MW'] = sum(mw_rows['thput_avg'] * mw_rows['thput_sample']) / sum(mw_rows['thput_sample'])
				tmp['best_LTE+MW'] = max(mw_rows['thput_avg'])

			no_mw_rows = sub_group[sub_group.cell_set_type != 'LTE+MW']
			if len(no_mw_rows) > 0:
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
				'rsrp_25':row['rsrp_25'],
				'rsrp_50':row['rsrp_50'],
				'rsrp_75':row['rsrp_75'],
				# 'rsrp_90':row['rsrp_90'],
				# 'rsrp_100':row['rsrp_100'],
				# 'rsrq_avg':float(row['rsrq_avg']),
				# 'rsrq_std':row['rsrq_std'],
				# 'rsrq_0':row['rsrq_0'],
				# 'rsrq_10':row['rsrq_10'],
				'rsrq_25':row['rsrq_25'],
				'rsrq_50':row['rsrq_50'],
				'rsrq_75':row['rsrq_75'],
				# 'rsrq_90':row['rsrq_90'],
				# 'rsrq_100':row['rsrq_100'],
			}
	return result_dict


def get_grid_cellset_source_cell(df):
	print(len(df))
	df = df[df.src_pcell_cfg.notna()]
	print(len(df))
	result_dict = {}
	for grid, grp in df.groupby(by=['grid_lat', 'grid_lon']):
		result_dict[grid] = {}
		for cellset, sub_grp in grp.groupby(by=cellcet_keys):
			result_dict[grid][cellset] = set([])
			for _, row in sub_grp.iterrows():
				result_dict[grid][cellset].add((row['src_pcell_freq'], row['src_pcell_cid']))
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


def get_general_config(df):
	for _, row in df.iterrows():
		pcell_freq = row['src_pcell_freq']
		pcell_cid = row['src_pcell_cid']

		try:
			cfg_lst = re.findall(r'\[(.*?)\]', row['src_pcell_cfg']) # src_pcell_cfg dst_pcell_cfg
		except:
			continue

		for cfg in cfg_lst:
			freq, event, th1, th2 = cfg.split('/')

			if event == 'a2' and pcell_freq == freq:
				a2_dict[(pcell_freq, pcell_cid)] = float(th1)

			elif event in ['a3', 'a4', 'a5']:
				if event == 'a5':
					thresh = float(th2)
				else:
					thresh = float(th1)
				a3_a5_dict[(pcell_freq, pcell_cid, freq)] = (event, thresh)

		pcell_freq = row['pcell_freq']
		pcell_cid = row['pcell_cid']

		try:
			cfg_lst = re.findall(r'\[(.*?)\]', row['dst_pcell_cfg']) # src_pcell_cfg dst_pcell_cfg
		except:
			continue

		for cfg in cfg_lst:
			freq, event, th1, th2 = cfg.split('/')

			if event == 'a2' and pcell_freq == freq:
				a2_dict[(pcell_freq, pcell_cid)] = float(th1)

			elif event in ['a3', 'a4', 'a5']:
				if event == 'a5':
					thresh = float(th2)
				else:
					thresh = float(th1)
				a3_a5_dict[(pcell_freq, pcell_cid, freq)] = (event, thresh)


not_found_count = 0
no_change_count = 0

no_a2_cell = set([])
no_a3_a5_cell = set([])

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

	# Step 2: Load per-grid cell rss
	df = pd.read_csv(sys.argv[2], index_col=False, dtype = {'grid_lat':'object','grid_lon':'object', 'freq':'object', 'cid':'object'})
	print(len(df))
	df = df[(df['sample'] > 3) & ((df['rsrq_50'] != 'None') | (df['rsrp_50'] != 'None'))]
	print(len(df))
	grid_cell_rss_dict = get_grid_cell_rss_dict(df)

	# Step 3: Load source cell distribution and config
	df = pd.read_csv(sys.argv[3], index_col=False, dtype = dtype_dict)
	# print(df.dtypes)
	print(len(df))
	df = df[(df.src_pcell_freq != 'None') & (df.pcell_freq != 'None') & (df.avail_sample > 3)]
	print(len(df))
	grid_cellset_source_cell_dict = get_grid_cellset_source_cell(df)
	get_general_config(df)

	for x,y in sorted(a2_dict.items()):
		print(x,y)

	for x,y in a3_a5_dict.items():
		print(x,y)

	sys.exit(0)

	# print(grid_cellset_source_config_dict)

	# sys.exit(0)

	results = []

	a3_delta = 0
	a2_delta = -10
	a5_delta = 0

	tmp_freqs = set([])

	for grid, group in perf_df.groupby(by=['grid_lat','grid_lon']):
		# break down each cell set based on its source PCell
		best_thput = {'LTE+Sub6':0, 'LTE':0, 'LTE+MW':0}
		for tag, grp in group.groupby(by=['cell_set_type']):
			best_thput[tag] = max(grp['thput_avg'])

		if best_thput['LTE+MW'] > 0:
			new_tput = best_thput['LTE+MW']
		else:
			new_tput = max(best_thput['LTE+Sub6'], best_thput['LTE'])

		for _, row in group.iterrows():

			old_dest_freq, old_dest_cid = row['pcell_freq'], row['pcell_cid']
			old_dest_cell = (old_dest_freq, old_dest_cid)

			cellset = tuple([row[x] for x in cellcet_keys])

			if grid not in grid_cellset_source_cell_dict or cellset not in grid_cellset_source_cell_dict[grid]:
				logging.warning(f"No breakdown! {grid} {cellset}")
				not_found_count += 1
				# results.append([grid[0],grid[1],row['thput_avg'],new_tput])
				continue

			cfg_lst = grid_cellset_source_cell_dict[grid][cellset]

			legacy_perf, capp_perf, src_cell_cnt = 0, 0, len(cfg_lst)
			for src_cell in cfg_lst:
				src_freq, src_cid = src_cell

				src_rsrp, src_rsrq = -120, -18
				try:
					src_rsrp = float(grid_cell_rss_dict[grid][src_cell]['rsrp_50'])
				except:
					continue

				try:
					src_rsrq = float(grid_cell_rss_dict[grid][src_cell]['rsrq_50'])
				except:
					continue

				# fix a2 based on if inter is on

				a2 = a2_dict.get(src_cell, None)
				if a2 is None:
					no_a2_cell.add(src_cell)
					src_cell_cnt -= 1
					continue # ignore src cells whose rss unavailable
				
				# TODO: whether inter is on
				# if src['inter_on'] and src_rsrp >= a2:
				# 	src_rsrp = a2 - random.randint(1,3)

				a2_update = a2 + a2_delta
				inter_on = src_rsrp < a2_update

				# find all eligible candidates
				candidate_per_freq_perf = {} 
				
				for candidate, perf in grid_pcell_weighted_avg[grid].items():
					freq, cid = candidate

					avg_perf = row['thput_avg'] if candidate == old_dest_cell else perf['avg']

					add_cell = False

					legacy_cand = inter_on or freq == src_freq

					target_cfg = a3_a5_dict.get((src_freq, src_cid, freq), None)

					# simplify the logic: if config or rss unavailable, add it
					if target_cfg is None:
						no_a3_a5_cell.add((src_freq, src_cid, freq))
						# print(freq)
						continue
						# add_cell = True

					if candidate not in grid_cell_rss_dict[grid]:
						continue

					elif target_cfg[0] == 'a3': # use rsrq
						try:
							candidate_rsrq = float(grid_cell_rss_dict[grid][candidate]['rsrq_50'])
							a3 = float(target_cfg[1]) + a3_delta
							add_cell = (candidate_rsrq > src_rsrq + a3)
						except:
							add_cell = True

					elif target_cfg[0] == 'a5': # use rsrp
						try:
							candidate_rsrp = float(grid_cell_rss_dict[grid][candidate]['rsrp_50'])
							a5 = float(target_cfg[1]) + a5_delta
							add_cell = (candidate_rsrp > a5)
						except:
							add_cell = True

					if add_cell:
						if freq not in candidate_per_freq_perf:
							candidate_per_freq_perf[freq] = []
						candidate_per_freq_perf[freq].append((
							cid, 
							avg_perf, 
							perf['best_LTE+MW'], 
							perf['best_NO-NW'], 
							legacy_cand)
						)


				if len(candidate_per_freq_perf) == 0:
					src_cell_cnt -= 1
					continue

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
				opt_perf_nw, opt_perf_no_nw = 0, 0
				for _, cell_lst in candidate_per_freq_perf.items():
					opt_perf_nw = max(opt_perf_nw, max([x[2] for x in cell_lst]))
					opt_perf_no_nw = max(opt_perf_no_nw, max([x[3] for x in cell_lst]))
				capp_perf += opt_perf_nw if opt_perf_nw > 0 else opt_perf_no_nw

			# if src_cell_cnt == 0 or legacy_perf == 0:
			# 	results.append([grid[0],grid[1],row['thput_avg'],new_tput])
			# else:
			# 	results.append([grid[0],grid[1],legacy_perf/src_cell_cnt, capp_perf/src_cell_cnt])
			
			if src_cell_cnt > 0:
				results.append([grid[0],grid[1],legacy_perf/src_cell_cnt, capp_perf/src_cell_cnt])
			else:
				pass
				# results.append([grid[0],grid[1],row['thput_avg'],new_tput])

	new_df = pd.DataFrame(results, columns=['grid_lat','grid_lon',
		'legacy','ca++'])
	print(len(new_df))
	print('not_found_count', not_found_count)

	print(no_a2_cell)
	print(no_a3_a5_cell)

	new_df.to_csv(sys.argv[4], index=False)
