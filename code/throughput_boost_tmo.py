import sys, os 
import pandas as pd 

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


if __name__ == '__main__':
	df = pd.read_csv(sys.argv[1], index_col=False, dtype = {'grid_lat':'object','grid_lon':'object'}) # dtype={'pcell_cid':'object','pcell_freq':'object','nrcell_cid':'object',}, nrcell_freq,scell1_cid,scell1_freq,scell2_cid,scell2_freq,scell3_cid,scell3_freq,nrscell1_cid,nrscell1_freq,nrscell2_cid,nrscell2_freq,nrscell3_cid,nrscell3_freq
	print(len(df))
	df = df[(df.thput_sample > 3) & (df.thput_avg > 1) & (df.pcell_cid != 'None') & (df.pcell_freq != "None") & (df.grid_lon != 'None')] # 

	print(df.dtypes)
	print(len(df))

	results = []

	for grid, group in df.groupby(by=['grid_lat','grid_lon']): 
		best_thput = {'LTE+Sub6':0, 'LTE':0, 'LTE+MW':0}
		for tag, grp in group.groupby(by=['cell_set_type']):
			best_thput[tag] = max(grp['thput_avg'])
		# if best_thput:
		best_thput['all'] = max(list(best_thput.values()))

		widest_thput = {}

		for _,row in group.iterrows():
			# pid,freq = int(row['pcell_cid']), int(row['pcell_freq'])

			lte_width = (lte_bandwidth(row['pcell_freq']) + lte_bandwidth(row['scell1_freq'])) + lte_bandwidth(row['scell2_freq']) + lte_bandwidth(row['scell3_freq'])

			nr_width = (nr_bandwidth(row['nrcell_freq']) + nr_bandwidth(row['nrscell1_freq']) + nr_bandwidth(row['nrscell2_freq']) + nr_bandwidth(row['nrscell3_freq']))

			if  row['cell_set_type'] not in widest_thput or (lte_width+nr_width, row['thput_avg']) > tuple(widest_thput[row['cell_set_type']]):
				widest_thput[row['cell_set_type']] = [lte_width+nr_width, row['thput_avg']]

		if widest_thput:
			widest_thput['all'] = max(list(widest_thput.values()))[:]
		else:
			continue

		for _,row in group.iterrows():
			# new_tput = 0
			# if best_thput['LTE+MW'] > 0:
			# 	new_tput = best_thput['LTE+MW']
			# else:
			# 	new_tput = max(best_thput['LTE+Sub6'], best_thput['LTE'])

			results.append([grid[0],grid[1],row['thput_avg'],widest_thput['all'][1],best_thput['all']])

	new_df = pd.DataFrame(results, columns=['grid_lat','grid_lon',
		'legacy','ca++','optimal'])

	new_df.to_csv(sys.argv[2], index=False)
