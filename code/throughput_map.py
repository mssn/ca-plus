import sys, os 
import pandas as pd 

def lte_bandwidth(f):
	if f == "None":
		return 0
	freq_to_bandwidth = {675:15, 850:20, 1025:5, 5110:10, 5330:10, 9820:10, 9840:5, 
	52740:20, 52940:20, 52941:20, 53139:20, 53140:20, 53141:20, 53339:20, 53341:20, 53539:20,
	66486:10, 66661:5, 66686:10, 66936:10, 67086:10} # to be filled in

	if int(f) not in freq_to_bandwidth:
		return 0
	return freq_to_bandwidth[int(f)]

def nr_bandwidth(f):
	if f == "None":
		return 0
	if int(f) < 1e6:
		return 5
	return 100

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
		best_thput['all'] = max(list(best_thput.values()))

		# widest_thput = {'LTE+Sub6':[0,0], 'LTE':[0,0], 'LTE+MW':[0,0]}
		total_tput,total_time = 0,0
		for _,row in group.iterrows():
			# pid,freq = int(row['pcell_cid']), int(row['pcell_freq'])
			# lte_width = (lte_bandwidth(row['pcell_freq']) + lte_bandwidth(row['scell1_freq'])) + lte_bandwidth(row['scell2_freq']) + lte_bandwidth(row['scell3_freq'])

			# nr_width = (nr_bandwidth(row['nrcell_freq']) + nr_bandwidth(row['nrscell1_freq']) + nr_bandwidth(row['nrscell2_freq']) + nr_bandwidth(row['nrscell3_freq']))

			total_tput += row['thput_sample'] * row['thput_avg']
			total_time += row['thput_sample']

		results.append([grid[0],grid[1],total_tput/total_time,best_thput['all'],best_thput['LTE+MW'] if best_thput['LTE+MW'] > 0 else max(best_thput['LTE'],best_thput['LTE+Sub6'])])

	new_df = pd.DataFrame(results, columns=['grid_lat','grid_lon',
		'actual','oracle','ca++',])

	new_df.to_csv(sys.argv[2], index=False)
