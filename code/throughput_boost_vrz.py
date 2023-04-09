import sys, os 
import pandas as pd 

op = ''

def lte_bandwidth_tmo(f):
	freq_to_bandwidth = {677:15, 1123:15, 1200:10, 1275:15, 1475:5, 5035:5, 39874:20, 40072:20, 66736:10, 66811:15, 67011:5}

	if int(f) not in freq_to_bandwidth:
		unknown_channel.add(int(f))
		return 0
	return freq_to_bandwidth[int(f)]
	

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

def nr_bandwidth_tmo(f):
	freq_to_bandwidth_nr = {125290:20, 125330:15, 519630:100, 521550:80}
	if int(f) not in freq_to_bandwidth_nr:
		# print(f)
		unknown_channel.add(int(f))
		return 0
	return freq_to_bandwidth_nr[int(f)]

def nr_bandwidth(f):
	if f == "None":
		return 0

	if int(f) < 1e6:
		return 10
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
		# if best_thput:
		best_thput['all'] = max(list(best_thput.values()))

		widest_thput = {'LTE+Sub6':[0,0], 'LTE':[0,0], 'LTE+MW':[0,0]}

		for _,row in group.iterrows():
			pid,freq = int(row['pcell_cid']), int(row['pcell_freq'])

			new_tput = 0 
			# first select LTE+MW if available
			if best_thput['LTE+MW'] > 0:
				new_tput = best_thput['LTE+MW']
			else:
				new_tput = max(best_thput['LTE+Sub6'], best_thput['LTE'])

			results.append([grid[0],grid[1],row['thput_avg'],new_tput,max(list(best_thput.values()))])

	new_df = pd.DataFrame(results, columns=['grid_lat','grid_lon',
		'legacy','ca++','optimal'])

	new_df.to_csv(sys.argv[2], index=False)
