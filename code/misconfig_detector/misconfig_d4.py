import sys
import re
from collections import defaultdict
from operator import itemgetter

in_sib1 = None
in_sib3 = None # flag of current log
in_sib5 = None
in_sib5_freq = None
in_sib5_freq_get = None
in_sib5_freq_get_qmin = None
in_sib5_freq_get_threshq = None
in_sib3_freq_get_qmin = None
in_sib3_freq_get_threshq = None
cur_config = defaultdict(lambda: None) # {freqs: {freq: [sib5 thresh, sib5 high/low]}, sib3: False/True}
cid = None
pci = None
last_cid = None
last_freq = None
freq = None
op = None
wait_priority = False
gps = None
tac = None
ts = None
log = None
with open(sys.argv[1]) as file:
	for line in file:
		line = line.strip()
		if line =='sib3':
			in_sib3 = True
			in_sib1 = False
			in_sib5 = False
			# print('sib3')
		elif line == 'sib5':
			in_sib5 = True
			in_sib1 = False
			in_sib3 = False
		elif line == 'systemInformationBlockType1':

			in_sib1 = True
			in_sib3 = False
			in_sib5 = False
			# print('DEBUG', in_sib1)
		elif line.startswith('sib'):
			# in_sib1 = False # sib1 will be a standalone message
			in_sib3 = False
			in_sib5 = False
			# print('sib5')
		# elif 'LTE_RRC_MIB_Packet' in line or 'mobilityControlInfo' in line:

		# 	in_sib3 = None
		# 	in_sib5 = None
		# 	in_sib5_freq = None
		# 	in_sib5_freq_get = None
		# 	in_sib5_freq_get_qmin = None
		# 	in_sib5_freq_get_threshq = None
		# 	in_sib3_freq_get_qmin = None
		# 	in_sib3_freq_get_threshq = None			

		elif re.match('^20..-..-..', line):
			# print(line)

			# if in_sib3:
				# print in_sib3_freq_get_qmin, in_sib3_freq_get_threshq
			# if not pci:
			blocks = line.split()
			if len(blocks) > 2 and blocks[2][0].isdigit():
				pci = blocks[2]
				freq = blocks[3]
				# print(freq, pci)
			# print(last_cid)
			# print(last_freq)

			if in_sib5_freq_get:
					# if not in_sib5_freq_get_qmin and in_sib3_freq_get_threshq:
				if not cur_config['freqs']:
					cur_config['freqs'] = {}
				if qual_min:
					qual_high = threshq_high + qual_min
					qual_low = threshq_low + qual_min
				else:
					qual_high = -666
					qual_low = -666
				cur_config['freqs'][sib5_freq] = [in_sib5_freq_get_qmin, in_sib5_freq_get_threshq, priority, qual_high, qual_low]
				in_sib5_freq_get = False

			if last_cid and (last_cid != pci or last_freq != freq):
				# print('INFO', last_freq, last_cid, freq, pci)
				# print(cur_config)
				# if cur_config['sib3_priority']:
				sib3_priority = cur_config['sib3_priority']
				# if cur_config['sib3_get_threshq']:
				in_sib3_freq_get_threshq = cur_config['in_sib3_freq_get_threshq']
				# print(cur_config['freqs'])


				if cur_config['freqs']:
					priorities = [cur_config['freqs'][sib5_freq][2] for sib5_freq in cur_config['freqs'] if cur_config['freqs'][sib5_freq][2]]
					# print(cur_config)
					for sib5_freq in cur_config['freqs']:
						if sib5_freq == last_freq or cur_config['sib3_priority'] == None:
							continue
						# print(cur_config['freqs'][sib5_freq][2])
						# print(priorities)
						if not cur_config['freqs'][sib5_freq][0] and in_sib3_freq_get_threshq and sib3_priority != None:
							highest_pri = False
							if cur_config['freqs'][sib5_freq][2] and cur_config['freqs'][sib5_freq][2] == max(priorities) and cur_config['freqs'][sib5_freq][2] != sib3_priority:
								highest_pri = True
							higher_serv = False
							if cur_config['freqs'][sib5_freq][2] and cur_config['freqs'][sib5_freq][2] > sib3_priority:
								higher_serv = True
							# 	print('INFO', 'type 3')
							# elif cur_config['freqs'][sib5_freq][2] and cur_config['freqs'][sib5_freq][2] < sib3_priority:
							# 	print('INFO', 'type 4')
						else:
							continue
						
						if highest_pri or higher_serv:
							#print(log)
							#print(gps)
							print(ts, end=' ') 
							print(cid, 'PCI', last_cid, 'EARFCN', last_freq, sib5_freq, cur_config['freqs'][sib5_freq][0], cur_config['freqs'][sib5_freq][1], 'sib3', \
							in_sib3_freq_get_threshq, sib3_priority, cur_config['freqs'][sib5_freq][2], op, 'TAC', tac, int(cid)//256, cur_config['freqs'][sib5_freq][3:])
							#print([(sib5_freq, cur_config['freqs'][sib5_freq][2]) for sib5_freq in cur_config['freqs'] if cur_config['freqs'][sib5_freq][2]])
							# check different types


						# rxlev_intra = # search if <
						# rxlev_nonintra = # search if <
						# rxlev_low = # search if <
						# print(cur_config)
						
							if cur_config['s-nonintraP'] != None:
								cur_config['rxlev_nonintra'] = cur_config['rxlev'] + cur_config['s-nonintraP']
							else:
								cur_config['s-nonintra'] = 0 if not cur_config['s-nonintra'] else cur_config['s-nonintra']
								cur_config['rxlev_nonintra'] = cur_config['rxlev'] + cur_config['s-nonintra']
							if cur_config['s-intraP'] != None:
								cur_config['rxlev_intra'] = cur_config['rxlev'] + cur_config['s-intraP']
							else:
								cur_config['s-intra'] = 0 if not cur_config['s-intra'] else cur_config['s-intra'] 
								cur_config['rxlev_intra'] = cur_config['rxlev'] + cur_config['s-intra']
							cur_config['rxlev_low'] = cur_config['rxlev'] + cur_config['thresh_low']


							if cur_config['thresh_lowq'] != None:
								if cur_config['qual_sib1'] != None and cur_config['qual_sib1'] != '-inf':
									cur_config['qual_low'] = cur_config['qual_sib1'] + cur_config['thresh_lowq']
								else:
									cur_config['qual_sib1'] = '-inf'
									cur_config['qual_low'] = '-inf'
								if cur_config['s-intraQ'] != None:
									cur_config['qual_intra'] = cur_config['qual_sib1'] + cur_config['s-intraQ']
								else:
									cur_config['qual_intra'] = cur_config['qual_sib1']
								if cur_config['s-nonintraQ'] != None:
									cur_config['qual_nonintra'] = cur_config['qual_sib1'] + cur_config['s-nonintraQ']
								else:
									cur_config['qual_nonintra'] = cur_config['qual_sib1']
							
							config_list = []
							for k, v in sorted(cur_config.items(), key=lambda x: itemgetter(0)(x), reverse = True):
							# for i in cur_config:
								# if i != 'freqs' and 'sib' not in i:
								if k.startswith('rxlev') or k.startswith('qual'):
									config_list += [k, cur_config[k]]
							#print(' '.join(map(str, config_list)))

						

				cur_config = defaultdict(lambda: None)
				in_sib5_freq = None
				in_sib5_freq_get = None
				in_sib5_freq_get_qmin = None
				in_sib5_freq_get_threshq = None
				in_sib3_freq_get_qmin = None
				in_sib3_freq_get_threshq = None		

			last_cid = pci
			last_freq = freq

			in_sib1 = None
			in_sib3 = None
			in_sib5 = None
			# in_sib5_freq = None
			# in_sib5_freq_get = None
			# in_sib5_freq_get_qmin = None
			# in_sib5_freq_get_threshq = None

			ts = ' '.join(blocks[:2])
			# in_sib3_freq_get_qmin = False
			# in_sib3_freq_get_threshq = False
			if 'customPacket' in line:
				if len(blocks) > 9:
					gps = blocks[8] + ' ' + blocks[7][:-1] + ' ' + blocks[9]
					# 2019-08-27 02:35:34.877809 [customPacket] 2019-08-27 02:35:34.863 Location updated: -118.44755544, 33.99064333, 0.0


		elif line.startswith('###[new log]'):
			log = line

		elif line.startswith('cellIdentity:'):
			cid = line.split()[-1][:-1]

		elif line.startswith('LteRrcStatus'):
			op = line.split()[11][3:] # LteRrcStatus cellID=24 DL_frequency=850 UL_frequency=18850 DL_bandwidth=20 MHz UL_bandwidth=20 MHz Band_indicator=2 TAC=34624 OP=ATT connected=False
			tac = line.split()[10][4:]

		else:
			if in_sib5 and line=='InterFreqCarrierFreqInfo': # InterFreqCarrierFreqInfo
				in_sib5_freq = True
				
			elif in_sib5_freq and line.startswith('dl-CarrierFreq:'): # dl-CarrierFreq: 9720
				# FIXME: sib3 and sib5 are independent
				# print(line)
				if in_sib5_freq_get:
					# if not in_sib5_freq_get_qmin and in_sib3_freq_get_threshq:
					if not cur_config['freqs']:
						cur_config['freqs'] = {}
					if qual_min:
						qual_high = threshq_high + qual_min
						qual_low = threshq_low + qual_min
					else:
						qual_high = -666
						qual_low = -666
					cur_config['freqs'][sib5_freq] = [in_sib5_freq_get_qmin, in_sib5_freq_get_threshq, priority, qual_high, qual_low]
					# if True:
					# 	print(log)
					# 	print(gps)
					# 	print(ts, end=' ') 
					# 	print(cid, 'PCI', pci, 'EARFCN', freq, sib5_freq, in_sib5_freq_get_qmin, in_sib5_freq_get_threshq, 'sib3', \
					# 	in_sib3_freq_get_threshq, sib3_priority, priority, op, 'TAC', tac, int(cid)//256)
				sib5_freq = line.split()[1]
				in_sib5_freq_get = True
				wait_priority = True
				priority = None
				in_sib5_freq_get_qmin = None
				in_sib5_freq_get_threshq = None
				threshq_high = 0
				threshq_low = 0
				qual_min = None
			elif in_sib5_freq and wait_priority and 'cellReselectionPriority:' in line:
				priority = int(line.split()[1])
				wait_priority = False
			elif in_sib5_freq_get and 'q-QualMin-r9 is NOT present' in line:
				# print(line)
				in_sib5_freq_get_qmin = False
			elif in_sib5_freq_get and 'q-QualMin-r9 is present' in line:
				# print(line)
				in_sib5_freq_get_qmin = True
			elif in_sib5_freq_get and 'threshX-Q-r9 is present' in line:
				# print(line)
				in_sib5_freq_get_threshq = True
			elif in_sib5_freq_get and 'threshX-Q-r9 is NOT present' in line:
				# print(line)
				in_sib5_freq_get_threshq = False
				# print(in_sib5_freq_get_threshq)

			elif in_sib5_freq_get and 'threshX-HighQ-r9::' in line:
				threshq_high = int(line.split()[1][:-2])
			elif in_sib5_freq_get and 'threshX-LowQ-r9:' in line:
				threshq_low = int(line.split()[1][:-2])
			elif in_sib5_freq_get and 'q-QualMin-r9:' in line:
				qual_min = int(line.split()[1][:-2])	

				# q-QualMin-r9: -30dB
				# threshX-Q-r9
				# threshX-HighQ-r9: 14dB
				# threshX-LowQ-r9: 14dB

			if in_sib1 and 'q-QualMin-r9:' in line:
				# print(line)
				cur_config['qual_sib1'] = int(line.split()[1][:-2])	

			if in_sib3 and 'q-QualMin-r9 is NOT present' in line:
				cur_config['in_sib3_freq_get_qqual'] = False
				# print(in_sib3_freq_get_qmin)
			elif in_sib3 and 'q-QualMin-r9 is present' in line:
				cur_config['in_sib3_freq_get_qqual'] = True
				# print(in_sib3_freq_get_qmin)
			elif in_sib3 and 'threshServingLowQ-r9 is NOT present' in line:
				cur_config['in_sib3_freq_get_threshq'] = False
				# print(in_sib3_freq_get_threshq)
			elif in_sib3 and 'threshServingLowQ-r9 is present' in line:
				cur_config['in_sib3_freq_get_threshq'] = True
				# print(in_sib3_freq_get_threshq)
			elif in_sib3 and 'cellReselectionPriority:' in line:
				cur_config['sib3_priority'] = int(line.split()[1])

			elif in_sib3 and 's-NonIntraSearch:' in line:
				cur_config['s-nonintra'] = int(line.split()[1][:-2])
			elif in_sib3 and 's-NonIntraSearchP-r9:' in line:
				cur_config['s-nonintraP'] = int(line.split()[1][:-2])
			elif in_sib3 and 's-IntraSearchP-r9:' in line:
				cur_config['s-intraP'] = int(line.split()[1][:-2])
			elif in_sib3 and 's-IntraSearch:' in line:
				cur_config['s-intra'] = int(line.split()[1][:-2])	
			elif in_sib3 and 'threshServingLow:' in line:
				cur_config['thresh_low'] = int(line.split()[1][:-2])
			elif in_sib3 and 'q-RxLevMin:' in line:
				cur_config['rxlev'] = int(line.split()[1][:-3])	

			# RSRQ thresholds
			elif in_sib3 and 's-NonIntraSearchQ-r9:' in line:
				# print(line)
				cur_config['s-nonintraq:'] = int(line.split()[1][:-2])
			elif in_sib3 and 's-IntraSearchQ-r9:' in line:
				cur_config['s-intraq'] = int(line.split()[1][:-2])	
			elif in_sib3 and 'threshServingLowQ-r9:' in line: 
				cur_config['thresh_lowq'] = int(line.split()[1][:-2])
			elif in_sib3 and 'q-QualMin-r9:' in line:
				cur_config['qual'] = int(line.split()[1][:-2])	

				# print(sib3_priority)
				# .... .1.. Optional Field Bit: True (s-NonIntraSearch-v920 is present)
				# .... ..1. Optional Field Bit: True (q-QualMin-r9 is present)
				# .... ...1 Optional Field Bit: True (threshServingLowQ-r9 is present)




