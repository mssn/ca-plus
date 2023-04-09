Here are instructions to generate results using our code and data:


Figure 10:
  (a)-(d): $ python measurement_time.py ../dataset/{att,tmobile,verizon}/{A,V,T}_instances.csv {A,T,V}
	# Note that city is recognized by column "region" in the file: 
	#   ATT: R1-C1, R3-C2; T-Mobile: R12-C1; Verizon: R6-C1

  (e): 	
    1. Download data at XXX and decompress into local folder hst_data
    2. $ python measurement_time_hst.py hst_data ../HST/enb_dict_hst.txt


Figure 11:
  (a):
    1. 5G data: $ python conn_time_5g.py ../dataset/{att,tmobile,verizon}/{A,V,T}_instances.csv
    2. HST data: $ find hst_data -type f | xargs -I {} python conn_time_hst.py {} ../HST/enb_dict_hst.txt
  
  (b): 
    1. 5G data: $ python measurement_report.py ../dataset/{att,tmobile,verizon}/{A,V,T}_instances.csv {A,T,V}
    2. HST data: $ find hst_data -type f | xargs -I {} python signaling_hst.py {} ../HST/enb_dict_hst.txt


Figure 12
  (a)(b): $ python throughput_map.py ../dataset/att/A-C1_grid_cell_set_ca_em_0405-1226.csv {output}

  (c)(d): Use data points in ../dataset/att/A-R1_grid_avg_bandwidth_ca_em_submission_0405-1226.csv


Figure 13:
  $ python channel_width_boost.py ../dataset/{att,tmobile,verizon}/instances_5g.csv {A,T,V}

Figure 14:
  $ python throughput_boost_{att,tmo,vrz}.py ../dataset/{att,tmobile,verizon}/{A-C1,A-C2,V-C1,T-C1}_grid_cell_set_ca_em_0405-1226.csv {output}

Figure 16:
  1. To tune PCell threshold A2/A3/A5
    a. Specify the delta using corresponding variables (a3_delta, a2_delta, a5_delta) in main function. For example, a3_delta = 0, a2_delta = -10, a5_delta = 0 means reduce A2 threshold by 10.
    b. Run command e.g. 
      $ python tune_policy_pcell_v.py ../verizon/V-C1_grid_cell_set_ca_em_0405-1226.csv ../verizon/V-C1_rss_cell_0405-0127_311480_0.0005_0_0.csv ../verizon/V-C1_grid_cellset_performance_cfg.csv {output}

  2. To tune LTE SCell threshold A1
    a. Specify the delta using variable lte_delta.
    b. Run command e.g. 
      $ python tune_policy_scell_lte_a.py ../att/A-C2_grid_cell_set_ca_em_0405-1226.csv ../att/A-C2_rss_cell_0405-0127_310410_0.0005_0_0.csv ../att/A-C2_grid_cellset_performance_cfg.csv {output}

  3. To tune NR SCell threshold B1
    a. Specify the delta using variable b1_nr_delta.
    b. Run command e.g.
      $ python tune_policy_scell_nr_a.py ../att/A-C2_grid_cell_set_ca_em_0405-1226.csv ../att/A-C2_rss_cell_0405-0127_310410_0.0005_0_0.csv ../att/A-C2_grid_cellset_performance_cfg.csv {output}

  3. To disable 5G cells
    a. Run command e.g.
      $ python tune_policy_disable_nr.py  ../verizon/V-C1_grid_cell_set_ca_em_0405-1226.csv ../verizon/V-C1_rss_cell_0405-0127_311480_0.0005_0_0.csv ../verizon/V-C1_grid_cellset_performance_cfg.csv {output}
