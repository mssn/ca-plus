# Dataset Release for CA++: Enhancing Carrier Aggregation Beyond 5G (MobiCom'23)

In this repo, we release datasets and analysis codes for experiments. 

If you use them in your publications, please cite our MobiCom'23 paper, 
@inproceedings{li2023ca++,
  title={CA++: Enhancing Carrier Aggregation Beyond 5G},
  author={Li, Qianru and Zhang, Zhehui and Liu, Yanbing and Tan, Zhaowei and Peng, Chunyi and Lu, Songwu},
  booktitle={Proceedings of the 29th Annual International Conference on Mobile Computing and Networking (MobiCom'23)},
  year={2023}
}

## Instructions

Users can follow instructions here to use data and generate experiment results. We have ```$DATA=./dataset``` and ```$SRC=./code```.
  
### Evaluation with real-world datasets

- Figure 10:
  - (a)-(d): Run the following command to get results. Note that city is recognized by column "region" in the file, as we have ATT: R1-C1, R3-C2; T-Mobile: R12-C1; Verizon: R6-C1
  
  ```$ python $SRC/measurement_time.py $DATA/{att,tmobile,verizon}/{A,V,T}_instances.csv {A,T,V}```

  - (e): Download [HST dataset](http://mssn3.cs.purdue.edu/350.zip)[1], and then unzip it into any folder. Assume the data folder is $hst_data. Run command:
  
  ```$ find $hst_data -type f | xargs -I {} python $SRC/measurement_time_hst.py {} $DATA/HST/enb_dict_hst.txt```


- Figure 11:
  - (a) Perform different operatons for for 5G and HST data respectively:
  
    5G data: Run command ```$ python $SRC/conn_time_5g.py $DATA/{att,tmobile,verizon}/{A,V,T}_instances.csv```
    
    HST data: Download data file from [HST dataset](http://mssn3.cs.purdue.edu/350.zip)[1], and then unzip it into any folder. Assume the data folder is $hst_data. Then run command ```$ find $hst_data -type f | xargs -I {} python $SRC/conn_time_hst.py {} $DATA/HST/enb_dict_hst.txt```
  
  - (b) Perform different operatons for for 5G and HST data respectively:
    
    5G data: Run command ```$ python $SRC/measurement_report.py $DATA/{att,tmobile,verizon}/{A,V,T}_instances.csv {A,T,V}```
    
    HST data: Download data file from [HST dataset](http://mssn3.cs.purdue.edu/350.zip)[1], and then unzip it into any folder. Assume the data folder is $hst_data. Then run command ```$ find $hst_data -type f | xargs -I {} python $SRC/signaling_hst.py {} $DATA/HST/enb_dict_hst.txt```


- Figure 12
  - (a)(b): ```$ python $SRC/throughput_map.py $DATA/att/A-C1_grid_cell_set_ca_em_0405-1226.csv {output}```

  - (c)(d): Use data points in ```$DATA/att/A-R1_grid_avg_bandwidth_ca_em_submission_0405-1226.csv```


- Figure 13:
  ```$ python $SRC/channel_width_boost.py $DATA/dataset/{att,tmobile,verizon}/instances_5g.csv {A,T,V}```

- Figure 14:
  ```$ python $SRC/throughput_boost_{att,tmo,vrz}.py $DATA/{att,tmobile,verizon}/{A-C1,A-C2,V-C1,T-C1}_grid_cell_set_ca_em_0405-1226.csv {output}```

- Figure 16: We need four sets of operations to tune various policies:
  - PCell threshold A2/A3/A5
    - Specify the delta using corresponding variables (a3_delta, a2_delta, a5_delta) in main function. For example, a3_delta = 0, a2_delta = -10, a5_delta = 0 means reduce A2 threshold by 10.
    - Run command e.g. 
      ```$ python $SRC/tune_policy_pcell_v.py $DATA/verizon/V-C1_grid_cell_set_ca_em_0405-1226.csv $DATA/verizon/V-C1_rss_cell_0405-0127_311480_0.0005_0_0.csv $DATA/verizon/V-C1_grid_cellset_performance_cfg.csv {output}```

  - LTE SCell threshold A1
    - Specify the delta using variable lte_delta.
    - Run command e.g. 
      ```$ python $SRC/tune_policy_scell_lte_a.py $DATA/att/A-C2_grid_cell_set_ca_em_0405-1226.csv $DATA/att/A-C2_rss_cell_0405-0127_310410_0.0005_0_0.csv $DATA/att/A-C2_grid_cellset_performance_cfg.csv {output}```

  - NR SCell threshold B1
    - Specify the delta using variable b1_nr_delta.
    - Run command e.g.
      ```$ python $SRC/tune_policy_scell_nr_a.py $DATA/att/A-C2_grid_cell_set_ca_em_0405-1226.csv $DATA/att/A-C2_rss_cell_0405-0127_310410_0.0005_0_0.csv $DATA/att/A-C2_grid_cellset_performance_cfg.csv {output}```

  - To disable 5G cells
    - Run command e.g.
      ```$ python $SRC/tune_policy_disable_nr.py  $DATA/verizon/V-C1_grid_cell_set_ca_em_0405-1226.csv $DATA/verizon/V-C1_rss_cell_0405-0127_311480_0.0005_0_0.csv $DATA/verizon/V-C1_grid_cellset_performance_cfg.csv {output}```
      
### Code for inter-channel wireless quality inference

- Run ```$SRC/inference/main.m``` to get recovery error with CA++
      
      
[1] Processed raw modem traces and generated text logs for the original dataset from Wang, Jing, et al. "An active-passive measurement study of tcp performance over lte on high-speed rails." The 25th Annual International Conference on Mobile Computing and Networking. 2019.
