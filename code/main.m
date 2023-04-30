% We read the dataset of one freq and use CA++ algorithm to infer another freq
clear;

M = 1024; N = 14;
new_M = 1024; new_N = 14;
delta_f = 10e6/1024; % 15khz
T = 1/delta_f; % T*delta_f==1 in OFDM 
delta_tau = 1/(new_M*delta_f);
delta_nu = 1/(new_N*T);

freq = [2.45, 2.55, 3.65, 5.45, 5.55, 58, 62, 66] * 1e9;
f1 = freq(5);
f2 = freq(6);
% Traces1 = load(matname1).Trace_otfs;
% Traces2 = load(matname2).Trace_otfs;
mock = zeros(M, N);
mock(1,1) = 1;
Traces1 = cell(1, 1);
Traces1{1} = a;
Traces2 = cell(1, 1);
Traces2{1} = a;

pkt = 1;
H1= Traces1{pkt}; 
H1 = channel_est_L0norm(H1, new_M, new_N);
H2=Traces2{pkt}; % matrix to infer
H2=channel_est_L0norm(H2, new_M, new_N);

error_CAP = recover_channel_cap(H1, f2, f1, T, delta_f, delta_nu, delta_tau, H2, new_M, new_N);