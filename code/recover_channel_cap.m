function [error]= recover_solve_usrp(H, f2, f1, T, delta_f, delta_nu, delta_tau, Orig_H, M2, N2)

M = size(H, 1);
N = size(H, 2);
M_max = min(10, M); % we only check for 12 * delata_delay
P_max = 10; 

nu_p = [];
tau_p = [];
h_p = [];

for k = 1:M_max
    if sum(abs(H(k, :))) > 1e-10
        nu2_int = acot(abs(H(k, 1)) / abs(H(k, N/2 + 1)));
        nu2 = delta_nu * nu2_int * N / pi;
        nu2 = min(nu2, 100);
        h = sqrt(sum(abs(H(k, :)).^2));
        tau = (k-1) * delta_tau; 
        nu_p = [nu_p nu2];
        tau_p = [tau_p tau];
        h_p = [h_p h];
    end
end

% nu_p
% h_p
% tau_p

nu_p2 = abs(nu_p).*f2/f1;
P_max = size(nu_p, 2);
[h,F2,P2,G2]=h_w(M2,N2,P_max,delta_f,delta_tau,delta_nu,T,tau_p,nu_p2,h_p);
[h_old,F2,P2,G2]=h_w(M2,N2,P_max,delta_f,delta_tau,delta_nu,T,tau_p,nu_p,h_p);
error = get_error(h, Orig_H);

end