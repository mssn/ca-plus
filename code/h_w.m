function [H,F,P,G]=h_w(M,N,P_max,delta_f,delta_tau,delta_nu,T,tau_p,nu_p,h_p)

F=zeros(M,P_max);
P=zeros(P_max,P_max);
G=zeros(P_max,N);

for k=0:M-1
    for p=1:P_max
        for d=0:M-1
            F(k+1,p) = F(k+1,p) + exp(1j*2*pi*d*delta_f*(k*delta_tau-tau_p(p)));
        end
    end
end
F = F./M;

for l=0:N-1
    for p=1:P_max
        for c=0:N-1
            G(p,l+1) = G(p,l+1) + exp(-1j*2*pi*c*T*(l*delta_nu-nu_p(p)));
        end
    end
end
G = G./N;

for p=1:P_max
    P(p,p)=h_p(p);
end


H=F*P*G;

end
