function [H_est]= channel_est_L0norm(y, M, N)

Y=y'*M*N;
maximum = max(max(abs(Y)));
[r,c]=find(abs(Y)>maximum-0.0001);
Y(abs(Y)<maximum/10) = 0;
Y2 = circshift(Y,[-r(1)+1  -c(1)+1]);
remainY = Y2(2:end,:);
maximum = max(max(abs(remainY)));
[r,c]=find(abs(remainY)>maximum-0.0001);
remainY = circshift(remainY,[-r(1)+1  -c(1)+1]);
Y2 = [Y2(1,:); remainY];

H_est0 = zeros(M,N);
y_abs = abs(Y2);
[row, col] = find(y_abs == max(max(y_abs)));
H_est0(1:2,1:3) = y_abs(1:2,1:3); % initilization
H_est0 = H_est0(:);
y_abs=y_abs(:);
y=y_abs;
fun = @(x)norm(x-y_abs);
nonlcon = @(x)mynonlcon(x, M, N);
options = optimoptions('fmincon','Display','iter','MaxFunctionEvaluations', 1e3);
H_est = fmincon(fun,H_est0,[],[],[],[],[],[], nonlcon, options);

error = H_est-y_abs;
y_sum=sum(abs(y));
error_sum=sum(abs(error));
percent=sum(abs(error))/sum(abs(y));
fprintf('orig %f error %f percent %f\n',y_sum,error_sum,percent);
fprintf('orig %f H0(1,1) after %f H(1,1) \n', H_est0(1,1), H_est(1,1));

H_est = H_est/abs(max(H_est));
H0 = reshape(H_est, M, N);
H_est = H0;

end


function [c,ceq] = mynonlcon(x, M, N)
c = sum(abs(x(2:M*N)));
H_filter = ones(M,N);
H_filter(1:10,1:10) = 0;
ceq = x .* reshape(H_filter, M*N, 1);
end
