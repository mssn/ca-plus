function [error] = get_error(est_H, Orig_H)
est_norm = mean(mean((abs(est_H))));
gt_norm = mean(mean((abs(Orig_H))));
error = 10*log10(est_norm/gt_norm);
fprintf('===\nest %f orig %f error %f db\n===\n',est_norm, gt_norm,error);
end