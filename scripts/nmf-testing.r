# Basic proof of concept to show how the correlation structure is preserved using NMF.
library(devtools)

taq_file = "~/projects/jackson_lecture/may_trades/taq_20100506_trades_all.csv"

#document("..")
library(nxcore)

data(sp)
symbols = sp$symbol[sample(1:length(sp$symbol), 20)]
x = read_taq(taq_file, symbols, on="minutes", k=1)
ci1 = coint_measure(x, minutes(30))


x_mat = as.matrix(x)
plot(x_mat[,1], type='l', ylim=range(x_mat))
for (i in 2:ncol(x_mat)) 
  lines(x_mat[,i], col=i)

dev.new()

nmf_fit = nmf(x_mat, rank=7)
W = nmf_fit@fit@W
H = nmf_fit@fit@H

x_proj = tcrossprod(x_mat, H)
x_proj_ts = xts(x_proj, order.by=time(x))
ci2 = coint_measure(x_proj_ts, minutes(30))

plot(x_proj[,1], type='l', ylim=range(x_proj))
for (i in 2:ncol(x_proj)) 
  lines(x_proj[,i], col=i)

svd_fit = svd(x_mat)
x_proj2 = x_mat %*% svd_fit$v[,1:7]
x_proj_ts2 = xts(x_proj2, order.by=time(x))
ci3 = coint_measure(x_proj_ts2, minutes(30))

# Now call the NMF and SVD "online".

ci22 = coint_measure(x, minutes(30), dr="nmf", dr_rank=7)

ci32 = coint_measure(x, minutes(30), dr="svd", dr_rank=7)

plot(ci1, ylim=range(ci1, ci2, ci3))
lines(ci2, col="red")
lines(ci3, col="blue")
lines(ci22, col="green")
lines(ci32, col="grey")

dev.new()
dm = cbind(ci2-ci1, ci3-ci1, ci22-ci1, ci32-ci1)
plot(dm[,1], ylim=range(dm), color="red")
lines(dm[,2], col="blue")
lines(dm[,3], col="green")
lines(dm[,4], col="grey")

library(doMC)
registerDoMC(3)
nmf_meas = foreach(i=2:10, .combine=rbind) %do% {
  ci_nmf = coint_measure(x, minutes(30), dr="nmf", dr_rank=i)
  c(mean(as.vector(ci_nmf)-ci1), var(ci_nmf-ci1))
} 

svd_meas = foreach(i=2:10, .combine=rbind) %do% {
  ci_svd = coint_measure(x, minutes(30), dr="svd", dr_rank=i)
  c(mean(as.vector(ci_svd)-ci1), var(ci_svd-ci1))
} 

# NMF wins both for bias and variance.
plot(nmf_meas[,1], type="l", ylim=range(c(svd_meas[,1], nmf_meas[,1])))
lines(svd_meas[,1], col="red")
dev.new()
plot(nmf_meas[,2], type="l", ylim=range(c(svd_meas[,2], nmf_meas[,2])))
lines(svd_meas[,2], col="red")

nmf_meas = foreach(i=2:20, .combine=rbind) %do% {
  print(
    system.time({ci_nmf = coint_measure(x, minutes(30), dr="nmf", dr_rank=i)}))
  print(i)
  c(mean(as.vector(ci_nmf)-ci1), var(ci_nmf-ci1))
} 


# Let's try all of them.
registerDoMC(3)
x = read_taq(taq_file, sp$symbol, on="minutes", k=1)
nmf_meas_sp = foreach(i=2:15, .combine=rbind) %do% {
  print(
    system.time({ci_nmf = coint_measure(x, minutes(30), dr="nmf", dr_rank=i)}))
  print(i)
  c(mean(as.vector(ci_nmf)-ci1), var(ci_nmf-ci1))
} 

