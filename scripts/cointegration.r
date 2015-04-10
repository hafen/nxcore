library(devtools)
library(doMC)
library(iotools)
library(xts)
registerDoMC(cores=16)

document("..")
col_types=c(rep("character",3), rep("numeric", 3), rep("character", 2))

x = dstrsplit(readAsRaw("/home/mkane/taq_20100506_trades_all.csv"),
              sep=",", skip=1, col_types=col_types)              
names(x) = c("symbol", "date", "time", "price", "size", "corr", "cond", "ex")	

# Remove bunk trades.
x = na.omit(x[!(x$cond %in% c("L", "N", "O", "Z", "P")),1:5])
x = x[x$size > 0,]

sym_split = split(1:nrow(x), x$symbol)
x$time_stamp = paste(x$date, x$time)
x$date_time=strptime(x$time_stamp, format="%Y%m%d %H:%M:%S", tz=Sys.timezone())

data(sp)

sp_split = sym_split[names(sym_split) %in% sp$symbol]
sp_stocks = foreach(inds=sp_split, .combine=cbind) %do% {
  xts(cbind(x$price[inds], x$size[inds]), order.by=x$date_time[inds])
}
psp = matrix(1:ncol(sp_stocks), ncol=2, byrow=TRUE)
sp_stocks2 = foreach(i = 1:nrow(psp), .combine=cbind) %dopar% {
  vwap(as.vector(sp_stocks[,psp[i, 1]]), as.vector(sp_stocks[,psp[i,2]]),
       time(sp_stocks))
}

sp_stocks2 = foreach(j=1:ncol(sp_stocks), .combine=cbind) %dopar% {
  carry_price_forward(sp_stocks[,j])
}
