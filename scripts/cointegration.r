library(devtools)
library(doMC)
library(iotools)
library(xts)
library(devtools)
library(doMC)
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
x = x[x$symbol %in% sp$symbol,]

# x is a data frame with a price, size, and symbol column.
clean_and_normalize_transactions = function(x, on="minutes", k=1) {
  sp_split = split(1:nrow(x), x$symbol)
  x = foreach(inds=sp_split, .combine=cbind) %do% {
    xts(cbind(x$price[inds], x$size[inds]), order.by=x$date_time[inds])
  }
  psp = matrix(1:ncol(x), ncol=2, byrow=TRUE)
  # The following is a hog and it needs to be better. Should vwap return an
  # xts object or would it be better as a data frame?
  x = foreach(i = 1:nrow(psp), .combine=cbind) %dopar% {
    vwap(as.vector(x[,psp[i, 1]]), as.vector(x[,psp[i,2]]),
         time(x), on=on, k=k)
  }
  x = foreach(j=1:ncol(x), .combine=cbind) %dopar% {
    carry_price_forward(x[,j])
  }
  colnames(x) = names(sp_split)
  x
}

sp_trades = clean_and_normalize_transactions(x, on="seconds", k=1)

