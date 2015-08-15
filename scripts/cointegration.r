library(devtools)
library(doMC)
library(iotools)
library(xts)
library(devtools)
library(doMC)
#registerDoMC(cores=16)
#registerDoMC(cores=10)

document("..")
col_types=c(rep("character",3), rep("numeric", 3), rep("character", 2))

x = dstrsplit(readAsRaw(
 "/Users/mike/projects/jackson_lecture/may_trades/taq_20100506_trades_all.csv"),
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

on="minutes"
k=30

sym_split = split(1:nrow(x), x$symbol)
# Create the consolidated trade data.
cat("Consolidating trade data\n")
taq = foreach (sym = sp$symbol, .combine=rbind, .inorder=FALSE) %dopar% {
  registerDoSEQ()
  d = x[sym_split[[sym]],]
  ret = NULL
  if (nrow(d) > 0) {
    ret = as.data.frame(consolidate_prices(d$date, d$time, d$price, d$size,
                             time_format="%H:%M:%S", date_format="%Y%m%d",
                             on=on, k=k))
    ret$symbol = sym
  }
  ret
}

taq$date_time = strptime(rownames(taq), "%Y-%m-%d %H:%M:%S")
# Create the xts matrix of stock values.
sym_split = split(1:nrow(x), x$symbol)
prices = foreach(sym_inds=sym_split, .combine=cbind) %dopar% {
  xs = x[sym_inds,]
  xst = xts(xs$price, order.by=xs$date_time)
  xts(xs$price, order.by=xs$date_time)
}

colnames(prices) = names(sym_split)
# Carry prices forward for each column.
cat("Carrying prices forward.\n")
prices = carry_prices_forward(prices)
prices = na.omit(prices)

# Make sure that we are dealing with the right resolution after combining.
prices = period.apply(prices, endpoints(prices, on=on, k=k),
  function(ps) {
    xts(matrix(apply(as.matrix(ps), 2, mean, na.rm=TRUE), nrow=1),
        order.by=time(ps[1]))
  })




# x is a data frame with a price, size, and symbol column.
clean_and_normalize_transactions = function(x, on="minutes", k=1) {
  sp_split = split(1:nrow(x), x$symbol)
  x = foreach(inds=sp_split, .combine=cbind) %dopar% {
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
document("..")
foreach(it = volume_window_gen(time(x)), .combine=rbind) %dopar% {
  ci = cointegration_info(x[it,])
  c(ci$p_value, ci$p_stat)
}
