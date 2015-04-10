library(quantmod)
library(iotools)

library(foreach)
library(itertools)
library(Matrix)
library(fUnitRoots)

log_rate_return = function(prices) {
  log(as.vector(prices[2:length(prices)])) / 
    log(as.vector(prices[1:(length(prices)-1)]))
}

cointegration_p_matrix = function(x) {
  x = na.omit(x)
  cs = combn(colnames(x), 2)
  # parallelize this for bigger matrices.
  if (is.null(getDoParName())) registerDoSEQ()
  r=foreach(it=isplitCols(cs, chunks=getDoParWorkers()), .combine=`+`) %dopar% {
    # Create the sparse matrix for this process.
    m = Matrix(data=0, nrow=ncol(x), ncol=ncol(x), sparse=TRUE,
               dimnames=list(names(x), names(x)))
    for (j in 1:ncol(it)) {
      # Fit the cointegration.
      fit=lm(as.formula(paste(it[1,j], "~", it[2,j], "+0")), data=x)
      val=as.vector(unitrootTest(fit$residuals, type="nc")@test$p.value[1])
      m[it[1,j], it[2,j]] = val
    }
    m
  }
  r[lower.tri(r)]=0
  r
}

cointegration_info= function(x) {
  ps = cointegration_p_matrix(x)
  p_vals = suppressMessages(ps[upper.tri(ps)])
  list(p_matrix=cointegration_p_matrix(x), 
       p_value=ks.test(p_vals, punif)$p.value,
       p_stat=100*sum(1-p_vals)/length(p_vals))
}

load("../data/ishares_sector_funds.rda")
adj_closes=foreach(symbol = ishares_sector_funds$symbol, .combine=cbind) %do% {
  stock_lookup_env = new.env()
  ret = try(getSymbols(paste("", symbol, sep=""), env=stock_lookup_env))
  if (inherits(ret, "try-error"))
    ret = NULL
  else 
    ret = stock_lookup_env[[ret]][,6]
  ret
}

names(adj_closes) = gsub(".Adjusted", "", names(adj_closes))

adj_closes = na.omit(adj_closes)
x=adj_closes
y = foreach(j=1:ncol(x), .combine=cbind) %do% {
  log_rate_return(x[,j])
}
y = as.data.frame(y)
names(y) = names(x)
cointegrate(x)
cointegrate(y)



x = iotools:::read.table.raw(
  "/Users/mike/projects/jackson_lecture/may_trades/taq_20100506_trades_all.csv",
  header=TRUE, sep=",")

names(x) = tolower(names(x))
# Remove bunk trades.
x = na.omit(x[!(x$cond %in% c("L", "N", "O", "Z", "P")),1:5])
x = x[x$size > 0,]

sym_split = split(1:nrow(x), x$symbol)
x$time_stamp = paste(x$date, x$time)
x$date_time = strptime(x$time_stamp, format="%Y%m%d %H:%M:%S", tz="EST")

price = x$price[x$symbol=="AAPL"]
size = x$size[x$symbol=="AAPL"]
date_time = x$date_time[x$symbol=="AAPL"]

vwap = function(price, size, date_time, on="minutes", k=1) {
  tx = xts(cbind(price, size), order.by=date_time, unique=FALSE)
  ep = endpoints(date_time, on=on, k=k)
}

