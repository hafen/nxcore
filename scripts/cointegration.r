library(devtools)

document("..")

x = iotools:::read.table.raw(
  "/Users/mike/projects/jackson_lecture/may_trades/taq_20100506_trades_all.csv",
  header=TRUE, sep=",")

names(x) = tolower(names(x))
# Remove bunk trades.
x = na.omit(x[!(x$cond %in% c("L", "N", "O", "Z", "P")),1:5])
x = x[x$size > 0,]

sym_split = split(1:nrow(x), x$symbol)
x$time_stamp = paste(x$date, x$time)
x$date_time = strptime(x$time_stamp, format="%Y%m%d %H:%M:%S", tz=Sys.timezone())

data(sp)

sp_split = sym_split[names(sym_split) %in% sp$symbol]
sp_stocks = foreach(inds=sp_split, .combine=cbind) %dopar% {
  vwap(x$price[inds], x$size[inds], x$date_time[inds])
}
names(sp_stocks) = names(sp_split)
