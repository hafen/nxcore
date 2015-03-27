
#' @export
realized_volatility = function(price, date_time, interval=minutes(5)) {
  x = data.frame(list(price=price, date_time=date_time))
  x$price = c(NA, x$price[1:(length(x$price)-1)])
  x$r = log(x$price/x$last_price)
  foreach(it=volume_window_gen(x$date_time, interval), .combine=rbind) %dopar% {
    xs = x[it,]
    data.frame(
      list(volatility=100*sqrt(252/ncol(xs) * mean(xs$r^2, na.rm=TRUE)),
           time=tail(xs$date_time, 1)))
  }
}

