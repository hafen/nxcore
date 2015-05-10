#' Calculate the realized volatility for a transction price time-series
#'
#' @param price and xts object containting transaction prices
#' @param interval the window size to calculate realized volatility over. The
#' default is 5 mintes.
#' @param interval const the number of trading periods specified by the interval
#  for example if you are looking at daily this should be set to 252, which
#' is the number of trading days in a year. It should be noted that this is 
#' only needed for calculations like realized volatility. For comparison
#' of two stocks over the same intervals it is fine to leave this as 1.
#' @examples
#' data(aapl_fix)
#' ctp = consolidate_prices(aapl_fix$sys_date, aapl_fix$sys_time, 
#'  aapl_fix$td_price, aapl_fix$td_size, date_format="%Y-%m-%d", 
#'  time_format="%H:%M:%S")
#' rv = realized_volatility(ctp$price)
#' plot(rv)
#' @export
realized_volatility = function(price, interval=minutes(5),
                               interval_const=1) {
  log_price = log(price)
  log_diff_price = na.omit(diff(log_price))
  if (is.null(getDoParName())) registerDoSEQ()
  ret=foreach(it=inclusive_window_gen(time(log_diff_price), interval), 
    .combine=rbind) %dopar% {
    ldp = log_diff_price[it]
    data.frame(
      list(volatility=
        100*sqrt(interval_const/length(ldp)*mean(ldp^2, na.rm=TRUE)),
        time=tail(time(ldp), 1)))
  }
  xts(ret$volatility, order.by = ret$time)
}

