#' Calculated the VWAP for a specified interval
#'
#' @param price the product's tranaction price.
#' @param size the number of products transacted.
#' @param on the interval to find the average over.
#' @param k along every k elements.
#' @return an xts object with the volume-weighted average price for the
#'         specified interval of time.
#' @export
vwap = function(price, size, date_time, on="minutes", k=1) {
  tx = xts(cbind(price, size), order.by=date_time, unique=FALSE)
  ep = endpoints(date_time, on=on, k=k)
  vwap=period.apply(tx, ep, function(x) sum(x$price*x$size)/sum(x$size))
}

#' Get the log-rate-returns of a time-series
#'
#' @param prices the values to calculated the log-rate returns from.
#' @export
log_rate_return = function(prices) {
  log(as.vector(prices[2:length(prices)])) /
    log(as.vector(prices[1:(length(prices)-1)]))
}

