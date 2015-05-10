#' Calculated the VWAP for a specified interval
#'
#' @param price the product's tranaction price.
#' @param size the number of products transacted.
#' @param date_time the date-times for the tranactions
#' @param on the interval to find the average over.
#' @param k along every k elements.
#' @return an xts object with the volume-weighted average price for the
#'         specified interval of time.
#' @examples
#' data(aapl_fix)
#' aapl_fix$dt_string = paste(aapl_fix$sys_date, aapl_fix$sys_time)
#' aapl_fix$dt = strptime(aapl_fix$dt_string, "%Y-%m-%d %H:%M:%OS")
#' # Minute-aggregated vwap prices.
#' minute_prices = vwap(aapl_fix$td_price, aapl_fix$td_size, aapl_fix$dt)
#' @export
vwap = function(price, size, date_time, on="minutes", k=1) {
  tx = xts(cbind(price, size), order.by=date_time, unique=FALSE)
  ep = endpoints(date_time, on=on, k=k)
  vwap=period.apply(tx, ep, 
    function(x) sum(x$price*x$size, na.rm=TRUE)/sum(x$size, na.rm=TRUE))
}

log_rate_return = function(prices) {
  log(as.vector(prices[2:length(prices)])) /
    log(as.vector(prices[1:(length(prices)-1)]))
}

#' Get the log-rate-returns of a time-series
#'
#' @param prices the values to calculated the log-rate returns from.
#' @examples
# Get the cointegration information for the log-rate returns.
#' data(aapl_fix)
#' data(a_fix)
#' aapl_cons = consolidate_prices(aapl_fix$sys_date, aapl_fix$sys_time, 
#'  aapl_fix$td_price, aapl_fix$td_size)
#' a_cons = consolidate_prices(a_fix$sys_date, a_fix$sys_time, 
#'  a_fix$td_price, a_fix$td_size)
#' trades = merge(aapl_cons$price, a_cons$price)
#' trades = carry_prices_forward(trades)
#' trade_llr = log_rate_returns(trades) 
#' names(trades_lrr) = c("aapl", "a")
#' cointegration_p_matrix(trades_lrr)
#' @export
log_rate_returns = function(x, na.omit=TRUE) {
  if (is.null(getDoParName())) registerDoSEQ()
  ret = foreach(1:ncol(x), .combine=cbind) %dopar%  log_rate_return(x[,j])
  if (na.omit) ret = na.omit(ret)
  ret
}

#' Carry price forward
#' 
#' @param x an xts object that may have NA's
#' @return an xts object where NA's are replace by the last valid value. Note
#'         that if NA's appear in the beginning of the series they will appear
#'         in the result.
carry_price_forward = function(x) {
  vec=as.vector(x)
  is_na = is.na(vec)
  fgi = min(which(!is_na))
  if (!is.finite(fgi)) {
    warning("All values are NA.")
    return(x)
  }
  na_inds = which(is_na)
  na_inds = na_inds[na_inds > fgi]
  for (ind in na_inds) {
    vec[ind] = vec[ind-1]
  }
  xts(vec, order.by=time(x))
}

#' Carry prices forward
#' 
#' @param x an xtx object that may have mulitple columns and NA's
#' @param na.omit should the rows with NA's be omitted after carrying forward?
#' @return an xts object where NA's are replace by the last valid value. Note
#'         that if NA's appear in the beginning of the series they will appear
#'         in the result.
#' @examples
#' data(aapl_fix)
#' data(a_fix)
#' aapl_cons = consolidate_prices(aapl_fix$sys_date, aapl_fix$sys_time, 
#'  aapl_fix$td_price, aapl_fix$td_size)
#' a_cons = consolidate_prices(a_fix$sys_date, a_fix$sys_time, 
#'  a_fix$td_price, a_fix$td_size)
#' trades = merge(aapl_cons$price, a_cons$price)
#' trades = carry_prices_forward(trades)
#' names(trades) = c("aapl", "a")
#' cointegration_p_matrix(trades)
#' @export
carry_prices_forward = function(x, na.omit=TRUE) {
  if (is.null(getDoParName())) registerDoSEQ()
  ret = foreach(1:ncol(x), .combine=cbind) %dopar%  carry_price_foward(x[,j])
  if (na.omit) ret = na.omit(ret)
  ret
}
