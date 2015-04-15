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
  vwap=period.apply(tx, ep, 
    function(x) sum(x$price*x$size, na.rm=TRUE)/sum(x$size, na.rm=TRUE))
}

#' Get the log-rate-returns of a time-series
#'
#' @param prices the values to calculated the log-rate returns from.
#' @export
log_rate_return = function(prices) {
  log(as.vector(prices[2:length(prices)])) /
    log(as.vector(prices[1:(length(prices)-1)]))
}

#' Carry prices forward
#' 
#' @param x an xts object that may have NA's
#' @return an xts object where NA's are replace by the last valid value. Note
#'         that if NA's appear in the beginning of the series they will appear
#'         in the result.
#' @export
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

