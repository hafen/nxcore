
#' Get the pairwise matrix of p-values testing for cointegration 
#'
#' @param x a data.frame, matrix, or xts object
#' @return An upper triangular matrix where each entry corresponds to the 
#' p-value from McKinnons's test assuming no time trend. Note that if one
#' stock in a pair-wise cointegration is constant the return value will be 1.
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
cointegration_p_matrix = function(x) {
  r = NA
  x = na.omit(x)
  if (nrow(x) > 0) {
    cs = combn(colnames(x), 2)
    if (is.null(getDoParName())) registerDoSEQ()
    r=foreach(it=isplitCols(cs,chunks=getDoParWorkers()),.combine=`+`) %dopar% {
      # Create the sparse matrix for this process.
      m = Matrix(data=0, nrow=ncol(x), ncol=ncol(x), sparse=TRUE,
                 dimnames=list(names(x), names(x)))
      for (j in 1:ncol(it)) {
        # Fit the cointegration.
        val = try({
          p_val = 1
          y_vec = as.matrix(x)[,it[1,j],drop=FALSE]
          x_vec = cbind(as.matrix(x)[,it[2,j],drop=FALSE], rep(1, nrow(y_vec)))

          # Get the summed differences for y and x. If either is a vector
          # of constant value, handle it.
          x_sum_diff = sum(diff(x_vec[,1]))
          y_sum_diff = sum(diff(y_vec))
          if (x_sum_diff != 0 && y_sum_diff != 0) {
            # Neither vector is constant. Test for cointegration.
            qr_x = Matrix::qr(x_vec)
            if (qr_x$rank < 2) {
              x_vec = x_vec[, 1, drop=FALSE]
              qr_x = Matrix::qr(x_vec)
            }
            qr_solve = qr.solve(x_vec, y_vec)
            qr_coef = qr.coef(qr_x, y_vec)
            resids = na.omit(y_vec - x_vec %*% qr_coef)
            p_val = as.vector(unitrootTest(resids, type="nc")@test$p.value[1])
          } else {
            # At least one vector is constant. Print a warning.
            if (y_sum_diff == 0 && x_sum_diff == 0) {
              warning(paste(it[1,j], "and", it[2,j], "are constant."))
            } else if (y_sum_diff == 0) {
              warning(paste(it[1,j], "is constant."))
            } else {
              warning(paste(it[2,j], "is constant."))
            }
          }
          p_val
        })
        # Check for other problems. 
        if (inherits(val, "try-error")) {
          warning(paste("Problem at", it[1,j], it[2,j]))
          val = NA
        }
        m[it[1,j], it[2,j]] = val
      }
      m
    }
    r[lower.tri(r)]=0
  }
  r
}

#' Get cointegration information for time series data.
#'
#' @param x a data.frame, matrix, or xts object
#' @return a list containting the pairwise cointegration p-values, the 
#'         p-value of the Kolmogorov-Smirnov test for uniformity in the p-values
#'         (null is uniformity), and a measure of the "cointegratedness" of
#'         the time series vectors (100 indicates all are cointegrated zero
#'         indicates they are all independent).
#' @examples
#' data(aapl_fix)
#' data(a_fix)
#' aapl_cons = consolidate_prices(aapl_fix$sys_date, aapl_fix$sys_time, 
#'  aapl_fix$td_price, aapl_fix$td_size)
#' a_cons = consolidate_prices(a_fix$sys_date, a_fix$sys_time, 
#'  a_fix$td_price, a_fix$td_size)
#' # Make the times for AAPL overlap A. 
#' time(aapl_cons) = time(a_cons)[1:nrow(aapl_cons)]
#' trades = merge(aapl_cons$price, a_cons$price)
#' names(trades) = c("aapl", "a")
#' cointegration_info(trades)
#' @export
cointegration_info= function(x) {
  ps = cointegration_p_matrix(x)
  ret = NA
  if (inherits(ps, "Matrix")) {
    p_vals = suppressMessages(ps[upper.tri(ps)])
    ret = list(p_matrix=p_vals, 
         p_stat=100*sum(1-p_vals)/length(p_vals))
  }
  ret
}

#' Get the cointegration measure for a rolling windows of stocks.
#'
#' @param x an xts object of stock prices.
#' @param interval the width of the cointegration window.
#' @export
coint_measure = function(x, interval=minutes(5)) {
  ci_info=try({
    foreach(it=inclusive_window_gen(time(x), interval=interval)
            ) %do% {
      ret = try({
        ci = cointegration_info(x[it,])
        xts(ci$p_stat, order.by=time(x)[tail(it, 1)]) })
      if (inherits(ret, "try-error")) {
        print(it)
        ret = NULL
      }
      ret
    }
  })
  ret = Reduce(rbind, ci_info)
  indexTZ(ret) = Sys.getenv("TZ")
  colnames(ret) = "p_stat"
  ret
}
