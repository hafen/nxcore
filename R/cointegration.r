
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
    if (is.null(colnames(x))) colnames(x) = paste("V", 1:ncol(x), sep="")
    cs = combn(colnames(x), 2)
    if (is.null(getDoParName())) registerDoSEQ()
    r=foreach(it=isplitCols(cs,chunks=getDoParWorkers()),.combine=`+`) %do% {
      # Create the sparse matrix for this process.
      m = Matrix(data=0, nrow=ncol(x), ncol=ncol(x), sparse=TRUE,
                 dimnames=list(names(x), names(x)))
      for (j in 1:ncol(it)) {
        # Fit the cointegration.
        p_val = 1
        y_vec = as.matrix(x)[,it[1,j],drop=FALSE]
        x_vec = cbind(as.matrix(x)[,it[2,j],drop=FALSE], rep(1, nrow(y_vec)))
        # Get the summed differences for y and x. If either is a vector
        # of constant value, handle it.
        tol = 1e-8
        x_sum_diff = sum(diff(x_vec[,1]))
        y_sum_diff = sum(diff(y_vec))
        if (abs(x_sum_diff) > tol && abs(y_sum_diff) > tol ) {
          # Neither vector is constant. Test for cointegration.
          qr_x = Matrix::qr(x_vec)
          if (qr_x$rank < 2) {
            x_vec = x_vec[, 1, drop=FALSE]
            qr_x = Matrix::qr(x_vec)
          }
          qr_solve = qr.solve(x_vec, y_vec)
          qr_coef = qr.coef(qr_x, y_vec)
          resids = na.omit(y_vec - x_vec %*% qr_coef)
          if (sum(resids^2) < 1e-6) {
            warning("Residuals are zero. Returning NA.")
            pval = NA
          } else {
            p_val = as.vector(unitrootTest(resids, type="nc")@test$p.value[1])
          }
        } 
        m[it[1,j], it[2,j]] = p_val
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
#' @param dr the dimension reduction technique: "none", "svd", "nmf"
#' @param dr_rank if dr is "svd" or "nmf" then this specifies the rank of the subspace
#' to project onto.
#' @export
coint_measure = function(x, interval=minutes(5), dr=c("none", "svd", "nmf"), 
                         dr_rank) {
  if (!(dr[1] %in% c("none", "svd", "nmf"))) 
    stop("Unknown dimension reduction type")
  if (dr != "none" && missing(dr_rank)) 
    stop("You must specify a rank for dimension reduction")

  dr_fun = switch(dr[1],
    "none" = function(x) x,
    "svd" = function(x) {
      x_mat = as.matrix(x)
      x_ret = x_mat %*% svd(x_mat)$v[,1:dr_rank]
      xts(x_ret, order.by=time(x))
    },
    "nmf" = function(x) {
      x_mat = as.matrix(x)
      x_ret = tcrossprod(x_mat, nmf(x_mat, rank=dr_rank)@fit@H)
      xts(x_ret, order.by=time(x))
    })

  ci_info=foreach(it=inclusive_window_gen(time(x), interval=interval)) %dopar% {
    a = dr_fun(x[it,])
    ci = cointegration_info(a)
    xts(ci$p_stat, order.by=time(x)[tail(it, 1)]) 
  }
  ret = Reduce(rbind, ci_info)
  indexTZ(ret) = Sys.getenv("TZ")
  colnames(ret) = "p_stat"
  ret
}
