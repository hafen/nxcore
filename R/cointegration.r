
#' Get the pairwise matrix of p-values testing for cointegration 
#'
#' @param x a data.frame, matrix, or xts object
#' @return An upper triangular matrix where each entry corresponds to the result
#'         of McKinnons's test assuming no intercept or time trend.
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
        fit=lm(as.formula(paste(it[1,j], "~", it[2,j], "+0")), data=x)
        val=as.vector(unitrootTest(fit$residuals, type="nc")@test$p.value[1])
        m[it[1,j], it[2,j]] = val
      }
      m
    }    
    r[lower.tri(r)]=0
  }
  r
}     

#' Get cointegration information for time series data
#'
#' @param x a data.frame, matrix, or xts object
#' @return a list containting the pairwise cointegration p-values, the 
#'         p-value of the Kolmogorov-Smirnov test for uniformity in the p-values
#'         (null is uniformity), and a measure of the "cointegratedness" of
#'         the time series vectors (100 indicates all are cointegrated zero
#'         indicates they are all independent).
#' @export
cointegration_info= function(x) {
  ps = cointegration_p_matrix(x)
  ret = NA
  if (!is.na(ps)) {
    p_vals = suppressMessages(ps[upper.tri(ps)])
    ret = list(p_matrix=ps, 
         p_value=ks.test(p_vals, punif)$p.value,
         p_stat=100*sum(1-p_vals)/length(p_vals))
  }
}

