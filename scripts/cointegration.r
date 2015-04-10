library(quantmod)

library(foreach)
library(itertools)
library(Matrix)
library(fUnitRoots)

log_rate_return = function(prices) {
  log(as.vector(prices[2:length(prices)])) / 
    log(as.vector(prices[1:(length(prices)-1)]))
}

# Takes an xts object and returns the p-values for the pairwise 
# cointegrations.
cointegration_p_matrix = function(x) {
  x = na.omit(x)
  cs = combn(colnames(x), 2)
  # parallelize this for bigger matrices.
  if (is.null(getDoParName())) registerDoSEQ()
  foreach(it=isplitCols(cs, chunks=getDoParWorkers()), .combine=`+`) %dopar% {
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
}

cointegrate = function(x) {
  ps = cointegration_p_matrix(x)
  ks.test(suppressMessages(ps[upper.tri(ps)]), punif) 
  list(p_matrix=cointegration_p_matrix(x)
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

y = foreach(j=1:ncol(x), .combine=cbind) %do% {
  log_rate_return(x[,j])
}
y = as.data.frame(y)
names(y) = names(x)
