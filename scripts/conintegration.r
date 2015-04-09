library(urca)

library(quantmod)
data(nasdaq_index_funds)
adj_closes = foreach(symbol = nasdaq_index_funds$symbol, .combine=cbind) %do% {
  stock_lookup_env = new.env()
  ret = try(getSymbols(paste("^", symbol, sep=""), env=stock_lookup_env))
  if (inherits(ret, "try-error"))
    ret = NULL
  else 
    ret = stock_lookup_env[[ret]][,6]
  ret
}

names(adj_closes) = gsub(".Adjusted", "", names(adj_closes))

adj_closes = foreach(symbol=ls(data_env), .combine=cbind) %do% {
  data_env[[symbol]][,6]  
}
names(adj_closes) = ls(data_env)
adj_closes = na.omit(adj.closes)
ca.jo(as.data.frame(adj_closes),type="trace",K=2,ecdet="none", spec="longrun")


cointegrate = function(x, type = c("eigen", "trace"), 
                       ecdet = c("none", "const", "trend"),   
                       K = 2, spec = c("longrun", "transitory"), season = NULL, 
                       dumvar = NULL)
  # See if we need to reduce the number of columns.
  m = as.matrix(na.omit(x))
  ch = suppressWarnings(chol(crossprod(m), pivot=TRUE))
  if (attributes(ch)$rank < ncol(m)) {
    # It's rank deficient.
    x = x[,attributes(ch)$pivot[1:attributes(ch)$rank]]
  }
  ca.jo(x, type, ecdet, K, spec, season, dumvar)
}
