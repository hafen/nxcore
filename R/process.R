#' Take care of insertions in NxCore trade data
#'
#' @param x a data frame for a single day and a single symbol of NxCore trade data, as read by \code{\link{read_nx_trade}}
#' @note The approach is ...
#' @export
process_insertions <- function(x) {
  # get_td_price_flags(0:127)[,5:6]
  # x <- x[order(x$td_exg_seq),] # want to do it without sorting
  ord <- order(x$td_exg_seq)
  insert_flag <- c(16:31, 48:63, 80:95, 112:127)
  idx <- which(x$td_price_flag[ord] %in% insert_flag)
  n_ins <- length(idx)
  if(n_ins > 0) {
    message("inserting ", n_ins, " record", ifelse(n_ins > 1, "s", ""), "...")
    x$sys_datetime[ord][idx] <- x$sys_datetime[ord][idx - x$td_rec_back[ord][idx]]
    x$td_exg_seq[ord][idx] <- x$td_exg_seq[ord][idx - x$td_rec_back[ord][idx]] + 0.5
    x$td_rec_back[ord][idx] <- 0
  }

  x
}

#' Take care of cancellations in NxCore trade data
#'
#' @param x a data frame for a single day and a single symbol of NxCore trade data, as read by \code{\link{read_nx_trade}}
#' @note The approach is ...
#' @export
process_cancellations <- function(x) {
  cancel_flag <- c(32:63, 96:127)
  idx <- which(x$td_price_flag %in% cancel_flag)
  n_canc <- length(idx)
  if(n_canc > 0) {
    message("removing ", n_canc, " record", ifelse(n_canc > 1, "s", ""), "...")
    x <- subset(x, !td_exg_seq %in% x$td_exg_seq[idx])
  }

  x
}


#' Consolidate prices at a specified resolution
#' 
#' @param date transaction date strings
#' @param time transaction time strings
#' @param price a tranaction prices
#' @param size a transaction sizes
#' @param date_format the format for the sys_date column 
#' (default is "\%Y-\%m-\%d").
#' @param time_format the format for the sys_time column 
#' (default is "\%H:\%M:\%S.\%f").  
#' @param on the resolution to consolidate trades. The default is "seconds"
#' and any resolution supported by the xts::endpoints function is valid.
#' @param k along every k-th elements. For example if "seconds" is specified 
#' as the "on" parameter and k=2 then prices are consolidated for every 2 
#' seconds.
#' @return an xts object with (vwapped) price, max price, min price, and volume.
#' @examples
#' # Consolidate TAQ trades...
#' data(aapl_taq)
#' ctp = consolidate_prices(aapl_taq$sys_date, aapl_taq$sys_time, 
#'  aapl_taq$td_price, aapl_taq$td_size, date_format="%Y%m%d", 
#'  time_format="%H:%M:%S")
#' # or FIX trades...
#' data(aapl_fix)
#' cfp = consolidate_prices(aapl_fix$sys_date, aapl_fix$sys_time, 
#'  aapl_fix$td_price, aapl_fix$td_size)
#' @export
consolidate_prices = function(date, time, price, size, 
  date_format = "%Y-%m-%d", time_format="%H:%M:%OS", on="seconds", k=1) {
  date_time = paste(date, time)
  if (sum(duplicated(date_time)) > 0) {
    ds = split(1:length(date_time), date_time)
    x = foreach(s=ds, .combine=rbind) %do% {
      # VWAP the duplicates.
      data.frame(price=sum(price[s]*size[s], na.rm=TRUE)/sum(size[s], na.rm=TRUE), 
                 size=sum(size[s]))
    }
    date_time = names(ds)
    price = x$price
    size = x$size
  }
  date_time = strptime(date_time, paste(date_format, time_format))
  x_ts = xts(cbind(price, size), order.by=date_time)
  ret = period.apply(x_ts, endpoints(x_ts, on=on, k=k),
    FUN = function(x) {
      c(sum(x$price*x$size, na.rm=TRUE)/sum(x$size, na.rm=TRUE),
        max(x$price, na.rm=TRUE),
        min(x$price, na.rm=TRUE),
        sum(x$size, na.rm=TRUE))
    })
  names(ret) = c("price", "max_price", "min_price", "volume")
  ret
}

#' Register price information across a set of stocks.
#'
#' Takes a list of stocks from the output of consolidate prices, 
#' registers them in time, and display them at the specified resolution.
#' @param x the list of stocks with a column "price"
#' @param on the resolution to consolidate trades. The default is "seconds"
#' and any resolution supported by the xts::endpoints function is valid.
#' @param k along every k-th elements. For example if "seconds" is specified 
#' as the "on" parameter and k=2 then prices are consolidated for every 2 
#' seconds.
#' @export
register_prices = function(x, on="seconds", k=1) {
  ts = foreach(ts=x, .combine=cbind) %do% ts[,"price"]
  colnames(ts) = names(x)
  ts = carry_prices_forward(ts)
  ts= period.apply(ts, endpoints(ts, on=on, k=k),
    function(ps) {
      xts(matrix(apply(as.matrix(ps), 2, mean, na.rm=TRUE), nrow=1), 
          order.by=time(ps[1]))
    })
  ts
}

#' Go from TAQ file to registered time series
#' 
#' @param file_name the name of the taq file
#' @param symbols the symbols to use
#' @param on the resolution to consolidate trades. The default is "seconds"
#' and any resolution supported by the xts::endpoints function is valid.
#' @param k along every k-th elements. For example if "seconds" is specified 
#' as the "on" parameter and k=2 then prices are consolidated for every 2 
#' seconds.
#' @export
read_taq = function(file_name, symbols=NULL, on="seconds", k=1) {
  #col_types=c(rep("character",3), rep("numeric", 3), rep("character", 2))
  #dstrsplit(readAsRaw(file_name), sep=",", skip=1, col_types=col_types)

  x = read.csv.raw(file_name, header=FALSE, skip=1)

  names(x) = c("symbol", "date", "time", "price", "size", "corr", "cond", "ex")

  # Filter out the symbols we're not interested in.
  if (!is.null(symbols)) x = x[x$symbol %in% symbols,]

  # Remove bunk trades.
  x = na.omit(x[x$corr == 0,1:5]) #&
#                !(x$cond %in% c("O","Z","B","T","L","G","W","J","K")),1:5])
  x = x[x$size > 0,]

  sym_split = split(1:nrow(x), x$symbol)
  x$time_stamp = paste(x$date, x$time)
  x$date_time=strptime(x$time_stamp, format="%Y%m%d %H:%M:%S", 
                       tz=Sys.timezone())

  sym_split = split(1:nrow(x), x$symbol)
  # Create the consolidated trade data.
  cat("Consolidating trade data\n")

  if (is.null(getDoParName())) registerDoSEQ() 
  x=foreach (sym = names(sym_split), .combine=rbind, .inorder=FALSE) %dopar% {
    registerDoSEQ()
    d = x[sym_split[[sym]],]
    ret = NULL
    if (nrow(d) > 0) {
      ret = as.data.frame(consolidate_prices(d$date, d$time, d$price, d$size,
                               time_format="%H:%M:%S", date_format="%Y%m%d",
                               on="seconds", k=1))
      ret$symbol = sym
    }
    ret
  }

  x$date_time = strptime(rownames(x), "%Y-%m-%d %H:%M:%S")
  # Create the xts matrix of stock values.
  sym_split = split(1:nrow(x), x$symbol)
  prices = foreach(sym_inds=sym_split, .combine=cbind) %dopar% {
    xs = x[sym_inds,]
    xst = xts(xs$price, order.by=xs$date_time)
    xts(xs$price, order.by=xs$date_time)
  }

  colnames(prices) = names(sym_split)
  # Carry prices forward for each column.
  cat("Carrying prices forward.\n")
  prices = carry_prices_forward(prices)
  prices = na.omit(prices)

  prices = period.apply(prices, endpoints(prices, on=on, k=k),
    function(ps) {
      xts(matrix(apply(as.matrix(ps), 2, mean, na.rm=TRUE), nrow=1),
          order.by=time(ps[1]))
    })

  prices
}
