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
#' ctp = consolidate_prices(aapl_fix$sys_date, aapl_fix$sys_time, 
#'  aapl_fix$td_price, aapl_fix$td_size, date_format="%Y-%m-%d", 
#'  time_format="%H:%M:%S")
#' # or FIX trades...
#' data(aapl_fix)
#' cfp = consolidate_prices(aapl_fix$sys_date, aapl_fix$sys_time, 
#'  aapl_fix$td_price, aapl_fix$td_size)
#' @export
consolidate_prices = function(date, time, price, size, 
  date_format = "%Y-%m-%d", time_format="%H:%M:%OS", on="seconds", k=1) {
  date_time = paste(date, time)
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

