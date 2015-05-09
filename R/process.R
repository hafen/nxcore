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

#' Consolidate prices at a single second resolution
#' 
#' @param x a data frame with columns named including sys_date, sys_time, 
#' td_price, td_size.
#' @return an xts matrix with (vwapped) price, max price, min price, and volume.
#' @examples
#' # Consolidate TAQ trades...
#' data(aapl_taq)
#' ctp = consolidate_prices(aapl_taq)
#' # or FIX trades...
#' data(aapl_fix)
#' cfp = consolidate_prices(aapl_fix)
#' @export
consolidate_prices = function(x) {
  time_split = split(1:nrow(x), x$sys_time)
  # Consolidate on second. Use the mean prices as the price.
  if (is.null(getDoParName())) registerDoSEQ()
  x = foreach(inds=time_split, .combine=rbind) %dopar% {
    xs = x[inds,]
    data.frame(price=sum(xs$price*xs$td_size)/sum(xs$td_size),
               max_price=max(xs$td_price),
               min_price=min(xs$td_price), volume=sum(xs$td_size), 
               date=xs$sys_date[1], time=xs$sys_time[1])
  }
  x$date_time = paste(x$sys_date, x$sys_time)
  x$date_time = strptime(x$date_time, "%Y%m%d %H:%M:%S")
  x = x[order(x$date_time),]
  x
}

