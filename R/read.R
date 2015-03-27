#' Turn XDATA-processed NxCore "trade" text file into R data frame
#'
#' @note This presumes the input is a single file
#' @export
#' @importFrom fasttime fastPOSIXct
read_nx_trade <- function(file) {
  x <- read.delim(file, header = FALSE, stringsAsFactors = FALSE)
  # assign names for the variables
  names(x) <- names(nxcore_dict$trade)
  # make date POSIXct (note: R is dumb and does some strange things with subsecond digits)
  x$sys_datetime <- fastPOSIXct(paste(x$sys_date, x$sys_time))
  # order by symbol, sys time, and exg_seq
  x[order(x$symbol, x$sys_datetime, x$td_exg_seq),]
}
