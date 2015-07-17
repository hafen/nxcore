#' Create a sliding window iterator generator
#' 
#' @param sorted_times a vector of date-times in ascending order.
#' @param interval a time interval. The default is minutes(5).
#' @param skip elements on the interval. 
#' @return an iterator that, when called with nextElem returns the indices
#' in the next window.
#' data(aapl_fix)
#' aapl_fix$dt_string = paste(aapl_fix$sys_date, aapl_fix$sys_time)
#' aapl_fix$dt = strptime(aapl_fix$dt_string, "%Y-%m-%d %H:%M:%OS")
#' it = inclusive_window_gen(aapl_fix$dt)
#' window1 = nextElem(it)
#' aapl_fix$dt[c(window1[1], tail(window1, 1)]
#' window2 = nextElem(it)
#' aapl_fix$dt[c(window2[1], tail(window2, 1)]
#' @export
inclusive_window_gen = function(sorted_times, interval=minutes(5), skip=1) {
  if (sorted_times[2] < sorted_times[1]) 
    stop("volume_window_gen expects times sorted in ascending order.")
  end_ind = max(which(sorted_times <= sorted_times[1] + interval))
  end_inds = end_ind:length(sorted_times)
  start_inds = rep(NA, length(end_inds))
  start_inds[1] = 1
  for (i in 1:length(end_inds)) {
    while (!is.na(sorted_times[start_inds[i]]) &&
           ((sorted_times[end_inds[i]]-sorted_times[start_inds[i]]) <=
            as.difftime(interval, units="secs"))){
      start_inds[i] = start_inds[i] + 1
    }
    if (i < length(start_inds)) start_inds[i+1] = start_inds[i] + 1
  }
  ind=1
  nextEl = function() {
    if (ind > length(start_inds)) stop("StopIteration", call. = FALSE)
    ret = start_inds[ind]:end_inds[ind]
    ind <<- ind + skip
    ret
  }
  randEl = function() {
    rand_ind = sample(1:length(start_inds), 1)
    start_inds[rand_ind]:end_inds[rand_ind]
  }
  saei = function() {
    cbind(start_inds, end_inds)
  }
  it = list(nextElem=nextEl, randElem=randEl,start_and_end_inds=saei )
  class(it) = c("abstractiter", "iter")
  it
}

#' Create a sliding window iterator generator
#' 
#' @param sorted_times a vector of date-times in ascending order.
#' @param interval a time interval. The default is minutes(5).
#' @param skip elements on the interval. 
#' @return an iterator that, when called with nextElem returns the indices
#' in the next window. If there is only one value in the window the last
#' valid index is returned as the first. This allows for carrying prices
#' forward when transactions are sparse in time.
#' data(aapl_fix)
#' aapl_fix$dt_string = paste(aapl_fix$sys_date, aapl_fix$sys_time)
#' aapl_fix$dt = strptime(aapl_fix$dt_string, "%Y-%m-%d %H:%M:%OS")
#' it = carry_forward_window_gen(aapl_fix$dt)
#' window1 = nextElem(it)
#' aapl_fix$dt[c(window1[1], tail(window1, 1)]
#' window2 = nextElem(it)
#' aapl_fix$dt[c(window2[1], tail(window2, 1)]
#' @export
carry_forward_window_gen = function(sorted_times, interval, skip=1) {
  if (sorted_times[2] < sorted_times[1]) 
    stop("carry_forward_window_gen expects times sorted in ascending order.")
  end_inds = 2:(length(sorted_times))
  start_inds = rep(NA, length(end_inds))
  start_inds[1] = 1
  for (i in 2:length(end_inds)) {
    start_inds[i] = start_inds[i-1]

    while (sorted_times[end_inds[i]] - sorted_times[start_inds[i]] <=
           as.difftime(interval, units="secs")) {
      start_inds[i] = start_inds[i] + 1
    }
  }
  ind = 1
  nextEl = function() {
    if (ind > length(start_inds)) stop("StopIteration", call. = FALSE)
    ret = start_inds[ind]:end_inds[ind]
    ind <<- ind + skip
    ret
  }
  randEl = function() {
    rand_ind = sample(1:length(start_inds), 1)
    start_inds[rand_ind]:end_inds[rand_ind]
  }
  it = list(nextElem=nextEl)
  class(it) = c("abstractiter", "iter")
  it
}

