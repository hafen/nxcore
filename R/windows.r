#' @export
volume_window_gen = function(sorted_times, interval, skip=1) {
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
  it = list(nextElem=nextEl)
  class(it) = c("abstractiter", "iter")
  it
}

#' @export
proforma_window_gen = function(sorted_times, interval, skip=1) {
  end_inds = 2:(length(sorted_times))
  start_inds = rep(NA, length(end_inds))
  start_inds[1] = 1
  for (i in 2:length(end_inds)) {
    start_inds[i] = start_inds[i-1]

    while (sorted_times[end_inds[i]] - sorted_times[start_inds[i]] >
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
  it = list(nextElem=nextEl)
  class(it) = c("abstractiter", "iter")
  it
}

