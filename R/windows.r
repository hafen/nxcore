#' @export
volume_window_gen = function(sorted_times, interval) {
  sorted_times=sorted_times
  interval=interval
  end_ind = max(which(sorted_times <= sorted_times[1] + interval))
  end_inds = end_ind:length(sorted_times)
  start_inds = rep(NA, length(end_inds))
  start_inds[1] = 1
  for (i in 1:length(end_inds)) {
    while ((sorted_times[end_inds[i]]-sorted_times[start_inds[i]]) >=
            as.difftime(interval)){
      start_inds[i] = start_inds[i] + 1
    }
    if (i < length(start_inds)) start_inds[i+1] = start_inds[i] + 1
  }
  ind=1
  nextElem = function() {
    if (ind > length(start_inds)) stop("StopIteration", call. = FALSE)
    ret = start_inds[ind]:end_inds[ind]
    ind <<- ind + 1
    ret
  }
  it = list(nextElem=nextElem)
  class(it) = c("abstractiter", "iter")
  it
}

