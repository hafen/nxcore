#' @export
get_singleton_outliers <- function(x, thresh = 15, k = 3) {
  # expects a day of data that has insertions and cancellations removed
  if(any(x$td_rec_back != 0))
    warning("Expecting all insertions and cancellations taken care of... results may not be accurate...")

  # make sure data is in order and subset to trading hours
  idx <- order(x$sys_datetime, x$td_exg_seq)
  idx <- idx[which(x$sys_datetime[idx] > as.POSIXct("2014-04-10 09:30", tz = "UTC") & x$sys_datetime[idx] < as.POSIXct("2014-04-10 16:00", tz = "UTC"))]
  rmd <- runmed(x$td_price[idx], k)
  rr <- x$td_price[idx] - rmd
  # plot(x$sys_datetime[idx], rr)
  # abline(h = c(-1, 1) * 15 * mad(rr[rr != 0]))
  oidx <- which(abs(rr) > 15 * mad(rr[rr != 0]))
  if(length(idx) == 0) {
    NULL
  } else {
    data.frame(time = x$sys_datetime[idx][oidx],
      price = x$td_price[idx][oidx],
      cond_idx = x$td_cond_idx[idx][oidx],
      dev = rr[oidx],
      pct_dev = rr[oidx] / rmd[oidx] * 100)
  }
}

#' @export
get_mini_flash_crashes <- function(x) {
  # expects a day of data that has insertions and cancellations removed
  if(any(x$td_rec_back != 0))
    warning("Expecting all insertions and cancellations taken care of... results may not be accurate...")

  # make sure data is in order and subset to trading hours
  idx <- order(x$sys_datetime, x$td_exg_seq)
  idx <- idx[which(x$sys_datetime[idx] > as.POSIXct("2014-04-10 09:30", tz = "UTC") & x$sys_datetime[idx] < as.POSIXct("2014-04-10 16:00", tz = "UTC"))]

  # identical(x$td_exg_seq[idx], sort(x$td_exg_seq[idx]))
  # identical(x$sys_datetime[idx], sort(x$sys_datetime[idx]))

  # pp <- runmed(x$td_price[idx], 3) # smooth it out a bit?
  pp <- x$td_price[idx]
  tt <- x$sys_datetime[idx]

  get_flash <- function(down = TRUE, pp, tt) {
    nn <- length(pp)

    # build a sequence that is
    # zero when non-increasing (when down = TRUE)
    # or zero when non-decreasing (when down = FALSE)
    # and 1 otherwise
    dd <- rep(1, nn)
    if(down) {
      dd[which(pp[2:nn] - pp[1:(nn - 1)] <= 0) + 1] <- 0
    } else {
      dd[which(pp[2:nn] - pp[1:(nn - 1)] >= 0) + 1] <- 0
    }

    # pad the sequence
    dd2 <- c(-pi, dd, pi)
    # find where it changes to/from zero
    jump_ind <- which(abs(diff(dd2)) > 0)
    # these are the indices where it is jumping to zero
    repeat_zero <- which(dd2[jump_ind] == 0)
    # repeat_tab <- table(jump_ind[repeat_zero] - jump_ind[repeat_zero - 1])

    # where the zero (non-decreasing or non-increasing) sequences start and end
    start_idx <- jump_ind[repeat_zero - 1]
    end_idx <- jump_ind[repeat_zero] - 1
    repeat_lengths <- end_idx - start_idx + 1

    # only want sequences with at least 10 values
    rp_idx <- which(repeat_lengths >= 10)
    st <- start_idx[rp_idx]
    nd <- end_idx[rp_idx]

    # plot(sort((pp[nd] - pp[st]) / pp[st]) * 100)

    # compute the change in price and time for these sequences
    pct_chg <- abs((pp[nd] - pp[st]) / pp[st]) * 100
    time_chg <- as.numeric(tt[nd]) - as.numeric(tt[st])
    # plot(pct_chg, time_chg)
    # dd[st[1]:nd[1]]
    # pp[st[1]:nd[1]]
    # plot(pp[(st[cand[1]] - 1):(nd[cand[1]] + 1)])

    fc <- which(pct_chg > 0.8 & time_chg < 1.5)

    if(length(fc) == 0) {
      NULL
    } else {
      data.frame(start = tt[st[fc]], duration = time_chg[fc], pct_chg = pct_chg[fc])
    }
  }

  rbind(
    get_flash(TRUE, pp, tt),
    get_flash(FALSE, pp, tt))
}
