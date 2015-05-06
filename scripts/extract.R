library(Rhipe)
rhinit()
rhoptions(zips = '/user/rhafen/rhipe.tar.gz')
rhoptions(runner = 'sh ./rhipe/library/Rhipe/bin/RhipeMapReduce.sh')

## compute total trades, total size, # inserts, # cancels
## for each exchange / symbol / day combination
##---------------------------------------------------------

a <- rhread("/user/rhafen/tmp_text2", max = 2000)
map.values <- sapply(a, "[[", 2)

map <- expression({
  b <- gsub("\xc0\x80", "", map.values)
  b2 <- strsplit(b, "\t")

  symb_pre <- c("e", "o")
  # symb_pre <- c("p", "z")

  for(ii in seq_along(b2)) {
    if(substr(b2[[ii]][15], 1, 1) %in% symb_pre && length(b2[[ii]]) == 46)
      rhcollect(c(b2[[ii]][15], b2[[ii]][1]), b[ii])
  }
})

reduce <- expression(
  pre = {
    adata <- list()
  }, reduce = {
    adata[[length(adata) + 1]] <- reduce.values
  }, post = {
    res <- read.delim(textConnection(paste(unlist(reduce.values), collapse = "\n")),
      header = FALSE, stringsAsFactors = FALSE)
      # colClasses = c(rep("character", 2), rep("numeric", 5), "character", rep("numeric", 4), "character", "numeric", "character", rep("numeric", 7), "character", rep("integer", 5), rep("numeric", 8), rep("integer", 10)))

    names(res) <- c("sys_date", "sys_time", "sys_tz", "dst_ind", "ndays1883", "dow",
      "doy", "sess_date", "sess_dst_ind", "sess_ndays1883", "sess_dow",
      "sess_doy", "exg_time", "exg_time_tz", "symbol", "list_exg_idx",
      "rept_exg_idx", "sess_id", "td_price_flag", "td_cond_flag", "td_cond_idx",
      "td_vol_type", "td_bate_code", "td_size", "td_exg_seq", "td_rec_back",
      "td_tot_vol", "td_tick_vol", "td_price", "td_price_open", "td_price_high",
      "td_price_low", "td_price_last", "td_tick", "td_price_net_chg",
      "ana_filt_thresh", "ana_filt_bool", "ana_filt_level", "ana_sighilo_type",
      "ana_sighilo_secs", "qt_match_dist_rgn", "qt_match_dist_bbo",
      "qt_match_flag_bbo", "qt_match_flag_rgn", "qt_match_type_bbo",
      "qt_match_type_rgn")
    rhcollect(reduce.key, res)
  }
)

res <- rhwatch(
  input = rhfmt("/SummerCamp2015/nxcore/processed/trade/2014/", type = "text"),
  # input = rhfmt("/user/rhafen/tmp_text2", type = "seq"),
  output = "/user/rhafen/trade_equity_option",
  mapred = list(mapred.reduce.tasks = 100),
  map = map, reduce = reduce,
  readback = FALSE)

# 3.4 hours

a <- rhread("/user/rhafen/trade_equity_option", max = 2)

## group by symbol / day so all options are together
##---------------------------------------------------------

a <- rhread("/user/rhafen/trade_equity_option", max = 2)
map.keys <- lapply(a, "[[", 1)
map.values <- lapply(a, "[[", 2)

map <- expression({
  for(ii in seq_along(map.keys)) {
    if(!grepl("index option", map.keys[[ii]][1])) {
      if(substr(map.keys[[ii]][1], 1, 1) == "e") {
        new_key <- simple_symbol(map.keys[[ii]][1])
      } else {
        new_key <- simple_symbol(map.keys[[ii]][1], type = "o")
      }
      rhcollect(paste(new_key, map.keys[[ii]][2], sep = "_"), map.values[[ii]])
    }
  }
})

reduce <- expression(
  pre = {
    res <- list()
  }, reduce = {
    for(ii in seq_along(reduce.values))
      res[[reduce.values[[ii]]$symbol[1]]] <- reduce.values[[ii]]
  }, post = {
    rhcollect(reduce.key, res)
  }
)

res <- rhwatch(
  input = "/user/rhafen/trade_equity_option",
  output = rhfmt("/user/rhafen/trade_equity_option_grouped", type = "map"),
  mapred = list(mapred.reduce.tasks = 100),
  map = map, reduce = reduce,
  readback = FALSE)

# 5 minutes

a <- rhread("/user/rhafen/trade_equity_option_grouped", type = "map", max = 1)

a <- rhmapfile("/user/rhafen/trade_equity_option_grouped")
b <- a[["GWL_2014-03-25"]]

eo_grp <- ddo(hdfsConn("/user/rhafen/trade_equity_option_grouped", type = "map"))

b <- eo_grp[["GWL_2014-03-25"]]

eo_grp_keys <- sort(unlist(getKeys(eo_grp)))

## get just equities
##---------------------------------------------------------

map <- expression({
  for(ii in seq_along(map.keys)) {
    if(substr(map.keys[[ii]][1], 1, 1) == "e") {
      new_key <- simple_symbol(map.keys[[ii]][1])
      rhcollect(paste(new_key, map.keys[[ii]][2], sep = "_"), map.values[[ii]])
    }
  }
})

res <- rhwatch(
  input = "/user/rhafen/trade_equity_option",
  output = rhfmt("/user/rhafen/trade_equity", type = "map"),
  mapred = list(mapred.reduce.tasks = 100),
  map = map, readback = FALSE)

a <- rhread("/user/rhafen/trade_equity", type = "map", max = 1)

a <- rhmapfile("/user/rhafen/trade_equity")
b <- a[["GWL_2014-03-25"]]

e <- ddo(hdfsConn("/user/rhafen/trade_equity", type = "map"))

b <- e[["GWL_2014-03-25"]]

e_keys <- sort(unlist(getKeys(e)))

rhsave(e_keys, eo_grp_keys, file = "/user/rhafen/trade_keys.Rdata")

## get equities for active symbols only
## and aggregate
##---------------------------------------------------------

a <- rhread("/user/rhafen/trade_equity", max = 2, type = "map")
map.keys <- lapply(a, "[[", 1)
map.values <- lapply(a, "[[", 2)

load("~/Documents/Projects/NxCore/data/trade/active_symbols.Rdata")

setup <- expression({
  suppressMessage(suppressWarnings(library(nxcore)))
  suppressMessage(suppressWarnings(library(dplyr)))
  suppressMessage(suppressWarnings(library(fasttime)))
})

map <- expression({
  for(ii in seq_along(map.keys)) {
    k <- strsplit(map.keys[[ii]], "_")[[1]][1]
    if(k %in% active_symbols$symb) {
      nm <- active_symbols$name[active_symbols$symb == k]

      x <- map.values[[ii]] %>%
        filter(td_cond_idx %in% c(0, 95, 115) & td_size > 0) %>%
        mutate(sys_datetime = fastPOSIXct(paste(sys_date, sys_time)))

      x <- process_insertions(x)
      x <- process_cancellations(x)

      # extract some things before aggregation
      filt <- subset(x, ana_filt_bool == 1)
      filt_times <- unique(trunc(filt$sys_datetime, "secs"))

      # get outliers and mini flash crashes
      outl <- get_singleton_outliers(x)
      # fc <- get_mini_flash_crashes(x)

      # do a little aggregation
      tmp <- x %>%
        mutate(time = as.POSIXct(trunc(sys_datetime, "secs"))) %>%
        group_by(time) %>%
        summarise(
          price = sum(td_price * td_size) / sum(td_size),
          volume = sum(td_size),
          max_price = max(td_price),
          min_price = min(td_price),
          n = n())

      rhcollect(map.keys[[ii]],
        list(data = tmp, outl = outl, filt_times = filt_times, name = nm))
    }
  }
})

res <- rhwatch(
  input = "/user/rhafen/trade_equity_option",
  output = rhfmt("/user/rhafen/trade_equity", type = "map"),
  mapred = list(mapred.reduce.tasks = 100),
  map = map, readback = FALSE)


##
##---------------------------------------------------------

library(datadr)

trade_eo <- ddf(hdfsConn("/user/rhafen/trade_equity_option"))
# trade_eo_summ <- addTransform(trade_eo, function(x)
#   data.frame(n = nrow(x), symbol = x$symbol[1], date = x$sys_date[1]))
# trade_eo_summ <- recombine(trade_eo_summ, combRbind)
# # the rbind would create 150 million row data frame so this is not a good idea

map <- expression({
  for(ii in seq_along(map.values)) {
    map.keys[[i]][1]
  }
})

trade_eo_summ <- addTransform()

## aggregate to second level
##---------------------------------------------------------

# take care of insertions / cancellations
x <- process_insertions(x)
x <- process_cancellations(x)

# extract some things before aggregation
filt <- subset(x, ana_filt_bool == 1)
filt_times <- unique(trunc(filt$sys_datetime, "secs"))

# get outliers and mini flash crashes
outl <- get_singleton_outliers(x)
fc <- get_mini_flash_crashes(x)

# do a little aggregation
tmp <- x %>%
  mutate(time = as.POSIXct(trunc(sys_datetime, "secs"))) %>%
  group_by(time) %>%
  summarise(
    price = sum(td_price * td_size) / sum(td_size),
    volume = sum(td_size),
    max_price = max(td_price),
    min_price = min(td_price),
    n = n())



## merge into subsets by exchange / symbol (combine days)
##---------------------------------------------------------

map <- function(k, v) {
  rhcollect(c(k[2], k[3]), data.frame(date = k[1], t(v), stringsAsFactors = FALSE))
}

reduce <- expression(
  pre = {
    adata <- list()
  }, reduce = {
    adata[[length(adata) + 1]] <- reduce.values
  }, post = {
    adata <- do.call("rbind", unlist(adata, recursive = FALSE))
    names(adata) <- c("date", "n", "n_ins", "n_canc", "tot_size")
    adata$date <- as.Date(adata$date)
    rhcollect(reduce.key, adata[order(adata$date),])
  }
)

res <- rhwatch(
  input = "/user/rhafen/trade_symbol_daily_summary",
  output = "/user/rhafen/trade_symbol_summary",
  mapred = list(mapred.reduce.tasks = 20),
  map = map, reduce = reduce,
  readback = FALSE)

rhread("/user/rhafen/trade_symbol_summary", max = 10)



##
##---------------------------------------------------------

load("~/Documents/Projects/NxCore/data/trade/trade_summ.Rdata")

sum(trade_summ$n_trades)
# 13,780,219,669

sec_type <- substr(trade_summ$symbol, 1, 1)
eqopt <- trade_summ[sec_type %in% c("e", "o"),]
sum(eqopt$n_trades)
# 3,887,525,609
## around 27% is equity or option

head(eqopt[order(eqopt$n_trades, decreasing = TRUE),])

library(rbokeh)
figure() %>%
  ly_quantile(n_trades, data = eqopt) %>%
  y_axis(log = TRUE)


##
##---------------------------------------------------------

# x <- read.delim(textConnection(paste(b, collapse = "\n")),
#   header = FALSE, stringsAsFactors = FALSE)
# names(x) <- names(nxcore_dict$trade)

# sec_type <- substr(x$symbol, 1, 1)
# x <- x[sec_type %in% c("e", "o")]

# # take care of insertions / cancellations
# x <- suppressMessages(process_insertions(x))
# x <- suppressMessages(process_cancellations(x))


# insert_flag <- c(16:31, 48:63, 80:95, 112:127)
# cancel_flag <- c(32:63, 96:127)
