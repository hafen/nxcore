library(Rhipe)
rhinit()
rhoptions(zips = '/user/rhafen/rhipe.tar.gz')
rhoptions(runner = 'sh ./rhipe/library/Rhipe/bin/RhipeMapReduce.sh')

## compute total trades, total size, # inserts, # cancels
## for each exchange / symbol / day combination
##---------------------------------------------------------

map <- expression({
  b <- gsub("\xc0\x80", "", map.values)
  b <- strsplit(b, "\t")
  insert_flag <- c(16:31, 48:63, 80:95, 112:127)
  cancel_flag <- c(32:63, 96:127)

  lapply(b, function(x) {
    if(length(x) == 46)
      rhcollect(c(x[8], x[16], x[15]),
        matrix(c(
          as.numeric(1),
          as.numeric(x[19] %in% insert_flag),
          as.numeric(x[19] %in% cancel_flag),
          as.numeric(x[24])
        ), nrow = 1)
      )
  })
})

res <- rhwatch(
  input = rhfmt("/SummerCamp2015/nxcore/processed/trade/2014/", type = "text"),
  output = "/user/rhafen/trade_symbol_daily_summary",
  mapred = list(mapred.reduce.tasks = 100),
  map = map,
  reduce = rhoptions()$templates$colsummer,
  readback = FALSE)

# something's not right with the cluster - this takes way too long

rhread("/user/rhafen/trade_symbol_daily_summary", max = 10)

rhwatch(
  map = function(k, v) rhcollect("1", v[1]),
  reduce = rhoptions()$templates$scalarsummer,
  input = "/user/rhafen/trade_symbol_daily_summary"
)

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

## make into a data frame and merge into a
## small number (26) of key-value pairs (distributed rbind)
##---------------------------------------------------------

map <- function(k, v) {
  rhcollect(sample(letters, 1),
    data.frame(
      exg = k[1],
      symbol = k[2],
      n_trades = sum(v$n),
      n_days = nrow(v),
      n_cancels = sum(v$n_canc),
      n_inserts = sum(v$n_ins),
      tot_size = sum(v$tot_size)))
}

res <- rhwatch(
  input = "/user/rhafen/trade_symbol_summary",
  output = "/user/rhafen/trade_summary",
  map = map, reduce = rhoptions()$templates$rbinder(),
  mapred = list(mapred.reduce.tasks = 26),
  readback = FALSE)

rhls("/user/rhafen/trade_summary")
trade_summ <- rhread("/user/rhafen/trade_summary")

trade_summ <- do.call(rbind, lapply(trade_summ, "[[", 2))
nrow(trade_summ)
trade_summ <- trade_summ[order(trade_summ$n_trades, decreasing = TRUE),]

trade_summ$symbol <- as.character(trade_summ$symbol)
trade_summ$exg_idx <- as.integer(as.character(trade_summ$exg))

exg_name <- c("NQEX", "NQAD", "NYSE", "AMEX", "CBOE", "ISEX", "PACF", "CINC", "PHIL", "OPRA", "BOST", "NQNM", "NQSC", "NQBB", "NQPK", "NQAG", "CHIC", "TSE", "CDNX", "CME", "NYBT", "NYBA", "COMX", "CBOT", "NYMX", "KCBT", "MGEX", "WCE", "ONEC", "DOWJ", "GEMI", "SIMX", "FTSE", "EURX", "ENXT", "DTN", "LMT", "LME", "IPEX", "MX", "WSE", "C2", "MIAX", "CLRP", "BARK", "TEN4", "NQBX", "HOTS", "EUUS", "EUEU", "ENCM", "ENID", "ENIR", "CFE", "PBOT", "HWTB", "NQNX", "BTRF", "NTRF", "BATS", "NYLF", "PINK", "BATY", "EDGE", "EDGX", "RUSL", "ISLD")

trade_summ$exg <- exg_name[trade_summ$exg_idx]

x <- trade_summ$symbol
exg <- trade_summ$exg
trade_summ$name <- get_symbol_names(trade_summ$symbol, trade_summ$exg)

# length(which(is.na(trade_summ$name)))

types <- c("(o) Eq/Idx Opt Root", "(p) Future Option", "(e) Equity", "(m) Mutual Fund", "(z) Spreads", "(f) Future", "(i) Index", "(b) Bond", "(c) Currency/Spot")
names(types) <- c("o", "p", "e", "m", "z", "f", "i", "b", "c")

ss <- substr(trade_summ$symbol, 1, 1)
trade_summ$type <- types[ss]

# write.csv(trade_summ, file = "/mount/team6/trade_summ.csv", row.names = FALSE)
save(trade_summ, file = "~/Documents/Projects/NxCore/data/trade/trade_summ.Rdata")

load("~/Documents/Projects/NxCore/data/trade/trade_summ.Rdata")

## subset the daily by symbol data to symbols of interest
##---------------------------------------------------------

map <- function(k, v) {
  if(nrow(v) > 100 & sum(v$n) / nrow(v) > 20)
    rhcollect(k, v)
}

res <- rhwatch(
  input = "/user/rhafen/trade_symbol_summary",
  output = "/user/rhafen/trade_symbol_summary_sub",
  map = map, reduce = 0,
  readback = FALSE)

##
##---------------------------------------------------------

# symb_summ <- ddf(hdfsConn("/user/rhafen/trade_symbol_summary_sub"))
# symb_summ <- convert(symb_summ, localDiskConn("~/Documents/Projects/NxCore/data/trade/symb_summ", nBins = 20))

library(datadr)
library(trelliscope)
library(parallel)
library(nxcore)

vdbConn("~/Documents/Projects/NxCore/vdb")

types <- c("(o) Eq/Idx Opt Root", "(p) Future Option", "(e) Equity", "(m) Mutual Fund", "(z) Spreads", "(f) Future", "(i) Index", "(b) Bond", "(c) Currency/Spot")
names(types) <- c("o", "p", "e", "m", "z", "f", "i", "b", "c")

options(defaultLocalDiskControl = localDiskControl(makeCluster(4)))
symb_summ <- ddf(localDiskConn("~/Documents/Projects/NxCore/data/trade/symb_summ"))

a <- symb_summ[["07/0b78d1165e469eb48666be0dc9560cc8.Rdata"]]
# a <- symb_summ[[1]]
k <- a$key
v <- a$value

panelFn <- function(k, v) {
  url <- paste("http://www.google.com/search?q=", get_clean_symbol(k[2]), "+",
    format(v$date, "%m/%d/%Y"), sep = "")
  p <- figure(width = 700, height = 400) %>%
    ly_points(date, n, data = v, size = 8,
      hover = date,
      url = url)
  list(k, p)
}

cogFn <- function(k, v) {
  exg <- exg_lookup$code[as.integer(k[1])]
  list(k,
  list(
    symbol = cog(k[2]),
    clean_symbol = cog(get_clean_symbol(k[2])),
    exchange = cog(exg),
    type = cog(types[substr(k[2], 1, 1)]),
    name = cog(get_symbol_names(k[2], exg)),
    lmdtr = cog(log10(mean(v$n))),
    lmdsz = cog(log10(mean(v$tot_size))),
    pct_canc = cog(sum(v$n_canc) / sum(v$n) * 100),
    pct_ins = cog(sum(v$n_ins) / sum(v$n) * 100)
  ))
}

# http://www.marketwatch.com/investing/stock/AAPL

makeDisplay(symb_summ, name = "test",
  panelFn = panelFn, cogFn = cogFn,
  width = 700, height = 400,
  state = list(labels = c("symbol", "exchange", "type", "name")))

panelFn <- function(k, v) {
  p <- figure(width = 700, height = 400) %>%
    ly_points(date, n_canc / n, data = v, size = 8,
      hover = list(date, n, n_canc, n_ins))
  list(k, p)
}

cogFn <- function(k, v) {
  exg <- exg_lookup$code[as.integer(k[1])]
  tmp <- subset(v, date == "2014-02-14")
  feb14pct <- NA
  if(nrow(tmp) == 1)
    feb14pct <- tmp$n_canc / tmp$n

  list(k,
  list(
    symbol = cog(k[2]),
    clean_symbol = cog(get_clean_symbol(k[2])),
    exchange = cog(exg),
    type = cog(types[substr(k[2], 1, 1)]),
    name = cog(get_symbol_names(k[2], exg)),
    feb14pct = cog(feb14pct) * 100,
    sig_canc_days = cog(length(which(v$n_canc / v$n > 0.0001)))
  ))
}

system.time(cogFn(k, v))
system.time(kvApply(cogFn, symb_summ[[1]]))

makeDisplay(symb_summ, name = "test2",
  panelFn = panelFn, cogFn = cogFn,
  width = 700, height = 400,
  state = list(labels = c("symbol", "exchange", "type", "name")))


# state doesn't work
# make updateDisplay
# fix cog problem
# fix k/v return problem

