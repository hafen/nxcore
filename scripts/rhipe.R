# some initial RHIPE tests
# run on vagrant vm connected to cluster

# to run from os x
# HADOOP_CONF_DIR=vagrant-xdata/hadoop/

library(Rhipe)
rhinit()

# # one time only (or every time you update packages)
# setwd("~/")
# hdfs.setwd("/user/rhafen")
# bashRhipeArchive("rhipe")

# now in all future sessions, put this boilerplate up front
rhoptions(zips = '/user/rhafen/rhipe.tar.gz')
rhoptions(runner = 'sh ./rhipe/library/Rhipe/bin/RhipeMapReduce.sh')

a <- rhread("/SummerCamp2015/nxcore/processed/trade/2014/01/20140101-r-00091.bz2", type = "text", max = 5)

map <- expression({
  rhcollect(map.keys[[1]], map.values[[1]])
})

res <- rhwatch(
  input = rhfmt("/SummerCamp2015/nxcore/processed/trade/2014/01/20140101-r-00091.bz2", type = "text"),
  output = "/user/rhafen/tmp_text", map = map, readback = FALSE, mapred = list(mapreduce.job.maps = "100"))

a <- rhread("/user/rhafen/tmp_text")
map.values <- sapply(a, "[[", 2)

## system memory usage
##---------------------------------------------------------

map <- expression({
  rhcollect(map.keys[[1]], system("free -t -m", intern = TRUE))
})

res <- rhwatch(
  input = rhfmt("/SummerCamp2015/nxcore/processed/trade/2014/01/20140101-r-00091.bz2", type = "text"),
  output = "/user/rhafen/tmp_mem", map = map, reduce = 0, readback = FALSE)

a <- rhread("/user/rhafen/tmp_mem")


##
##---------------------------------------------------------


# map <- expression({
#   b <- gsub("\xc0\x80", "", map.values)
#   b <- strsplit(b, "\t")
#   dt <- b[[1]][8]
#   symbols <- table(sapply(b, function(x) x[15]))
#   sn <- names(symbols)
#   for(ii in seq_along(symbols)) {
#     rhcollect(c(dt, sn[[ii]]), as.numeric(symbols[[ii]]))
#   }
# })

##
##---------------------------------------------------------

# http://beadooper.com/?p=165
# https://support.pivotal.io/hc/en-us/articles/201462036-Mapreduce-YARN-Memory-Parameters
# http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.0.9.1/bk_installing_manually_book/content/rpm-chap1-11.html
# yarn.scheduler.minimum-allocation-mb is 21 GB

rhread("/user/rhafen/tmp_count", max = 4)


##
##---------------------------------------------------------

library(datadr)

b <- read.delim(textConnection(paste(map.values, collapse = "\n")), header = FALSE, na.strings = "\xc0\x80", col.names = c("sys_date", "sys_time", "sys_tz", "dst_ind", "ndays1883", "dow", "doy", "sess_date", "sess_dst_ind", "sess_ndays1883", "sess_dow", "sess_doy", "exg_time", "exg_time_tz", "symbol", "list_exg_idx", "rept_exg_idx", "sess_id", "td_price_flag", "td_cond_flag", "td_cond_idx", "td_vol_type", "td_bate_code", "td_size", "td_exg_seq", "td_rec_back", "td_tot_vol", "td_tick_vol", "td_price", "td_price_open", "td_price_high", "td_price_low", "td_price_last", "td_tick", "td_price_net_chg", "ana_filt_thresh", "ana_filt_bool", "ana_filt_level", "ana_sighilo_type", "ana_sighilo_secs", "qt_match_dist_rgn", "qt_match_dist_bbo", "qt_match_flag_bbo", "qt_match_flag_rgn", "qt_match_type_bbo", "qt_match_type_rgn"))

rhmkdir("/user/rhafen/test_input")
rhcp("/SummerCamp2015/nxcore/processed/trade/2014/01/20140101-r-00091.bz2", "/user/rhafen/test_input/")

hdfs.setwd("/user/rhafen")
# rhdel("test")
x <- drRead.delim(hdfsConn("/SummerCamp2015/nxcore/processed/trade/2014/01/", type = "text"),
  na.strings = "\xc0\x80",
  output = hdfsConn("test"),
  header = FALSE, stringsAsFactors = FALSE,
  control = list(mapred = list(mapred.reduce.tasks = 30)))

rhdel("/SummerCamp2015/nxcore/processed/trade/2014/01/_meta")

  # control = list(mapred = list(mapreduce.input.fileinputformat.split.maxsize = as.integer(1024*1024*64))), overwrite = TRUE)


  control = list(mapred = list(mapreduce.job.maps = "100")))

# http://www.cloudera.com/content/cloudera/en/documentation/core/v5-2-x/topics/cdh_ig_mapreduce_to_yarn_migrate.html
# yarn.nodemanager.resource.cpu-vcores
