# some initial RHIPE tests
# run on vagrant vm connected to cluster

library(Rhipe)
rhinit()
rhoptions(zips = '/user/rhafen/rhipe.tar.gz')
rhoptions(runner = 'sh ./rhipe/library/Rhipe/bin/RhipeMapReduce.sh')

a <- rhread("/SummerCamp2015/nxcore/processed/trade/2014/01/20140101-r-00091.bz2", type = "text", max = 5)

map <- expression({
  rhcollect(map.keys[[1]], map.values[[1]])
})

rhwatch(
  input = rhfmt("/SummerCamp2015/nxcore/processed/trade/2014/01/20140101-r-00091.bz2", type = "text"),
  output = "/user/hafen/tmp", map = map, readback = FALSE)

a <- rhread("/user/hafen/tmp")

# good news: RHIPE can read bzip files with no problem
# bad news: strings with null character are truncated
