# here's an example to read and aggregate a day's worth of data for one security


# in bash:
# bzgrep eRAD, -a 20140410-r-00052.bz2 | tr -d '\000' > RAD20140410.txt

library(nxcore)

x <- read.delim("RAD20140410_2.txt", header = FALSE, stringsAsFactors = FALSE)
names(x) <- names(nxcore_dict$trade)

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
