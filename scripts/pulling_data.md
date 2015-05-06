
There are currently two sets of data `trade_equity` and `trade_equity_option_grouped`.  Both come from the trade data.

`trade_equity` is raw equity data partitioned by symbol and date.  Each subset has a string key like "symbol_date" and the value is a data frame of the raw trade data for that symbol and date.

`trade_equity_option_grouped` is raw equity and option data grouped by symbol and date.  Each subset has a key like "symbol_date" and the value is a named list.  Each list element in the value is a data frame of raw equity or option data for the symbol.  There will be one equity data frame list entry and 0 or more option data frame list entries.  The list names provide information about what type of data is contained in the element.

The data reside on the XDATA Cloud HDFS as serialized R object Hadoop mapfiles.  We can easily connect to the data with RHIPE using a modified XDATA VM and extract any subset we want from the data by key.

1. Clone the Vagrant repo and fire up the VM

```bash
# run if vbguest has not been installed
vagrant plugin install vagrant-vbguest

git clone -b tessera https://github.com/hafen/vagrant-xdata/

cd vagrant-xdata

vagrant up

vagrant ssh
```

2. Connect to the vpn

Can do this either with vpnc inside the VM or with your parent OS.

3. Launch R and load RHIPE

```r
library(Rhipe)
rhinit()

# set up mapfiles for extracting from the data
e_mapfile <- rhmapfile("/user/rhafen/trade_equity")
eo_mapfile <- rhmapfile("/user/rhafen/trade_equity_option_grouped")

# get list of all possible keys for each data set
rhload("/user/rhafen/trade_keys.Rdata")
# this will load two objects: e_keys and eo_grp_keys
# you can investigate these keys to see what subsets are available
# to help you decide what to pull out

# get the equity data for the first key
d <- e_mapfile[[e_keys[1]]]

# get the grouped equity option data for the first key
d <- eo_mapfile[[eo_grp_keys[1]]]
```

