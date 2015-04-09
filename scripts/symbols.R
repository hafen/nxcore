

get_symbol_csv <- function(x) {
  res <- read.csv(paste("_ignore/symbols/", x, ".csv", sep = ""),
    header = FALSE, stringsAsFactors = FALSE)
  names(res) <- c("exg", "prefix", "symbol", "name", "activity", "lastquote", "lasttrade")
  res
}

symb_c <- get_symbol_csv("c")
# this has no labels...

symb_b <- get_symbol_csv("b")
nrow(symb_b)
length(unique(symb_b$symbol))
# symbols are unique

symb_b <- symb_b[,c("exg", "symbol", "name")]
save(symb_b, file = "data/symb_b.rda")

symb_i <- get_symbol_csv("i")
nrow(symb_i)
length(unique(symb_i$symbol))
length(unique(paste(symb_i$exg, symb_i$symbol)))
# indices are uniquely defined by symbol and exchange

sort(table(symb_i$symbol))
subset(symb_i, symbol == "VWRL.IV")

symb_i <- symb_i[,c("exg", "symbol", "name")]
save(symb_i, file = "data/symb_i.rda")

load("data/symb_i.rda")

tb <- table(symb_i$symbol)
symb_i_unq <- subset(symb_i, symbol %in% names(tb[tb == 1]))

save(symb_i_unq, file = "data/symb_i_unq.rda")





symb_e <- get_symbol_csv("e")
nrow(symb_e)
length(unique(paste(symb_e$exg, symb_e$symbol)))
# equities are uniquely defined by symbol and exchange

aa <- get_names_e(x[idx], exg[idx])
bb <- get_names_e(x[idx])
length(which(is.na(aa)))
length(which(is.na(bb)))

idx2 <- which(is.na(aa))
trade_summ[,c("symbol", "exg")][idx,][idx2,]
x[idx][idx2][4]
exg[idx][idx2][4]

which(x[idx][idx2] %in% symb_e$symbol)

subset(symb_e, exg == "PACF" & symbol == "SPY")
subset(symb_e, symbol == "SPY")

# fix AMD
symb_e$exg[symb_e$symbol == "AMD" & symb_e$exg == "NQSC"] <- "NYSE"

symb_e <- symb_e[,c("exg", "symbol", "name")]
save(symb_e, file = "data/symb_e.rda")


load("data/symb_e.rda")

tb <- table(symb_e$symbol)
symb_e_unq <- subset(symb_e, symbol %in% names(tb[tb == 1]))

save(symb_e_unq, file = "data/symb_e_unq.rda")






symb_m <- get_symbol_csv("m")
nrow(symb_m)
length(unique(symb_m$symbol))
# symbols are unique (all have same exchange)

symb_m <- symb_m[,c("exg", "symbol", "name")]
save(symb_m, file = "data/symb_m.rda")

## spread needs lots of work
## there are a handful of base symbols
## everything on top of that are timing details
##---------------------------------------------------------

symb_z <- get_symbol_csv("z")
nrow(symb_z)
length(unique(symb_z$symbol))
table(symb_z$exg)
symb_z %>% filter(symbol %in% duplicated(symbol)) %>% arrange(symbol)
subset(symb_z, symbol %in% symbol[duplicated(symbol)]) %>% arrange(symbol)

tmp <- table(gsub(".*\\.([FGHJKMNQUVXZ]).*", "\\1", symb_z$symbol))

wat <- names(tmp)[nchar(names(tmp)) > 1]

symb_z[grepl(wat[1], symb_z$symbol),]

symb_z[!grepl("[FGHJKMNQUVXZ][0-9][0-9]_[FGHJKMNQUVXZ][0-9][0-9]", symb_z$symbol),]

tmp <- strsplit(symb_z$symbol, "\\.|_")

symb_z$symbol_base <- sapply(tmp, "[[", 1)

symb_z$name_base <- gsub("Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|vs\\.|('[0-9][0-9])|(Spot vs\\. 3mo)|('1U)", "", symb_z$name)
symb_z$name_base <- gsub(" +$", "", symb_z$name_base)

symb_z[grepl("0_3", symb_z$symbol),]

symb_z[sapply(tmp, length) > 3,-c(1:2, 5:7)]

symb_z <- symb_z[,c("exg", "symbol_base", "name_base")]
symb_z <- symb_z[!duplicated(symb_z),]

save(symb_z, file = "data/symb_z.rda")

##
##---------------------------------------------------------

symb_f <- get_symbol_csv("f")
nrow(symb_f)
length(unique(symb_f$symbol))


subset(symb_f, symbol == "MP.U15")
tmp <- symb_f[!duplicated(paste(symb_f$exg, symb_f$symbol)),]

symb_f[!grepl("\\.[FGHJKMNQUVXZ][0-9][0-9]", symb_f$symbol),]

tmp <- strsplit(symb_f$symbol, "\\.")

table(sapply(tmp, length))

symb_f[sapply(tmp, function(x) length(x) == 1),]
# these have no name

symb_f[sapply(tmp, function(x) length(x) == 3),]
# ignore the third element on these

# if the second element is a number, then it is "#mo" or if it is zero, it is "Spot"
# if it's "Y$$" then don't do anything for the suffix

symb_f$symbol_base <- sapply(tmp, "[[", 1)

symb_f$name_base <- gsub("Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|vs\\.|('[0-9][0-9])|Spot|([0-9]+mo)|('1U)", "", symb_f$name)
symb_f$name_base <- gsub(" +$", "", symb_f$name_base)

symb_f[!grepl("\\.[FGHJKMNQUVXZ][0-9][0-9]", symb_f$symbol),c("symbol", "name", "symbol_base", "name_base")]

table(gsub("(.*)\\.([FGHJKMNQUVXZ])([0-9][0-9])(.*)", "\\2", symb_f$symbol))
table(gsub(".*\\.([FGHJKMNQUVXZ])[0-9][0-9].*", "\\1", symb_f$symbol))
symb_f[grepl("^CBD", symb_f$symbol),]
symb_f[grepl("\\$", symb_f$symbol),]
symb_f[grepl("MAL.15", symb_f$symbol),]
symb_f[grepl("MAL0-MAL", symb_f$symbol),]

x <- symb_f$symbol
exg <- symb_f$exg

symb_f <- symb_f[,c("exg", "symbol_base", "name_base")]
symb_f <- symb_f[!duplicated(symb_f),]

save(symb_f, file = "data/symb_f.rda")

## o
##---------------------------------------------------------

symb_o <- get_symbol_csv("o")

gsub("(.*) [0-9]+\\.[0-9]+ [CP]", "\\1", head(symb_o$name))

x <- subset(trade_summ, type == "(o) Eq/Idx Opt Root")$symbol

tmp <- strsplit(symb_o$name, ":| ")
table(sapply(tmp, length))
head(symb_o[which(sapply(tmp, length) == 6),])

exgs <- sapply(tmp, "[[", 1)
nms <- sapply(tmp, "[[", 2)
pre <- substr(nms, 1, 1)
nms <- gsub("^[ei]", "", nms)

# # instead of messing with actual symbols, get it from name as above
# tmp <- strsplit(symb_o$symbol, ",")
# nms <- sapply(tmp, "[[", 1)
# nms <- gsub(" +$", "", nms)
# nc <- nchar(nms)
# table(substr(nms, nc, nc))
# nms <- gsub(" [A-Z]$", "", nms)
# nms <- gsub(" +$", "", nms)

tmp <- data.frame(exgs = exgs, nms = nms)
tmp <- tmp[!duplicated(tmp),]

subset(tmp, nms == "FB")
nrow(tmp)
length(unique(tmp$nms))

symb_o$exg <- exgs
symb_o$symbol <- nms
symb_o$prefix <- pre

symb_o <- symb_o[,c("exg", "symbol", "prefix")]
symb_o <- symb_o[!duplicated(symb_o),]
rownames(symb_o) <- NULL

save(symb_o, file = "data/symb_o.rda")



# note: the exchange for every option symbol is OPRA
# instead we replace it with the exchange on which to look up the option symbol





x2 <- gsub("^o", "", x2)
x3 <- unique(x2)

which(!x3 %in% symb_e$symbol)

x3[31] %in% symb_i$symbol

length(c(symb_i$symbol, symb_e$symbol))
length(unique(c(symb_i$symbol, symb_e$symbol)))

aa <- c(symb_i$symbol, symb_e$symbol)
aa[duplicated(aa)]

length(c(paste(symb_i$exg, symb_i$symbol), paste(symb_e$exg, symb_e$symbol)))
length(unique(c(paste(symb_i$exg, symb_i$symbol), paste(symb_e$exg, symb_e$symbol))))

aa <- c(paste(symb_i$exg, symb_i$symbol), paste(symb_e$exg, symb_e$symbol))
aa[duplicated(aa)]

# if you match options on both exchange and symbol, the only conflict is IBLN
# which is IBILLIONAIRE INDEX in both cases (equity and index)
# and we don't see it in the trade data

# so rule of thumb is to match first on equity, and match remaining on index


## p
##---------------------------------------------------------

symb_p <- get_symbol_csv("p")

head(subset(trade_summ, type == "(p) Future Option"))

tmp <- strsplit(symb_p$symbol, "\\.")

table(sapply(tmp, length))
# all are 3 - that makes it easy

table(sapply(tmp, "[[", 2))
# all second parts are easy

symb_p$symbol_base <- sapply(tmp, "[[", 1)

symb_p$name_base <- gsub(" (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'([0-9][0-9]) ([0-9]+ )([CP]|-[CP])", "", symb_p$name)

idx <- which(!grepl("[CP]$", symb_p$name))

x <- symb_p$symbol
exg <- symb_p$exg

symb_p <- symb_p[,c("exg", "symbol_base", "name_base")]
symb_p <- symb_p[!duplicated(symb_p),]

save(symb_p, file = "data/symb_p.rda")


##
##---------------------------------------------------------

# x <- symb_z$symbol
# exg <- symb_z$exg




