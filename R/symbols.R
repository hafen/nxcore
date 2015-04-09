#' @export
get_clean_symbol <- function(x) {
  prefix <- substr(x, 1, 1)
  x <- gsub("^[befiopz]", "", x)
  x <- gsub(",\\(non_opt\\)", "", x)

  idx <- which(prefix %in% c("z", "f", "p"))
  if(length(idx) > 0)
    x[idx] <- sapply(strsplit(x[idx], "\\."), "[[", 1)

  idx <- which(prefix == "o")
  if(length(idx) > 0)
    x[idx] <- sapply(strsplit(x[idx], ","), "[[", 1)

  x
}


#' @export
get_symbol_names <- function(x, exg = NULL) {
  message("cleaning up symbol...")
  prefix <- substr(x, 1, 1)
  x <- gsub("^[befiopz]", "", x)
  x <- gsub(",\\(non_opt\\)", "", x)

  res <- rep(NA, length(x))

  message("bonds...")
  idx <- which(prefix == "b")
  if(length(idx) > 0)
    res[idx] <- get_names_b(x[idx], exg[idx])

  message("currencies...")
  idx <- which(prefix == "c")
  if(length(idx) > 0)
    res[idx] <- get_names_c(x[idx], exg[idx])

  message("mutual funds...")
  idx <- which(prefix == "m")
  if(length(idx) > 0)
    res[idx] <- get_names_m(x[idx], exg[idx])

  message("equities...")
  idx <- which(prefix == "e")
  if(length(idx) > 0)
    res[idx] <- get_names_e(x[idx], exg[idx])

  message("indices...")
  idx <- which(prefix == "i")
  if(length(idx) > 0)
    res[idx] <- get_names_i(x[idx], exg[idx])

  message("spreads...")
  idx <- which(prefix == "z")
  if(length(idx) > 0)
    res[idx] <- get_names_z(x[idx], exg[idx])

  message("futures...")
  idx <- which(prefix == "f")
  if(length(idx) > 0)
    res[idx] <- get_names_f(x[idx], exg[idx])

  message("options...")
  idx <- which(prefix == "o")
  if(length(idx) > 0)
    res[idx] <- get_names_o(x[idx], exg[idx])

  message("future options...")
  idx <- which(prefix == "p")
  if(length(idx) > 0)
    res[idx] <- get_names_p(x[idx], exg[idx])

  res
}

## internal
##---------------------------------------------------------

month_code <- c("F","G","H","J","K","M","N","Q","U","V","X","Z")
month_val <- c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

simple_lookup <- function(x, ltab) {
  ltab$name[match(x, ltab$symbol)]
}

lookup_w_exg <- function(x, exg, ltab, ltabu, name = "") {

  res <- ltabu$name[match(x, ltabu$symbol)]

  if(is.null(exg)) {
    message(name, " symbols are uniquely defined by exchange and symbol and exchange was not specified... symbols that are not unique across all exchanges will not be matched")
  } else {
    idx <- which(is.na(res))
    res[idx] <- ltab$name[match(paste(exg[idx], x[idx]), paste(ltab$exg, ltab$symbol))]
  }
  res
}

get_names_z <- function(x, exg = NULL) {
  tmp <- strsplit(x, "\\.|_")
  symb_base <- sapply(tmp, "[[", 1)
  name_base <- symb_z$name_base[match(paste(exg, symb_base), paste(symb_z$exg, symb_z$symbol_base))]
  first <- sapply(tmp, "[[", 2)
  second <- sapply(tmp, "[[", 3)

  to_time <- function(a, first = TRUE) {
    res <- rep("", length(a))
    idx <- which(nchar(a) == 3 & grepl("^[GFXVQNJKUHZM]", a))

    yr <- substr(a[idx], 2, 3)
    yri <- suppressWarnings(as.integer(yr))
    yr[!is.na(yri)] <- paste("'", yr[!is.na(yri)], sep = "")
    yr[is.na(yri)] <- ""

    res[idx] <- paste(" ",
      month_val[match(substr(a[idx], 1, 1), month_code)],
      yr, ifelse(first, " ", ""), sep = "")

    # "0_3" is "Spot vs. 3mo"
    res[a == "0"] <- " Spot "
    res[a == "3"] <- " 3mo"

    res
  }

  new <- paste(name_base, to_time(first), "vs.", to_time(second, FALSE), sep = "")
  gsub("^ +", "", new)
}


get_names_c <- function(x, exg = NULL) {
  rep("", length(x))
}

get_names_f <- function(x, exg = NULL) {
  tmp <- strsplit(x, "\\.")
  symb_base <- sapply(tmp, "[[", 1)
  name_base <- symb_f$name_base[match(paste(exg, symb_base), paste(symb_f$exg, symb_f$symbol_base))]

  suffix <- sapply(tmp, function(a) {
    if(length(a) == 1) {
      return("")
    } else {
      return(a[2])
    }
  })

  nc <- nchar(suffix)

  res <- rep("", length(suffix))
  idx <- which(nc == 3 & grepl("^[GFXVQNJKUHZM]", suffix))

  if(length(idx) > 0) {
    res[idx] <- paste(
      month_val[match(substr(suffix[idx], 1, 1), month_code)],
      "'", substr(suffix[idx], 2, 3), sep = "")
  }

  idx <- which(nc > 0 & nc < 3)
  if(length(idx) > 0) {
    res[idx] <- paste(suffix[idx], "mo", sep = "")
    res[idx][res[idx] == "0mo"] <- "Spot"
  }

  res <- paste(name_base, res)
  res <- gsub(" +$", "", res)

  # symb_f$res <- res
  # idx <- which(symb_f$res != symb_f$name)

  res
}

get_names_p <- function(x, exg = NULL) {
  tmp <- strsplit(x, "\\.")
  symb_base <- sapply(tmp, "[[", 1)
  name_base <- symb_p$name_base[match(paste(exg, symb_base), paste(symb_p$exg, symb_p$symbol_base))]

  a <- sapply(tmp, "[[", 2)
  b <- sapply(tmp, "[[", 3)
  tm <- paste(
    month_val[match(substr(a, 1, 1), month_code)],
    "'", substr(a, 2, 3), sep = "")

  res <- paste(name_base, tm, gsub("([0-9]+)", "\\1 ", b))

  # symb_p$res <- res
  # idx <- which(symb_p$res != symb_p$name)

  res
}

get_names_e <- function(x, exg = NULL) {
  lookup_w_exg(x, exg, symb_e, symb_e_unq, "equity")
}

get_names_i <- function(x, exg = NULL) {
  lookup_w_exg(x, exg, symb_i, symb_i_unq, "index")
}

get_names_b <- function(x, exg = NULL) {
  simple_lookup(x, symb_b)
}

get_names_m <- function(x, exg = NULL) {
  simple_lookup(x, symb_m)
}

get_names_o <- function(x, exg = NULL) {
  symb <- gsub("([A-Z0-9 ]+),.*", "\\1", x)
  # need to get right exg for the symbol
  exg <- symb_o$exg[match(symb, symb_o$symbol)]
  res <- rep("", length(x))

  idx <- symb_o$prefix == "e"
  if(length(which(idx)) > 0)
    res[idx] <- paste(get_names_e(symb[idx], exg[idx]), "(equity option)")

  if(length(which(!idx)) > 0)
    res[!idx] <- paste(get_names_i(symb[!idx], exg[!idx]), "(index option)")

  res
}


