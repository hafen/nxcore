
#' Convert decimal to matrix of bits
#'
#' @param x decimal or vector of decimal integers
#' @param n number of bits
#' @examples
#' get_bit_matrix(1:10)
#' get_bit_matrix(1:10, n = 4)
#' @export
get_bit_matrix <- function(x, n = 32) {
  if(ceiling(log2(max(x) + 1)) > n)
    warning("some values of 'x' require more bits to be represented than specified by 'n'")
  matrix(as.integer(intToBits(x)), ncol = 32, byrow = TRUE)[,1:n,drop=FALSE]
}

#' Convert decimal NxCore price flags to bit representation
#'
#' @param x vector of decimal price flag values
#' @examples
#' get_td_price_flags(1)
#' @export
get_td_price_flags <- function(x) {
  res <- data.frame(get_bit_matrix(x, 7))
  names(res) <- c("NxTPF_SETLAST", "NxTPF_SETHIGH", "NxTPF_SETLOW", "NxTPF_SETOPEN", "NxTPF_EXGINSERT", "NxTPF_EXGCANCEL", "NxTPF_SETTLEMENT")
  res
}

#' Convert decimal NxCore trade condition flags to bit representation
#'
#' @param x vector of decimal trade condition flag values
#' @examples
#' get_td_cond_flags(1)
#' @export
get_td_cond_flags <- function(x) {
  res <- data.frame(get_bit_matrix(x, 3))
  names(res) <- c("NxTCF_NOLAST", "NxTCF_NOHIGH", "NxTCF_NOLOW")
  res
}


# a1 <- system.time(b1 <- matrix(as.integer(intToBits(1:50000)), ncol = 32, byrow = TRUE)[,n:1])
# a2 <- system.time(b2 <- do.call(rbind, lapply(1:50000, function(x) as.integer(intToBits(x))[n:1])))

# a2 / a1
