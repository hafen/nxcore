% Generated by roxygen2 (4.1.1): do not edit by hand
% Please edit documentation in R/cointegration.r
\name{coint_measure}
\alias{coint_measure}
\title{Get the cointegration measure for a rolling windows of stocks.}
\usage{
coint_measure(x, interval = minutes(5), dr = c("none", "svd", "nmf"),
  dr_rank)
}
\arguments{
\item{x}{an xts object of stock prices.}

\item{interval}{the width of the cointegration window.}

\item{dr}{the dimension reduction technique: "none", "svd", "nmf"}

\item{dr_rank}{if dr is "svd" or "nmf" then this specifies the rank of the subspace
to project onto.}
}
\description{
Get the cointegration measure for a rolling windows of stocks.
}

