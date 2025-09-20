# bring in 057
library(data.table)
library(lubridate)
library(tidyr)
library(stringr)
library(dplyr)
library(ggplot2)
library(arrow)
source("E:/H112057/arrow/arrow_20231023/arrow_helpers.R")

disk_case <- "E:/H112057/"
parquet_file <- paste0(disk_case, "parquet/")
csv_file <- paste0(disk_case, "data/")
rdata_path <- paste0(disk_case, "H112057_chienn/R data/")
csv_path <- paste0(disk_case, "H112057_chienn/CSV/")

filedate <- str_replace(gsub("-","",tstrsplit(Sys.time(), " ", fixed = T, keep = 1L)), "20","")
start_timer <- function() { start_time <<- Sys.time() }
end_timer   <- function() { end_time <<- Sys.time(); print(end_time - start_time) }

options(scipen = 999)
