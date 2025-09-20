#' README
#' ======
#'
#' What does this file do?
#' =======================
#' This script build income profile
#'
#'
#' What does this file take?
#' =========================
#' - Raw Data:  ENROL[89-109]
#'
#'
#' What does this file output?
#' ===========================
#' - Income: rdata_path\{filedate}_DATA2012_ANNULINC_WORKPAR.RData







### make info h07====

calculate_mode <- function(x) {
  uniqx <- unique(x)
  uniqx[which.max(tabulate(match(x, uniqx)))]
}

annually <- data.frame()
for(y in 94:109){
  df <- data.frame()
  mon <- sprintf("%02d", 1:12)
  
  for (i in mon) {
    data07 <- open_dataset(paste0(parquet_file,"H_NHI_ENROL",y,i,".parquet"))%>%
      filter(ID %in% data_2012$ID & as.character(ID_STATUS)=="1")  %>%
      select(ID, ID1, ID1_AMT, ID1_UNIT, ID1_IDENT, UNIT_ID, BAN) %>%
      collect()
    setDT(data07)
    
    ident_blacklist <- c("21", "22", "31Q", "31R", "32", "40", "40U", "41", "42", "43V", "43W", "43X", "51", "52", "61", "62", "62S", "62T", "U")
    data07[, work := ID == ID1 & ID1_AMT > 3000 & !ID1_IDENT %in% ident_blacklist ]
    df <- rbind(df, data07)
    message("finish ",y,i)
    gc()
  }
  preg_at_that_year <- first_born_info[preg_year == 1911+y & L_D == 1, ]
  setDT(df)
  df[, unit_ident := paste0(ID1_UNIT, ID1_IDENT)]
  monthly <- df[, ":="(annual_inc = sum(ID1_AMT*work),
                       work_12mon = sum(work),
                       main_BAN = calculate_mode(BAN),
                       main_UNIT_ID = calculate_mode(UNIT_ID),
                       main_unit_ident =  calculate_mode(unit_ident), 
                       IDorID1 = mean(ID==ID1) ), by = .(ID)]
  monthly[, year := as.numeric(y)+1911]
  monthly[, preg := ID %in% preg_at_that_year$ID_M]
  annually <- rbind(annually,monthly) # error??
}
setDT(annually)
save(annually, file=paste0(rdata_path, filedate,"_DATA2012_ANNULINC_WORKPAR.RData"))

data_ <- data_2012[annually, on = .(ID, year)]

