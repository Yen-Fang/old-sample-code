#' README
#' ======
#'
#' What does this file do?
#' =======================
#' This script build the medical usage incl. visit and points
#'
#'
#' What does this file take?
#' =========================
#' - Raw Data: OPDTE[89-109]
#'
#'
#' What does this file output?
#' ===========================
#' - Medical Usage: rdata_path/{filedate}_reg_file_finish_raw_no_sp.RData



### make visit dot ====
load(paste0(rdata_path,filedate,"_data2010_id.RData"))
load(paste0(rdata_path, filedate,"_DATA2012_ANNULINC_WORKPAR.RData"))


data_ <- annually[data_2012, on = .(ID)]
# annually[, cnt_check := .N, by = "ID"]
data_[, cnt_check := .N, by = "ID"]
data_  <- data_[cnt_check==16 ]
##### mental ====
gc()

icd <- fread(paste0(csv_path, "icd.csv"))
icd <- icd[  grepl("657" ,ccs) | 
               grepl("657" ,ccs_label)|
               grepl("670" ,ccs) | 
               grepl("670" ,ccs_label)|
               grepl("650" ,ccs) | 
               grepl("650" ,ccs_label)|
               grepl("651" ,ccs) | 
               grepl("651" ,ccs_label)|
               grepl("658" ,ccs) | 
               grepl("658" ,ccs_label)|
               grepl("mental disorders", eng)|
               grepl("Postpartum mood disturbance", eng)
]
icd[, icd9_ :=as.character(icd9)]
icd[, icd9_ := as.numeric(gsub("([.])", "", as.character(icd9_)))]
icd[, icd10_ := gsub("([.])", "", as.character(icd10))]
#load(paste0(rdata_path,"240619_data2010_id.RData"))

annually_mood <- data.table()

for(y in 94:109){#94:109
  df <- data.table()
  mon <- sprintf("%02d", 1:12)
  
  for (i in mon) {
    if(y<=104){
      data01 <- open_dataset(paste0(parquet_file,"H_NHI_OPDTE",y,i,"_10.parquet"))%>% 
        filter(ID %in% data_$ID & (ICD9CM_1 %in% icd$icd9_ | ICD9CM_2 %in% icd$icd9_ | ICD9CM_3 %in% icd$icd9_))  %>% 
        select(ID, FUNC_DATE, T_DOT, ICD9CM_1 ,ICD9CM_2,ICD9CM_3) %>% 
        collect()
    }else{
      data01 <- open_dataset(paste0(parquet_file, "H_NHI_OPDTE",y,i,"_10.parquet"))%>% 
        filter(ID %in% data_$ID & (ICD9CM_1 %in% icd$icd10_ | ICD9CM_2 %in% icd$icd10_ | ICD9CM_3 %in% icd$icd10_))  %>% # 
        select(ID, FUNC_DATE, T_DOT, ICD9CM_1 ,ICD9CM_2,ICD9CM_3) %>% 
        collect()
    }
    setDT(data01)
    
    df <- rbind(df, data01)
    message("finish ",y,i)
    gc()
  }
  
  monthly <- df[, .(visit_cnt_mood = uniqueN(FUNC_DATE), point_cnt_mood = sum(T_DOT)), by = "ID"]
  monthly[, year := as.numeric(y+1911)]
  
  annually_mood <- rbind(annually_mood, monthly)
}
save(annually_mood, file=paste0(rdata_path,filedate,"_annually_mood_94_109_visit_cnt_point_mental_general.RData"))

##### all ====
annually_h01 <- list()

for(y in 94:109){
  df <- data.table()
  mon <- sprintf("%02d", 1:12)
  
  for (i in mon) {
    data01 <- open_dataset(paste0(parquet_file,"H_NHI_OPDTE",y,i,"_10.parquet"))%>% 
      filter(ID %in% data_$ID) %>%
      select(ID, FUNC_DATE, T_DOT) %>% 
      collect()
    setDT(data01)
    data01 <- data01[, .(visit_cnt = uniqueN(FUNC_DATE), point_cnt = sum(T_DOT)), by = "ID"]
    
    df <- rbind(df, data01)
    message("finish ",y,i)
    gc()
  }
  
  monthly <- df[, .(visit_cnt = sum(visit_cnt), point_cnt = sum(point_cnt)), by = "ID"]
  monthly[, year := as.numeric(y+1911)]
  
  annually_h01[[y-93]] <-monthly
  rm(monthly)
}
annually_h01 <- rbindlist(annually_h01)


save(annually_h01, file=paste0(rdata_path,filedate,"_annually_h01_94_109_visit_cnt_point.RData"))


load(paste0(rdata_path,"240619_annually_mood_94_109_visit_cnt_point_mental_general.RData"))
data_ <- merge(data_, annually_h01, by = c("ID", "year"), all.x = T)
data_ <- merge(data_, annually_mood, by = c("ID", "year"), all.x = T)

data_[is.na(visit_cnt), visit_cnt := 0][is.na(point_cnt), point_cnt := 0][is.na(visit_cnt_mood), visit_cnt_mood := 0][is.na(point_cnt_mood), point_cnt_mood := 0]
colnames(data_)
save(data_, file=paste0(rdata_path,filedate,"_reg_file_finish_raw_no_sp.RData"))
