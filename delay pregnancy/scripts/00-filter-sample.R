#' README
#' ======
#'
#' What does this file do?
#' =======================
#' This script creates the require sample
#'
#'
#' What does this file take?
#' =========================
#' - Raw Data: OPDTE[89-109], ENROL[99], BIRTH[90-109]
#'
#'
#'
#' What does this file output?
#' ===========================
#' - Bitrh Record: rdata_path/{filedate}_first_born_info.RData
#' - Misc Record: rdata_path/{filedate}_annually_{yyyy}_abortion_record.RData
#' - Valid Sample: rdata_path/{filedate}_data2010_id.RData
#' 
#' 
#' 

# ==== birth info ====
first_born_info <- list()
for(y in 90:109){
  first_born_info[[y-88]] <- fread(paste0(csv_file, "H_BHP_BIRTH",y,".csv")) %>%
    select("ID_M", "SEX", "BIRTH_YM", "L_D", "WEEK", "DEL_NO")
}
first_born_info <- rbindlist(first_born_info)
setDT(first_born_info)
first_born_info[, preg_day := ymd(ym(as.numeric(BIRTH_YM))-weeks(WEEK))][, perg_year := year(preg_day)]
save(first_born_info, file = paste0(rdata_path,filedate,"_first_born_info.RData"))


# ==== misc info ====
icd <- fread(paste0(csv_path, "icd.csv"))

icd[, icd9_ :=as.character(icd9)]
icd <- icd[grepl(paste(as.character(c(630:639, "V22.2")), collapse = "|") ,icd9_)]
icd[, icd9_ := as.numeric(gsub("([.])", "", as.character(icd9_)))]
icd[, icd10_ := gsub("([.])", "", as.character(icd10))]

for(y in 89:109){
  dta_634_639 <- data.table()
  mon <- sprintf("%02d", 1:12)
  
  for (i in mon) {
    if(y<=104){
      data01 <- open_dataset(paste0(parquet_file,"H_NHI_OPDTE",y,i,"_10.parquet"))%>% 
        filter((ICD9CM_1 %in% icd$icd9_ | ICD9CM_2 %in% icd$icd9_ | ICD9CM_3 %in% icd$icd9_))  %>% 
        select(ID, FUNC_DATE, T_DOT, ICD9CM_1 ,ICD9CM_2,ICD9CM_3) %>% 
        collect()
    }else{
      data01 <- open_dataset(paste0(parquet_file,"H_NHI_OPDTE",y,i,"_10.parquet"))%>% 
        filter((ICD9CM_1 %in% icd$icd10_ | ICD9CM_2 %in% icd$icd10_ | ICD9CM_3 %in% icd$icd10_))  %>% # 
        select(ID, FUNC_DATE, T_DOT, ICD9CM_1 ,ICD9CM_2,ICD9CM_3) %>% 
        collect()
    }
    setDT(data01)
    dta_634_639 <- rbind(dta_634_639, data01)
    message("finish ",y,i)
    gc()
  }
  save(dta_634_639, file = paste0(rdata_path,filedate,"_annually_",y+1911,"_abortion_record.RData"))
  
}



# ==== filter valid id ====

abt_record <- function(){
  eliminate_list2 <- list()
  for (y in 89:98){
    load(paste0(rdata_path,filedate,"_annually_",y+1911,"_abortion_record.RData"))
    eliminate_list2[[y-88]] <- dta_634_639[, .(ID) ]
  }
  TMP <- rbindlist(eliminate_list2) %>% select(ID)
  return(TMP)
}

eliminate_list_abt <- abt_record()

load(paste0(rdata_path,filedate,"_first_born_info.RData"))
first_born_info[,preg_year:= perg_year ]
eliminate_list_born <- first_born_info[preg_year<2010, .(ID_M)]

### make group ====

f_name <- paste0(parquet_file, "H_NHI_ENROL9901.parquet")
group_o <- open_dataset(f_name) %>% 
  select("ID", "ID_S", "ID_BIRTHDAY", ID_STATUS)%>%
  filter(ID_BIRTHDAY%in% c(19800100:19851232)&
           ID_S==2 & ID_STATUS==1 & 
           !ID %in% eliminate_list_born$ID_M  &
           !ID %in% eliminate_list_abt$ID ) %>% collect()

setDT(group_o)
group_o[, age:= 99+1911 - year(ymd(ID_BIRTHDAY))]
group_o[, event := 'O'][, .(ID, event, ID_BIRTHDAY)] -> group_o
data_2012 <- group_o
data_2012[, event_year := 2010]

data_ <- data_2012
tmp <- data_
gc()

for (y in 2010:2015){
  
  # ==== filter a1 from o
  eliminate_list_born_c <- first_born_info[preg_year == y, ]
  group_a1 <- eliminate_list_born_c[ID_M %in% data_2012[event == 'O']$ID & L_D == 1, .(ID_M, preg_day)]
  setnames(group_a1, c("ID", "p_start"))
  setorder(group_a1, ID, p_start)
  group_a1 <- unique(group_a1, by=c("ID"))
  
  # ==== filter a2 from o
  load(paste0(rdata_path,filedate,"_annually_",y,"_abortion_record.RData"))
  group_a2 <- dta_634_639[ID %in% data_2012[event == 'O']$ID , ] #& !ID %in% eliminate_list_born[L_D==1,]$ID_M
  setorder(group_a2, ID, -FUNC_DATE)
  group_a2 <- unique(group_a2, by=c("ID"))
  
  # adjust 2010 whose a2 event is before a1 (eliminate who has icd during pregnant period)
  dup_a2_before_a1 <- group_a2[group_a1, on = "ID", nomatch = 0][ymd(FUNC_DATE) < ymd(p_start)-weeks(6)]
  print(uniqueN(dup_a2_before_a1$ID))
  dup_a2_within_a1 <- group_a2[group_a1, on = "ID", nomatch = 0][ymd(FUNC_DATE) >= ymd(p_start)-weeks(6)]
  
  group_a1 <- group_a1[!ID %chin% dup_a2_before_a1$ID, .(ID)]
  group_a2 <- group_a2[!ID %chin% dup_a2_within_a1$ID, ]
  
  # MAKE GROUP A2_ABORT & A2_MISCAR
  abort <- "635|636"
  group_a2_abort <- group_a2[grepl(abort, ICD9CM_1)| grepl(abort, ICD9CM_2) | grepl(abort, ICD9CM_3), .(ID)]
  group_a2_miscar <- group_a2[!ID %in% group_a2_abort$ID, .(ID)]
  LD2_ <- data_2012[event == 'O' & ID %in% eliminate_list_born_c[L_D==2]$ID_M, .(ID)]
  group_a2_miscar <- rbind(group_a2_miscar, LD2_)
  group_a2_miscar <- unique(group_a2_miscar, by = c("ID"))
  
  
  data_2012[ID %in% group_a1$ID, ":="(event = "a1", event_year = y)]
  data_2012[ID %in% group_a2_abort$ID , ":="(event = "a2_abort", event_year = y)]
  data_2012[(ID %in% group_a2_miscar$ID) , ":="(event = "a2_miscar", event_year = y)]
}
save(data_2012, file = paste0(rdata_path,filedate,"_data2010_id.RData"))
