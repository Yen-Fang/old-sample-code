#' README
#' ======
#'
#' What does this file do?
#' =======================
#' This script build the main
#'
#'
#' What does this file take?
#' =========================
#' - Raw Data: OPDTE[89-109], ENROL[89-109], BIRTH[90-109]
#'
#'
#' What does this file output?
#' ===========================
#' 



# Setup ---------------------------------------------------------------------------------------

source("setup.R")


# Main ----------------------------------------------------------------------------------------

# core info

source("scripts/00-filter-sample.R") # eliminate ppl with past birth and misc record
source("scripts/01-create-enrollment-data.R") # labor market outcomes
source("scripts/02-create-health-data.R") # health outcomes

# ==== not bring out from data center yet ====
source("scripts/03-create-husband-data.R")
source("scripts/03a-create-husband-enrollment-data.R")  
source("scripts/03b-create-husband-health-data.R") 

# summ stats
source("scripts/04-summary-stats.R") # 
source("scripts/05-raw-outcome-plot.R") # 

# analysis
source("scripts/06-benchnark.R") # 
source("scripts/07-cs-later-treated.R") # 
source("scripts/08-cs-never-treated-A1A2.R") # 

source("scripts/09-heterogeneity-pre-income.R") # 


