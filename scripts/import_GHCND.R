#Sawyer Balint
#Download NOAA meteorological variables

rm(list = ls()) #clear environment

# import libraries --------------------------------------------------------

#install rnoaa directly from github
#remotes::install_github("ropensci/rnoaa")

library(tidyverse)
library(readxl)
library(rnoaa)
library(doParallel) #for parallel
library(foreach) #for parallel

options(noaakey = NULL)

#noaa requires an API key
token1 <- "NztnZxyBXsFYlbxwgdTixLkVEexPrMgH"
token2 <- "lknPniPGkwccUmJIliwomrITapUnmsoL"

request.days <- 60 #how many days we want to request at a time.
#for most API stuff, can only do 30 days

#number of times to retry downloading
max_retries <- 5

#where we save the data
filepath <- "Rdata/GHCND"

#configure request parameters
mystation <- "GHCND:USW00014765" #the station we want to use for our request (KPVD)
mydataset <- "GHCND" #data type. we are requesting daily summaries

# initialize parallel computing -------------------------------------------

#due to API rate limits there is no reason to use a ton of cores
n_cores <- 2

cluster <- makeCluster(n_cores, type="FORK")
registerDoParallel(cl = cluster)

year.min <- 1850
year.max <- 2023 #the NOAA API stopped working in 2022

# function to import precipitation ----------------------------------------

#noaa will only deliver 1000 observations or one year (whichever is less) of data at a time.
#as a result, we need to request the data in 60-day chunks and stitch
#it all together

download_prcp <- function(startdate, enddate){
  
  attempt <- 1
  result <- NULL
  
  while(is.null(result) & attempt <= max_retries){
    
    result <- tryCatch(
      ncdc(datasetid="GHCND",
           stationid=mystation,
           startdate=startdate,
           enddate=enddate,
           limit=1000,
           add_units=TRUE,
           token=mytoken),
      error = throttle)
    
    attemps <- attemps+1
  }
  
  if (!is.null(result)){
    
    data.df <- result$data
    #meta.df <- result$data
    return(data.df)
  }
}

# import noaa data --------------------------------------------------------

date.min <- as.Date(paste0(year.min, "-01-01"))
date.max <- as.Date(paste0(year.max, "-12-31"))

#empty list to store results
result.list <- list()

#create temporary date variables that are request.days apart
date.max.temp <- date.max
date.min.temp <- date.max.temp-request.days

result <- import_prcp(date.min.temp, date.max.temp)

if (is.null(result)){
  result.list <- append(result.list, list(result)) 
}

while (date.min.temp > date.min){ #if we have more data to fetch:
  
  date.max.temp <- date.min.temp
  date.min.temp <- date.min.temp-request.days
  
  result <- import_prcp(date.min.temp, date.max.temp)
  
  if (is.null(result)){
    result.list <- append(result.list, list(result)) 
  }
}

NOAA.df <- bind_rows(result.list)

# tidy NOAA data ----------------------------------------------------------

NOAA.df <- NOAA.df %>%
  mutate(date=as.date(date),
         value=as.numeric(value),
         prcp.mm=ifelse(units=="mm_tenths",value/10,value))
