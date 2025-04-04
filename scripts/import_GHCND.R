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

request.days <- 30 #how many days we want to request at a time.
#for most API stuff, can only do 30 days

#number of times to retry downloading
max_retries <- 5

#where we save the data
filepath <- "Rdata/GHCND"

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
      ncdc(datasetid=mydataset,
           stationid=mystation,
           startdate=startdate,
           enddate=enddate,
           limit=1000,
           add_units=TRUE,
           token=mytoken)
    )
    
    attemps <- attemps+1
  }
  
  return(temp.df$data)
}

if (date.min<max.date){
  if (force_full_rebuild!=TRUE){
    date.min <- max(prcp.df$date.et)+1 
  }
}

# import noaa data --------------------------------------------------------

#noaa requires an API key
mytoken="NztnZxyBXsFYlbxwgdTixLkVEexPrMgH"

#configure request parameters
mystation <- "GHCND:USW00014765" #the station we want to use for our request (KPVD)
mydataset <- "GHCND" #data type. we are requesting daily summaries



request.days <- 60 #how many days we want to request at a time. up to 365

if (date.max-date.min < request.days){ #if we requesting less than request.days, life is simple

  NOAA.df <- import_NOAA_data(date.min, date.max)

} else { #if we are requesting more than request.days (angry face....)
  
  result.list <- list()
  
  date.min.temp <- date.min #create temporary date variables
  date.max.temp <- date.min.temp+request.days #our temporary end date is request.days from the beginning date
  
  temp.df <- import_NOAA_data(date.min.temp, date.max.temp)
  
  if (length(temp.df)>1){
    result.list <- append(result.list, list(temp.df)) 
  }
  
  while (date.max-date.max.temp > request.days){ #we have one chunk of data already. if we have more than request.days left to request:
    date.min.temp <- date.max.temp #the beginning date moves up by request.days
    date.max.temp <- date.min.temp+request.days #ditto for the end date
    
    temp.df <- import_NOAA_data(date.min.temp, date.max.temp)
    
    if (length(temp.df)>1){
      result.list <- append(result.list, list(temp.df)) 
    }
    
    pb$tick()
  }
  
  if (date.max-date.max.temp < request.days & date.max-date.max.temp >0){ #if we have less than request.days left to request:
    date.min.temp <- date.max.temp
    date.max.temp <- date.max #now our temporary end date is just the end date that we originally wanted
    
    temp.df <- import_NOAA_data(date.min.temp, date.max.temp)
    
    if (length(temp.df)>1){
      result.list <- append(result.list, list(temp.df)) 
    }
    
    pb$tick()
  }
  
  pb$terminate()

  NOAA.df <- bind_rows(result.list)
    
}


# tidy NOAA data ----------------------------------------------------------

NOAA.df <- NOAA.df %>%
  mutate(date=as.date(date),
         value=as.numeric(value),
         prcp.mm=ifelse(units=="mm_tenths",value/10,value)) %>%
  select(all_of(c("date","datatype","prcp.mm"))) %>%
  filter(datatype=="PRCP"|datatype=="SNOW") %>%
  drop_na("prcp.mm") %>%
  distinct() %>%
  pivot_wider(names_from=datatype,values_from=prcp.mm) %>%
  rename("prcp.mm.day"="PRCP",
         "snow.mm.day"="SNOW")




# combine data ------------------------------------------------------------

newdata.df <- full_join(NOAA.df,USGS.df) %>%
  mutate(discharge.m3.day = na.approx(discharge.m3.day)) %>%
  rename("date.et"="date")

mindate <- as.Date(min(newdata.df$date.et,na.rm=TRUE))
maxdate <- as.Date(max(newdata.df$date.et,na.rm=TRUE))

dates <- seq(mindate,maxdate,1) %>%
  as.Date()

newdata.df <- newdata.df %>%
  complete(date.et = dates) %>%
  mutate(year=format(date.et,"%Y"),
         month=format(date.et,"%m"))

newdata.df <- left_join(newdata.df,nao.df) %>%
  select(-c("month","year"))

if (force_full_rebuild){
  climate.df <- newdata.df
} else (
  climate.df <- rbind(climate.df,newdata.df) %>%
    distinct()
)

#determine wet vs dry classification using the same methodology as NBEP
summary.df <- climate.df %>%
  mutate(month=as.numeric(month)) %>%
  filter(month>=6 & month<=9) %>% #subset to june, july, august, september
  mutate(median=median(discharge.m3.day,na.rm=TRUE)) %>% #calculate median for entire timeseries
  group_by(year) %>%
  mutate(annual=median(discharge.m3.day,na.rm=TRUE)) %>% #calculate median per year
  mutate(classification=ifelse(annual>median,"Wet","Dry")) %>% #if the annual median is greater than the whole median it is a wet year
  select(year,classification) %>% #select only year and classification
  unique()

climate.df <- left_join(climate.df,summary.df) #join the wet vs dry data

saveRDS(climate.df,file="Rdata/climate/NOAA_prcp.rds") #export the data

