#Sawyer Balint
#download NOAA precipitation data

rm(list = ls()) #clear environment

# import libraries --------------------------------------------------------

library(tidyverse)
library(rnoaa)
library(progress)
library(dataRetrieval)
library(rsoi)
library(zoo)

#for when sawyer fucks up
force_full_rebuild <- TRUE

# import old NOAA data ----------------------------------------------------

prcp.df <- readRDS("Rdata/climate/NOAA_prcp.rds")

prcp.df <- climate.df %>%
  select(date.et,prcp.mm.day,snow.mm.day) %>%
  drop_na(date.et)%>%
  drop_na(prcp.mm.day) %>%
  filter(date.et<max(date.et)-365)

min.date <- min(prcp.df$date.et)
max.date <- max(prcp.df$date.et)

date.min <- as.Date("2000-01-01") #our beginning date
date.max <- Sys.Date() #our ending date, which is the current data

# import NAO data ---------------------------------------------------------

#north atlantic Oscillation (currently not working)
#nao.df <- download_nao()

nao.df <- read.table("raw/nao.txt",fill=TRUE,row.names = NULL) %>%
  pivot_longer(2:13)

colnames(nao.df) <- c("year","month","NAO")

nao.df <- nao.df %>%
  mutate(month=as.integer(factor(month, levels = month.abb)),
         month=sprintf("%02d",month))


# import noaa data --------------------------------------------------------

#noaa requires an API key
mytoken="NztnZxyBXsFYlbxwgdTixLkVEexPrMgH"

#configure request parameters
mystation <- "GHCND:USW00014765" #the station we want to use for our request (KPVD)
mydataset <- "GHCND" #data type. we are requesting daily summaries

#noaa will only deliver 1000 observations or one year (whichever is less) of data at a time.
#as a result, we need to request the data in 60-day chunks and stitch
#it all together

import_NOAA_data <- function(startdate, enddate){
  temp.df <- data.frame("data"=as.numeric(NA))
  
  attemps <- 0
  
  while(length(temp.df$data)==1 & attemps<3){
    try(
      temp.df <- ncdc(datasetid=mydataset,
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

request.days <- 60 #how many days we want to request at a time. up to 365

if (date.max-date.min < request.days){ #if we requesting less than request.days, life is simple

  NOAA.df <- import_NOAA_data(date.min, date.max)

} else { #if we are requesting more than request.days (angry face....)
  
  #it's going to take a while, so lets set up a progress bar to make sure it's running
  n_iter <- round(as.numeric(date.max-date.min)/request.days,digits=0)+1 #figure out how long it will run
  pb <- progress_bar$new(format = "(:spin) [:bar] :percent [:elapsed || :eta]",
                         total = n_iter,complete = "=",incomplete = "-",current = ">",
                         clear = FALSE, width = 100, show_after=0)
  pb$tick(0)
  
  result.list <- list()
  
  date.min.temp <- date.min #create temporary date variables
  date.max.temp <- date.min.temp+request.days #our temporary end date is request.days from the beginning date
  
  temp.df <- import_NOAA_data(date.min.temp, date.max.temp)
  
  if (length(temp.df)>1){
    result.list <- append(result.list, list(temp.df)) 
  }
  
  pb$tick()
  
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

