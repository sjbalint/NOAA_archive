#Sawyer Balint
#Download NOAA meteorological variables

rm(list = ls()) #clear environment

# import libraries --------------------------------------------------------

library(tidyverse)
#library(rnoaa)
library(progress)
library(rsoi)
library(zoo)
library(multidplyr) #for multiple cores
library(data.table) #for large datasets
library(dtplyr) #wrapper to use dplyr with data.table
library(parallel) #multiple cores

cluster <- new_cluster(detectCores())

# configure input ---------------------------------------------------------

NOAA.df <- readRDS("Rdata/climate/NOAA_met_raw.rds")

date.min <- as.Date(max(NOAA.df$datetime.et))-180 #our beginning date
date.max <- as.Date(Sys.Date()) #our ending date

station.list <- c(8454000, #providence
  8452944, #conimicut point
  8454049, #quansett
  8452660 #newport
)

#noaa requires an API key
mytoken="NztnZxyBXsFYlbxwgdTixLkVEexPrMgH"

# import noaa data --------------------------------------------------------

download_tide <- function(begin_date, end_date, station.number){
  
  numeric.date.min <- as.numeric(format(begin_date, "%Y%m%d"))
  numeric.date.max <- as.numeric(format(end_date, "%Y%m%d"))
  
  return.df <-
    coops_search(station_name = station.number,
                 begin_date = numeric.date.min,
                 end_date = numeric.date.max,
                 product = "water_level",
                 datum = "MLLW",
                 time_zone = "lst"
                 )
  
  return.df <- return.df$data %>%
    select(t,v)
  
  colnames(return.df) <- c("datetime.et","tide.m.mllw")
  
  return(return.df)
}

download_wind <- function(begin_date, end_date, station.number){
  
  numeric.date.min <- as.numeric(format(begin_date, "%Y%m%d"))
  numeric.date.max <- as.numeric(format(end_date, "%Y%m%d"))
  
  return.df <-
    coops_search(station_name = station.number,
                 begin_date = numeric.date.min,
                 end_date = numeric.date.max,
                 product = "wind",
                 time_zone = "lst"
    )
  
  return.df <- return.df$data %>%
    select(t,s,d,g)
  
  colnames(return.df) <- c("datetime.et","speed.ms","dir.deg","gust.ms")
  
  return(return.df)
}

download_temp <- function(begin_date, end_date, station.number){
  
  numeric.date.min <- as.numeric(format(begin_date, "%Y%m%d"))
  numeric.date.max <- as.numeric(format(end_date, "%Y%m%d"))
  
  return.df <-
    coops_search(station_name = station.number,
                 begin_date = numeric.date.min,
                 end_date = numeric.date.max,
                 product = "air_temperature",
                 time_zone = "lst"
    )
  
  return.df <- return.df$data %>%
    select(t,v)
  
  colnames(return.df) <- c("datetime.et","temp.c")
  
  return(return.df)
}

download_baro <- function(begin_date, end_date, station.number){
  
  numeric.date.min <- as.numeric(format(begin_date, "%Y%m%d"))
  numeric.date.max <- as.numeric(format(end_date, "%Y%m%d"))
  
  return.df <-
    coops_search(station_name = station.number,
                 begin_date = numeric.date.min,
                 end_date = numeric.date.max,
                 product = "air_pressure",
                 time_zone = "lst"
    )
  
  return.df <- return.df$data %>%
  select(t,v)
  
  colnames(return.df) <- c("datetime.et","baro.mb")
  
  return(return.df)
}

download_data <- function(begin_date, end_date, station.number){
  
  #create empty dataframes
  tide.df <- data.frame("datetime.et"=NA)
  wind.df <- data.frame("datetime.et"=NA)
  temp.df <- data.frame("datetime.et"=NA)
  baro.df <- data.frame("datetime.et"=NA)
  
  result.df <- data.frame("datetime.et"=NA)
  
  #try to download data. sometimes it doesn't work, in which case we keep moving on
  try(tide.df <- download_tide(begin_date, end_date, station.number))
  try(wind.df <- download_wind(begin_date, end_date, station.number))
  try(temp.df <- download_temp(begin_date, end_date, station.number))
  try(baro.df <- download_baro(begin_date, end_date, station.number))
  
  result.df <- full_join(result.df, tide.df, by=join_by(datetime.et))
  result.df <- full_join(result.df, wind.df, by=join_by(datetime.et))
  result.df <- full_join(result.df, temp.df, by=join_by(datetime.et))
  result.df <- full_join(result.df, baro.df, by=join_by(datetime.et))
  
  result.df$station <- as.character(station.number)
  
  result.df <- result.df %>%
    drop_na(datetime.et)
  
  return(result.df)
}

#noaa will only deliver 31 days of data at a time.
#as a result, we need to request the data in 30-day chunks and stitch
#it all together

result.list <- list()

request.days <- 30 #how many days we want to request at a time.

for (station in station.list){
  
  print(paste("downloading",station))
  
  if (date.max-date.min < request.days){ #if we requesting less than request.days, life is simple
    
    temp.df <- download_data(begin_date = date.min, 
                             end_date = date.max,
                             station.number = station)
    
    if (nrow(temp.df>0)){
      result.list <- append(result.list, list(temp.df)) 
    }
    
  } else { #if we are requesting more than request.days (angry face....)
    
    #it's going to take a while, so lets set up a progress bar to make sure it's running
    n_iter <- ceiling(as.numeric(date.max-date.min)/request.days) #figure out how long it will run
    pb <- progress_bar$new(format = "(:spin) [:bar] :percent [:elapsed || :eta]",
                           total = n_iter,complete = "=",incomplete = "-",current = ">",
                           clear = FALSE, width = 100, show_after=0)
    pb$tick(0)
    
    date.min.temp <- date.min #create temporary date variables
    date.max.temp <- date.min.temp+request.days #our temporary end date is request.days from the beginning date
    
    temp.df <- download_data(begin_date = date.min.temp, 
                             end_date = date.max.temp,
                             station.number = station)
    
    if (nrow(temp.df>0)){
      result.list <- append(result.list, list(temp.df)) 
    }
    
    pb$tick()
    
    while (date.max-date.max.temp > request.days){ #we have one chunk of data already. if we have more than request.days left to request:
      date.min.temp <- date.max.temp #the beginning date moves up by request.days
      date.max.temp <- date.min.temp+request.days #ditto for the end date
      
      temp.df <- try(
        download_data(begin_date = date.min.temp, 
                      end_date = date.max.temp,
                      station.number = station)
      )
      
      if (nrow(temp.df>0)){
        result.list <- append(result.list, list(temp.df)) 
      }
      
      pb$tick()
    }
    
    if (date.max-date.max.temp < request.days & date.max-date.max.temp >0){ #if we have less than request.days left to request:
      date.min.temp <- date.max.temp
      date.max.temp <- date.max #now our temporary end date is just the end date that we originally wanted
      
      temp.df <- download_data(begin_date = date.min.temp, 
                               end_date = date.max.temp,
                               station.number = station)
      
      if (nrow(temp.df>0)){
        result.list <- append(result.list, list(temp.df)) 
      }
      
      pb$tick()
    }
    
    pb$terminate()
    
  }
}

NOAA_updated.df <- bind_rows(result.list)

NOAA.df <- bind_rows(NOAA.df,NOAA_updated.df) %>%
  unique()

saveRDS(NOAA.df, file="Rdata/climate/NOAA_met_raw.rds")

# compile finalized data --------------------------------------------------

datetime.list <- seq(min(NOAA.df$datetime.et), max(NOAA.df$datetime.et), by="15 min")
station.list <- unique(NOAA.df$station)

NOAA_met.df <- NOAA.df %>%
  complete(datetime.et=datetime.list,
           station=station.list) %>%
  arrange(station, datetime.et) %>%
  group_by(station) %>%
  partition(cluster) %>%
  mutate_if(is.character, as.numeric) %>%
  mutate_if(is.numeric, na.approx, na.rm=FALSE, maxgap=15) %>%
  collect() %>%
  ungroup() %>%
  arrange(station, datetime.et)

saveRDS(NOAA_met.df, file="Rdata/climate/NOAA_met.rds")

NOAA_mean.df <- data.table(NOAA_met.df) %>%
  select(-station) %>%
  group_by(datetime.et) %>%
  partition(cluster) %>%
  summarize_all(mean, na.rm=TRUE) %>%
  collect() %>%
  ungroup() %>%
  arrange(datetime.et)

saveRDS(NOAA_mean.df, file="Rdata/climate/NOAA_met_bay_average.rds")
