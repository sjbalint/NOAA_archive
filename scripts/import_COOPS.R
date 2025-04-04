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
filepath <- "Rdata/COOPS"

# initialize parallel computing -------------------------------------------

#due to API rate limits there is no reason to use a ton of cores
n_cores <- 2

cluster <- makeCluster(n_cores, type="FORK")
registerDoParallel(cl = cluster)

# identify station IDS ----------------------------------------------------

#excel file of stations to downoad
stations.df <- read_excel("raw/NOAA_COOPS_stations.xlsx")

# configure input ---------------------------------------------------------

year.min <- 1990
year.max <- 2023 #the NOAA API stopped working in 2022

# functions for downloading specific data ---------------------------------

download_tide <- function(begin_date, end_date, station.number){
  
  #coops_search() requires numeric date
  numeric.date.min <- as.numeric(format(begin_date, "%Y%m%d"))
  numeric.date.max <- as.numeric(format(end_date, "%Y%m%d"))
  
  #download tide data
  coops.df <-
    coops_search(station_name = station.number,
                 begin_date = numeric.date.min,
                 end_date = numeric.date.max,
                 product = "water_level",
                 datum = "MLLW",
                 time_zone = "lst_ldt",
                 token = token1
                 )
  
  #extract data and metadata
  data.df <- coops.df$data
  meta.df <- coops.df$metadata
  return.df <- bind_cols(meta.df, data.df)
  
  #set informative column names
  colnames(return.df) <- c("stationID","name","lat.deg","lon.deg",
                           "datetime.et","tide.m.mllw","s.tide","f.tide","q.tide")
  
  return(return.df)
}

download_wind <- function(begin_date, end_date, station.number){
  
  numeric.date.min <- as.numeric(format(begin_date, "%Y%m%d"))
  numeric.date.max <- as.numeric(format(end_date, "%Y%m%d"))
  
  coops.df <-
    coops_search(station_name = station.number,
                 begin_date = numeric.date.min,
                 end_date = numeric.date.max,
                 product = "wind",
                 time_zone = "lst_ldt",
                 token = token1
    )
  
  data.df <- coops.df$data
  meta.df <- coops.df$metadata
  return.df <- bind_cols(meta.df, data.df)
  
  colnames(return.df) <- c("stationID","name","lat.deg","lon.deg",
                           "datetime.et","speed.ms","dir.deg","dir.card",
                           "gust.ms","f.wind")
  
  return(return.df)
}

download_temp <- function(begin_date, end_date, station.number){
  
  numeric.date.min <- as.numeric(format(begin_date, "%Y%m%d"))
  numeric.date.max <- as.numeric(format(end_date, "%Y%m%d"))
  
  
  coops.df <-
    coops_search(station_name = station.number,
                 begin_date = numeric.date.min,
                 end_date = numeric.date.max,
                 product = "air_temperature",
                 time_zone = "lst_ldt",
                 token = token2
    )
  
  data.df <- coops.df$data
  meta.df <- coops.df$metadata
  return.df <- bind_cols(meta.df, data.df)
  
  colnames(return.df) <- c("stationID","name","lat.deg","lon.deg",
                           "datetime.et","temp.c","f.temp")
  
  return(return.df)
}

download_baro <- function(begin_date, end_date, station.number){
  
  numeric.date.min <- as.numeric(format(begin_date, "%Y%m%d"))
  numeric.date.max <- as.numeric(format(end_date, "%Y%m%d"))
  
  coops.df <-
    coops_search(station_name = station.number,
                 begin_date = numeric.date.min,
                 end_date = numeric.date.max,
                 product = "air_pressure",
                 time_zone = "lst_ldt",
                 token = token2
    )
  
  data.df <- coops.df$data
  meta.df <- coops.df$metadata
  return.df <- bind_cols(meta.df, data.df)
  
  colnames(return.df) <- c("stationID","name","lat.deg","lon.deg",
                           "datetime.et","baro.mb","f.baro")
  
  return(return.df)
}

# function to download everything for one station -------------------------

download_data <- function(begin_date, end_date, station.number){
  
  #create empty dataframes
  empty.df <- data.frame("stationID" = NA,
                         "name" = NA,
                         "lat.deg" = NA,
                         "lon.deg" = NA,
                         "datetime.et"=NA)
  
  tide.df <- empty.df
  wind.df <- empty.df
  temp.df <- empty.df
  baro.df <- empty.df
  
  #try to download data. sometimes it doesn't work, in which case we keep moving on
  tide.df <- download_tide(begin_date, end_date, station.number)
  wind.df <- download_wind(begin_date, end_date, station.number)
  temp.df <- download_temp(begin_date, end_date, station.number)
  baro.df <- download_baro(begin_date, end_date, station.number)
  
  #combine everything together
  result.df <- reduce(list(tide.df, wind.df, temp.df, baro.df),
                        full_join, by=join_by(stationID, name, lat.deg, lon.deg, datetime.et)) %>%
    drop_na(datetime.et)
  
  return(result.df)
}

#function to throttle requests when the API limit is reached
#when the limit is hit, wait 5 minutes and try again
throttle <- function(e) {
  if (grepl("limit", e$message, ignore.case = TRUE)) {
    message("Rate limit reached. Throttling in progress")
    Sys.sleep(20*60)  
    return(NULL)
  }
}

# download data for all stations ------------------------------------------

#list of all stations to download
station.list <- stations.df %>%
  filter(state %in% c("RI", "MA")) %>%
  pull(stationID)

year.list <- seq(year.min, year.max) %>%
  sort(decreasing = TRUE)

#return all files greater than 500kb
#files less that are less than 500kb might not have been completely downloaded
file.list <- file.info(list.files(filepath, full.names=TRUE)) %>%
  filter(size > 000000)

#get station IDs and year from file name
completed.list <- rownames(file.list) %>%
  str_split_i(".rds",1) %>%
  str_split_i("/",4)


#iterate over every station
foreach (station.i = station.list) %dopar%{
#for (station.i in station.list){
  
  #iterate over every year individually
  for (year.i in year.list){
    
    if (!paste0(station.i,"_",year.i) %in% completed.list){
     
      date.min <- as.Date(paste0(year.i, "-01-01"))
      date.max <- as.Date(paste0(year.i, "-12-31"))
      
      #empty list to store results
      result.list <- list()
      
      #create temporary date variables that are request.days apart
      date.max.temp <- date.max
      date.min.temp <- date.max.temp-request.days
      
      attempt <- 1
      result <- NULL
      
      while (is.null(result) && attempt <= max_retries){
        
        result <- tryCatch(
          download_data(begin_date = date.min.temp, 
                        end_date = date.max.temp,
                        station.number = station.i),
          error = throttle)
        
        attempt <- attempt + 1
        
      }
      
      if (!is.null(result)){
        result.list <- append(result.list, list(result)) 
      }
      
      while (date.min.temp > date.min){ #if we have more data to fetch:
        
        date.max.temp <- date.min.temp
        date.min.temp <- date.min.temp-request.days
        
        attempt <- 1
        result <- NULL
        
        while (is.null(result) && attempt <= max_retries){
          
          result <- tryCatch(
            download_data(begin_date = date.min.temp, 
                          end_date = date.max.temp,
                          station.number = station.i),
            error = throttle)
          
          attempt <- attempt + 1
          
        }
        
        if (!is.null(result)){
          result.list <- append(result.list, list(result)) 
        }
      }
      
      output.df <- bind_rows(result.list)
      
      if (nrow(output.df)>1){
        
       output.df %>%
        filter(year(datetime.et) == year.i) %>%
         arrange(datetime.et) %>%
          saveRDS(file=paste0(filepath,"/",station.i,"_",year.i,".rds"))
      }
    }
  }
}

stopCluster(cl = cluster)
