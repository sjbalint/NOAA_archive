#Sawyer Balint
#This script doesn't work due to IP restrictions

rm(list = ls()) #clear environment

# load packages -----------------------------------------------------------

library(tidyverse) #for data manipulation
library(SWMPr) #to import SWMP data

# identify stations -------------------------------------------------------

code.list <- site_codes()

df <- apacpnut

# download data -----------------------------------------------------------

#empty list to store results
result.list <- list()

#iterate over every station
for (code.i in code.list){
  
  #print a message to keep us sane
  message(paste("downloading",code.i))
  
  #download data from the NEERS api
  df <- all_params(code.i, Max = 10^20, 
                   param = NULL, trace = TRUE) %>%
    mutate(station_id = code.i)
  
  #export the data as a csv
  write.csv(df, file=paste0("export/",code.i,".csv"), row.names=FALSE)
  
  result.list <- append(result.list, df)
}

df <- bind_rows(result.list)


