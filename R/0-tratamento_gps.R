rm(list = ls());gc()

library(stringdist)
library(stringi)
library(readxl)
library(sf)
library(openxlsx)
library(parquetize)
library(arrow)
library(dplyr)
library(data.table)

# csv_to_parquet(path_to_file = 'data-raw/PAITT_202503_281.csv',
#                path_to_parquet = 'data-raw/PAITT_202503_281.parquet')


gps <- read_parquet("data-raw/PAITT_202503_281.parquet", as_data_table = TRUE)

head(gps)

primeira_linha_dados <- names(gps)

nomes_colunas <- c("direction", "latitude", "longitude", "timestamp", 
                         "odometer", "routecode", "speed", "device_id", "vehicle_id")

primeira_linha_dados <- names(gps)
primeira_linha_dados[6] <- "0" 
primeira_linha_dados[7] <- "0" 

names(gps) <- nomes_colunas

primeira_linha_df <- setNames(as.list(primeira_linha_dados), nomes_colunas) |>
  tibble::as_tibble()

primeira_linha_df <- primeira_linha_df |>
  mutate(
    direction = as.numeric(direction),
    latitude = as.numeric(latitude),
    longitude = as.numeric(longitude),
    timestamp = as.numeric(timestamp),
    odometer = as.numeric(odometer),
    routecode = as.numeric(routecode),
    speed = as.numeric(speed),
    device_id = as.character(device_id),
    vehicle_id = as.numeric(vehicle_id)
  )

gps <- bind_rows(primeira_linha_df, gps)
setDT(gps)
gps[, timestamp := lubridate::ymd_hms(timestamp, tz = "UTC")]


write_parquet(gps,"data-raw/PAITT_202503_281_corrigido.parquet")


#save partioned by routecode
write_dataset(gps, path = "data-raw/gps_linha",
              format = "parquet",
              partitioning = "routecode")


