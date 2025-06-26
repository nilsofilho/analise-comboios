#Carregar pacotes e funções

library(stringdist)
library(stringi)
library(readxl)
library(sf)
library(openxlsx)
library(parquetize)
library(arrow)
library(dplyr)
library(data.table)
library(gtfstools)
library(mapview)
library(tidyr)
library(zoo)
library(stringr)
library(purrr)
library(furrr)
library(future)
library(lubridate)
library(lwgeom)
#not in function
`%nin%` <- Negate(`%in%`)

create_set_ids <- function(sequence, gap_threshold = 2) {
  set_ids <- integer(length(sequence))
  current_id <- 0
  
  for (i in 1:length(sequence)) {
    if (i == 1 || abs(sequence[i] - sequence[i - 1]) > gap_threshold) {
      current_id <- current_id + 1
    }
    set_ids[i] <- current_id
  }
  
  return(set_ids)
}


criar_lrs_preciso <- function(shape_sf, segment_length = 10) {
  
  shape_utm <- st_transform(shape_sf, 31984)
  
  linha_segmentada <- st_segmentize(shape_utm, units::set_units(segment_length, "m"))
  
  lrs_pontos <- st_cast(linha_segmentada, "POINT")
  
  dist_segmentos <- st_distance(
    lrs_pontos[-nrow(lrs_pontos), ], 
    lrs_pontos[-1, ],             
    by_element = TRUE
  )
  
  lrs_pontos$dist_acumulada <- c(0, cumsum(as.numeric(dist_segmentos)))
  
  return(lrs_pontos)
}



