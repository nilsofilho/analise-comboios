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
