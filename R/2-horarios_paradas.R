#2 - Detectar horário de passagem nas paradas

source("R/fun/setup.R")



linha <- 75
mes <- 3

dados <- read_parquet("")

tabela_linhas <- read_xlsx("data-raw/route_code_gps.xlsx")

#local para função

cat(sprintf("Lendo GPS tratado do mês %s da linha %s \n", mes, linha))
gps <- read_parquet(sprintf("data-raw/gps_linha_tratado/%s/gps_%s.parquet", mes, linha))

gps[, timestamp := lubridate::with_tz(timestamp, tzone = "America/Fortaleza")]

gtfs <- read_gtfs("data-raw/GTFS_start_202503.zip")

gtfs_linha <- gtfs %>% filter_by_route_id(str_pad(linha, width = 4, side = "left", pad = "0"))











