#3-Definição de comboios

rm(list = ls()); gc()
source("R/fun/setup.R")
options(future.globals.maxSize = 3000 * 1024^2)
linha <- 75
mes <- 3

lista_linhas <- list.files("data/viagens_linha/3/", pattern = ".parquet", full.names = T)

numeros_linhas <- sort(as.numeric(str_extract(basename(lista_linhas), "\\d+")))

tabela_linhas <- read_xlsx("data-raw/route_code_gps.xlsx")


#função de comboios



gps_paradas <- read_parquet(sprintf("data/viagens_linha/%s/linha_%s.parquet", mes, linha))






