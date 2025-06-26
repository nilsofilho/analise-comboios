#2 - Detectar horário de passagem nas paradas
rm(list = ls()); gc()
source("R/fun/setup.R")

linha <- 75
mes <- 3

lista_linhas <- list.files("data-raw/gps_linha_tratado/3/", pattern = ".parquet", full.names = T)

numeros_linhas <- sort(as.numeric(str_extract(basename(lista_linhas), "\\d+")))

tabela_linhas <- read_xlsx("data-raw/route_code_gps.xlsx")

processa_viagens <- function(mes, linha) {
  


#loading the necessary data

#preprocessed route gps
cat(sprintf("Lendo GPS tratado do mês %s da linha %s \n", mes, linha))
gps <- read_parquet(sprintf("data-raw/gps_linha_tratado/%s/gps_%s.parquet", mes, linha))

gps[, timestamp := lubridate::with_tz(timestamp, tzone = "America/Fortaleza")]

gps_sf <- gps %>% st_as_sf(coords = c("longitude", "latitude"), crs = 4326, remove = FALSE)

rm(gps)
#gtfs
gtfs <- read_gtfs("data-raw/GTFS_start_202503.zip")

#route gtfs
gtfs_linha <- gtfs %>% filter_by_route_id(str_pad(linha, width = 4, side = "left", pad = "0"))

rm(gtfs)

#route sf
stops_linha <- gtfstools::convert_stops_to_sf(gtfs_linha)
shapes_linha <- gtfstools::convert_shapes_to_sf(gtfs_linha)

ida <- gtfstools::convert_shapes_to_sf(gtfs_linha) %>%
  filter(shape_id == paste0("shape",str_pad(linha, width = 4, side = "left", pad = "0"),"-","I")) %>%
  st_as_sf()
volta <- gtfstools::convert_shapes_to_sf(gtfs_linha) %>%
  filter(shape_id == paste0("shape",str_pad(linha, width = 4, side = "left", pad = "0"),"-","V")) %>%
  st_as_sf()

trips_dt <- as.data.table(gtfs_linha$trips)
ida_trip_ids <- trips_dt[grepl("I", shape_id, ignore.case = TRUE), trip_id]
volta_trip_ids <- trips_dt[grepl("V", shape_id, ignore.case = TRUE), trip_id]


#Obter as sequências de paradas para cada sentido
stop_times_dt <- as.data.table(gtfs_linha$stop_times)


# Paradas e suas sequências para viagens de IDA
stops_ida_info <- stop_times_dt[trip_id %in% ida_trip_ids, .(stop_id, stop_sequence)] |> unique()

# Paradas e suas sequências para viagens de VOLTA
stops_volta_info <- stop_times_dt[trip_id %in% volta_trip_ids, .(stop_id, stop_sequence)] |> unique()

# Criar os objetos SF finais para cada sentido, juntando com a geometria
stops_ida_sf <- merge(stops_linha, stops_ida_info, by = "stop_id")
stops_volta_sf <- merge(stops_linha, stops_volta_info, by = "stop_id")


# mapview(shapes_linha)

#snapping of gps to line shape

#teste com uma única viagem e sentido

#some trips may have missing gps points due to equipment failure
#test with trip "75_44189_20250311_I_2"

trips <- unique(gps_sf$viagem_id_final)




# trip_id <- "75_44194_20250313_V_6"

stop_passage_trip <- function(trip_id) {
  
#test with complete trip
trip_gps <- gps_sf %>% filter(viagem_id_final == trip_id)

if (nrow(trip_gps) < 2){
  stop()
  }



# limite de tempo para uma quebra de viagem (em minutos)
limite_gap_minutos <- 30

trip_dt <- as.data.table(trip_gps)

setorder(trip_dt, timestamp)

trip_dt[, tempo_desde_anterior := as.numeric(
  difftime(timestamp, shift(timestamp, type = "lag"), units = "mins")
)]

trip_dt[, bloco_id := cumsum(tempo_desde_anterior > limite_gap_minutos | is.na(tempo_desde_anterior))]

contagem_blocos <- trip_dt[, .N, by = bloco_id]

if (nrow(contagem_blocos) > 0) {
  id_bloco_principal <- contagem_blocos[which.max(N), bloco_id]
  
  trip_dt_limpo <- trip_dt[bloco_id == id_bloco_principal]
} else {
  trip_dt_limpo <- trip_dt
}


trip_gps <- st_as_sf(trip_dt_limpo, 
                           coords = c("longitude", "latitude"), 
                           crs = 4326, 
                           remove = FALSE)





#for debugging purposes
# mapview(trip_gps, zcol = "vseq") + mapview(ida, color = "darkred") + mapview(stops_ida_sf, zcol = "stop_sequence")


#snap do gps para o shape da trip

if (unique(trip_gps$sentido)=="I"){
   shape <- ida
   stops <- stops_ida_sf %>% mutate(sentido = "I") %>% arrange(stop_sequence)
} else {
  shape <- volta
  stops <- stops_volta_sf %>% mutate(sentido = "V") %>% arrange(stop_sequence)
}


pontos_sf <- stops
shape_sf <- shape


# LRS para a rota da viagem
lrs_preciso <- criar_lrs_preciso(shape, segment_length = 10) %>%
  mutate(shape_pt_sequence = seq(1:nrow(.)))

# paradas no LRS
stops_utm <- st_transform(stops, 31984)
indices_paradas <- st_nearest_feature(stops_utm, lrs_preciso)
stops_utm$dist_acumulada <- lrs_preciso$dist_acumulada[indices_paradas]


# Garante que a distância das paradas seja sempre crescente
stops_dt <- as.data.table(stops_utm)
setorder(stops_dt, stop_sequence)
stops_dt[, dist_acumulada := nafill(pmax(dist_acumulada, shift(dist_acumulada, fill = 0)), type = "locf")]


# pontos de GPS no LRS
trip_gps_utm <- st_transform(trip_gps, 31984)
indices_gps <- st_nearest_feature(trip_gps_utm, lrs_preciso)
trip_gps_utm$dist_acumulada <- lrs_preciso$dist_acumulada[indices_gps]


# Força a distância a ser sempre crescente para lidar com ruído e laços de quadra
trip_gps_dt <- as.data.table(trip_gps_utm)
setorder(trip_gps_dt, timestamp)
trip_gps_dt[, dist_acumulada := cummax(dist_acumulada)]

vehicle_colar <- unique(trip_gps_dt$vehicle_id)
sentido_colar <- unique(trip_gps_dt$sentido)
trip_colar <- unique(trip_gps_dt$viagem_id_final)

# coluna de resultados
resultados_paradas <- copy(stops_dt)
resultados_paradas[,  `:=` (
  vehicle_id = vehicle_colar,
  viagem_id = trip_colar,
  sentido = sentido_colar
)]
resultados_paradas[, `:=`(
  horario_passagem = as.POSIXct(NA, tz = "America/Fortaleza"),
  metodo_estimativa = as.character(NA)
)]
# --- Parâmetros ---
tolerancia_m <- 1.5 # Tolerância em metros para um "acerto direto"
pontos_velocidade <- 10 # Nº de pontos GPS para calcular a velocidade local

# j <- 27


# FASE 1: Medição Direta e Interpolação

for (j in 1:nrow(stops_dt)) {
  # print(j)
  dist_parada <- stops_dt$dist_acumulada[j]
  
  # TENTATIVA 1: "ACERTO DIRETO"
  ponto_exato <- trip_gps_dt[abs(dist_acumulada - dist_parada) < tolerancia_m]
  if (nrow(ponto_exato) > 0) {
    resultados_paradas[j, `:=`(horario_passagem = ponto_exato$timestamp[1], metodo_estimativa = "medido")]
    next 
  }
  
  # TENTATIVA 2: INTERPOLAÇÃO "SANDUÍCHE" (COM VERIFICAÇÃO DE NA)
  ponto_antes <- trip_gps_dt[dist_acumulada <= dist_parada, .SD[.N]]
  ponto_depois <- trip_gps_dt[dist_acumulada > dist_parada, .SD[1]]
  
  if (
    nrow(ponto_antes) > 0 && 
    nrow(ponto_depois) > 0 && 
    !is.na(ponto_depois$dist_acumulada) &&  
    ponto_antes$dist_acumulada < ponto_depois$dist_acumulada
  ) {
    fracao_dist <- (dist_parada - ponto_antes$dist_acumulada) / (ponto_depois$dist_acumulada - ponto_antes$dist_acumulada)
    duracao_trecho <- as.numeric(difftime(ponto_depois$timestamp, ponto_antes$timestamp, units = "secs"))
    
    horario_estimado <- ponto_antes$timestamp + (fracao_dist * duracao_trecho)
    resultados_paradas[j, `:=`(horario_passagem = horario_estimado, metodo_estimativa = "medido")]
    
    next
  }
}

# FASE 2: Extrapolação
indices_validos <- which(!is.na(resultados_paradas$horario_passagem))

primeiro_valido_idx <- head(indices_validos, 1)
ultimo_valido_idx <- tail(indices_validos, 1)


if (length(primeiro_valido_idx) == 0) {
  
  cat("AVISO: Nenhum ponto pôde ser medido/interpolado. Usando modo de 'extrapolação total'.\n")
  
  if (nrow(trip_gps_dt) >= 2) {
    dist_total_gps <- max(trip_gps_dt$dist_acumulada) - min(trip_gps_dt$dist_acumulada)
    tempo_total_gps <- as.numeric(difftime(max(trip_gps_dt$timestamp), min(trip_gps_dt$timestamp), units = "secs"))
    
    if (tempo_total_gps > 0) {
      velocidade_media_viagem <- dist_total_gps / tempo_total_gps
      
      ponto_ref_dist <- trip_gps_dt$dist_acumulada[1]
      ponto_ref_horario <- trip_gps_dt$timestamp[1]
      
      for (k in 1:nrow(resultados_paradas)) {
        dist_a_percorrer <- resultados_paradas$dist_acumulada[k] - ponto_ref_dist
        tempo_extrapolado <- dist_a_percorrer / velocidade_media_viagem
        horario_estimado <- ponto_ref_horario + tempo_extrapolado
        
        resultados_paradas[k, `:=`(horario_passagem = horario_estimado, metodo_estimativa = "reconstruido_total")]
      }
    }
  }
  
} else {



if (!is.na(primeiro_valido_idx) && primeiro_valido_idx > 1) {
  if (nrow(trip_gps_dt) >= pontos_velocidade) {
    segmento_inicial <- trip_gps_dt[1:pontos_velocidade]
    dist_percorrida <- max(segmento_inicial$dist_acumulada) - min(segmento_inicial$dist_acumulada)
    tempo_gasto <- as.numeric(difftime(max(segmento_inicial$timestamp), min(segmento_inicial$timestamp), units = "secs"))
    
    if (tempo_gasto > 0) {
      velocidade_local <- dist_percorrida / tempo_gasto # em m/s
      
      ponto_ref_dist <- resultados_paradas$dist_acumulada[primeiro_valido_idx]
      ponto_ref_horario <- resultados_paradas$horario_passagem[primeiro_valido_idx]
      
      for (k in (primeiro_valido_idx - 1):1) {
        dist_a_percorrer <- resultados_paradas$dist_acumulada[k] - ponto_ref_dist 
        
        tempo_extrapolado <- dist_a_percorrer / velocidade_local
        horario_estimado <- ponto_ref_horario + tempo_extrapolado
        
        resultados_paradas[k, `:=`(horario_passagem = horario_estimado, metodo_estimativa = "reconstruido")]
      }
    }
  }
}


if (!is.na(ultimo_valido_idx) && ultimo_valido_idx < nrow(resultados_paradas)) {
  if (nrow(trip_gps_dt) >= pontos_velocidade) {
    N_gps <- nrow(trip_gps_dt)
    segmento_final <- trip_gps_dt[(N_gps - pontos_velocidade + 1):N_gps]
    dist_percorrida <- max(segmento_final$dist_acumulada) - min(segmento_final$dist_acumulada)
    tempo_gasto <- as.numeric(difftime(max(segmento_final$timestamp), min(segmento_final$timestamp), units = "secs"))
    
    if (tempo_gasto > 0) {
      velocidade_local <- dist_percorrida / tempo_gasto # em m/s
      
      ponto_ref_dist <- resultados_paradas$dist_acumulada[ultimo_valido_idx]
      ponto_ref_horario <- resultados_paradas$horario_passagem[ultimo_valido_idx]
      
      for (k in (ultimo_valido_idx + 1):nrow(resultados_paradas)) {
        dist_a_percorrer <- resultados_paradas$dist_acumulada[k] - ponto_ref_dist 
        
        tempo_extrapolado <- dist_a_percorrer / velocidade_local
        horario_estimado <- ponto_ref_horario + tempo_extrapolado
        
        resultados_paradas[k, `:=`(horario_passagem = horario_estimado, metodo_estimativa = "reconstruido")]
      }
    }
  }
}

}

# Controle
# Se apenas a primeira parada foi extrapolada, considere-a "medida"
if (!is.na(primeiro_valido_idx) && primeiro_valido_idx == 2) {
  resultados_paradas[1, metodo_estimativa := "medido"]
}
# Se apenas a última parada foi extrapolada, considere-a "medida"
if (!is.na(ultimo_valido_idx) && ultimo_valido_idx == (nrow(resultados_paradas) - 1)) {
  resultados_paradas[nrow(resultados_paradas), metodo_estimativa := "medido"]
}

retorno <- resultados_paradas %>% st_as_sf() %>% st_transform(4326)
return(retorno)

}


crs_final <- 4326 

geometria_vazia <- st_sfc(st_point(c(NA_real_, NA_real_)), crs = crs_final)

falha_df <- data.frame(
  stop_id = NA_character_,
  stop_code = NA_character_,
  stop_name = NA_character_,
  stop_desc = NA_character_,
  zone_id = NA_character_,
  stop_url = NA_character_,
  location_type = NA_integer_,
  parent_station = NA_character_,
  stop_timezone = NA_character_,
  wheelchair_boarding = NA_integer_,
  stop_sequence = NA_integer_,
  sentido = NA_character_,
  dist_acumulada = NA_real_,
  horario_passagem = as.POSIXct(NA),
  metodo_estimativa = "Falhou"
)

valor_otherwise_sf <- falha_df %>%
  mutate(geometry = geometria_vazia) %>%
  st_as_sf()

stop_passage_trip_safe <- possibly(stop_passage_trip,
                                   otherwise = valor_otherwise_sf)

# teste <- stop_passage_trip_safe("75_44194_20250313_V_4")

# teste <- map_df(.x = c("75_44194_20250313_V_4","75_44194_20250313_V_6"), .f = stop_passage_trip_safe)


plan(multisession, workers = 4) 
#apply function to all trips
trips_all <- future_map_dfr(
  .x = trips, 
  .f = stop_passage_trip_safe, 
  .options = furrr_options(seed = TRUE), 
  .progress = TRUE
)

plan(sequential)

#salvar parquet
coords <- st_coordinates(trips_all)
trips_para_salvar <- trips_all %>%
  mutate(
    longitude = coords[, 1], 
    latitude  = coords[, 2]  
  ) %>%
  st_drop_geometry() 

suppressWarnings(dir.create(sprintf("data/viagens_linha/%s/", mes), recursive = T))


arrow::write_parquet(trips_para_salvar,
                     sprintf("data/viagens_linha/%s/linha_%s.parquet", mes, linha))

}

walk(.x = , .f = processa_viagens(mes = 3))


