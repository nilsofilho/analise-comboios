#Identificaçaõ dos transbordos

rm(list = ls());gc()

source("R/fun/setup.R")


linhas_circular <- c(11,12,51,52,53)
desconhecidas <- c(0,2,5,8,13)

nao_linhas <- c(599)
nao_linhas <- data.frame(
  cod_integracao_externo = c(0,    1,    2,    5,    8,   11,   12,   13,
               22,   31,   32,   34,   35,   51,   52,   55,
               56,   59,   62,   63,   73,  289,  344,  354,
               445,  454,  457,  458,  460,  552,  560,  571,
               574,  575,  578,  593,  594,  595,  598,  599,
               605,  606,  637,  668, 1008, 1072, 1094, 1096),
               tipo = c("Não definido", "Garagem", "Garagem", "Garagem", "Garagem", "Circular", "Circular", "Garagem",
                        ""))
faltando_gtfs <- c(22)

a_fazer

linha <- 22

tabela_linhas <- read_xlsx("data-raw/route_code_gps.xlsx")


sentido <- function(linha){

cat(sprintf("Lendo GPS bruto da linha %s \n", linha))
gps <- read_parquet(sprintf("data-raw/gps_linha/routecode=%s/part-0.parquet", linha))

gps[, timestamp := lubridate::with_tz(timestamp, tzone = "America/Fortaleza")]
gps[, dia := lubridate::day(timestamp)]
setorder(gps, vehicle_id, dia, timestamp)
gps[, vseq := seq_len(.N), by = c("vehicle_id", "dia")]

gps_sf <- gps %>% st_as_sf(coords = c("longitude", "latitude"), crs = 4326, remove = FALSE)

# teste <- gps_sf[1:10000,]

rm(gps)

gtfs <- read_gtfs("data-raw/GTFS_start_202503.zip")

linha_certa <- tabela_linhas %>% filter(as.numeric(cod_integracao_externo) == linha) %>%
  pull(num_linha) %>% unique() %>% as.numeric()

gtfs_linha <- gtfs %>% filter_by_route_id(str_pad(linha_certa, width = 4, side = "left", pad = "0"))
# gtfs_linha <- gtfs %>% filter_by_route_id("0016")
rm(gtfs)

stops_linha <- gtfstools::convert_stops_to_sf(gtfs_linha)
shapes_linha <- gtfstools::convert_shapes_to_sf(gtfs_linha)


#Encerrar a função em caso de linha inexistente no GTFS
if (length(linha_certa)<1){
  
  return(paste0("Linha ", linha, " não existe na Planilha"))
  break
  
}

if (length(linha_certa) > 0 & nrow(stops_linha)<=0){
  
  cat(paste0("\n Linha ", linha_certa, " não encontrada no GTFS \n"))
  #
  
  return(paste0("Linha ", linha, " não existe no GTFS/Planilha"))
  break
}




# mapview(teste, zcol = "vseq") + mapview(shapes_linha, color = "darkred")
# Combinar os shapes de ida e volta e criar um buffer
# trip_id = 0 é ida, trip_id = 1 é volta
shapes_buffer <- gtfstools::convert_shapes_to_sf(gtfs_linha) %>%
  st_transform(31984) %>%
  st_buffer(50) %>% st_union() %>% st_as_sf() %>% mutate(line = linha)
# mapview(shapes)

# Filtrar os pontos GPS
gps_na_rota_sf <- st_filter(gps_sf %>% st_transform(31984),
                          shapes_buffer)
rm(shapes_buffer)

cat(sprintf("Identificando sentido dos registros da linha %s \n",linha))
# 1. Identificar os trip_ids para cada sentido
trips_dt <- as.data.table(gtfs_linha$trips)
ida_trip_ids <- trips_dt[grepl("I", shape_id, ignore.case = TRUE), trip_id]
volta_trip_ids <- trips_dt[grepl("V", shape_id, ignore.case = TRUE), trip_id]


# 2. Obter as sequências de paradas para cada sentido
stop_times_dt <- as.data.table(gtfs_linha$stop_times)


# Paradas e suas sequências para viagens de IDA
stops_ida_info <- stop_times_dt[trip_id %in% ida_trip_ids, .(stop_id, stop_sequence)] |> unique()

# Paradas e suas sequências para viagens de VOLTA
stops_volta_info <- stop_times_dt[trip_id %in% volta_trip_ids, .(stop_id, stop_sequence)] |> unique()

#Encerrar funçaõ em caso de linha circular
if (nrow(stops_ida_info %>% filter(stop_sequence == 1))>1 |
    nrow(stops_volta_info %>% filter(stop_sequence == 1))>1 |
    nrow(stops_ida_info)<=0 |
    nrow(stops_volta_info)<=0 ){
  
  return(paste0("Linha ", linha, " não processada - linha Circular \n ou ausente itinerario de ida/volta"))
  break
}


rm(stop_times_dt)

# 3. Criar os objetos SF finais para cada sentido, juntando com a geometria
stops_ida_sf <- merge(stops_linha, stops_ida_info, by = "stop_id")
stops_volta_sf <- merge(stops_linha, stops_volta_info, by = "stop_id")

# --- Snap para as paradas de IDA ---
indices_ida <- st_nearest_feature(gps_na_rota_sf,
                                  stops_ida_sf |> st_transform(31984))
dist_ida <- st_distance(gps_na_rota_sf,
                        stops_ida_sf[indices_ida, ] |> st_transform(31984),
                        by_element = TRUE)

# --- Snap para as paradas de VOLTA ---
indices_volta <- st_nearest_feature(gps_na_rota_sf,
                                    stops_volta_sf |> st_transform(31984))
dist_volta <- st_distance(gps_na_rota_sf,
                          stops_volta_sf[indices_volta, ] |> st_transform(31984),
                          by_element = TRUE)

# Adicionar os resultados ao data frame
gps_na_rota_sf$ida_stop_id <- stops_ida_sf$stop_id[indices_ida]
gps_na_rota_sf$ida_stop_seq <- stops_ida_sf$stop_sequence[indices_ida]
gps_na_rota_sf$ida_dist <- dist_ida

rm(dist_ida, indices_ida); gc()

gps_na_rota_sf$volta_stop_id <- stops_volta_sf$stop_id[indices_volta]
gps_na_rota_sf$volta_stop_seq <- stops_volta_sf$stop_sequence[indices_volta]
gps_na_rota_sf$volta_dist <- dist_volta

rm(dist_volta, indices_volta); gc()
# Converter para data.table
gps_analise_dt <- as.data.table(st_drop_geometry(gps_na_rota_sf))

rm(gps_na_rota_sf)

max_seq_ida <- max(stops_ida_sf$stop_sequence)
max_seq_volta <- max(stops_volta_sf$stop_sequence)

setorder(gps_analise_dt, vehicle_id, dia, timestamp)

#LÓGICA DE IDENTIFICAÇÃO DO SENTIDO DA VIAGEM

# --- PASSO 1 ---

# 1.1. CalculAR a direção instantânea do movimento (1 para frente, -1 para trás, 0 para parado)
# Usamos sign() para capturar apenas a direção da mudança, não a magnitude.
gps_analise_dt[, `:=`(
  dir_ida   = sign(ida_stop_seq - shift(ida_stop_seq)),
  dir_volta = sign(volta_stop_seq - shift(volta_stop_seq))
), by = .(vehicle_id, dia)]

# 1.2. Suavizar o ruído calculando a tendência em uma janela móvel (sugerido: 5 pontos = ~2.5 minutos)
# frollmean() calcula a média da direção nos últimos 5 pontos.
# Se a média for positiva, a tendência é de avanço. Se for negativa, de ré.

window_size <- 5

gps_analise_dt[, `:=`(
  tendencia_ida   = frollmean(dir_ida, n = window_size, align = "center", fill = 0, na.rm = TRUE),
  tendencia_volta = frollmean(dir_volta, n = window_size, align = "center", fill = 0, na.rm = TRUE)
), by = .(vehicle_id, dia)]

# 1.3. Definir a progressão OK com base na TENDÊNCIA, não no movimento instantâneo.
# Usamos um pequeno limiar (ex: 0.1) para garantir que o movimento seja consistente.
limiar_tendencia <- 0.1


gps_analise_dt[, `:=`(
  ida_ok   = tendencia_ida > limiar_tendencia,
  volta_ok = tendencia_volta > limiar_tendencia
)]


# 1.4 Decisão Primária: Casos não ambíguos baseados na progressão
# Se um progride e o outro não, a decisão é clara.
gps_analise_dt[, sentido := fcase(
  ida_ok & !volta_ok, "I",
  volta_ok & !ida_ok, "V",
  default = NA_character_
)]


# --- PASSO 2: Ancoragem do Fim da Viagem ---
# Criar um ID para cada bloco contíguo de NAs para podermos trabalhar neles
gps_analise_dt[, na_block_id := rleid(is.na(sentido))]

# Propagar o último sentido conhecido para termos o contexto de qual viagem está terminando
gps_analise_dt[, sentido_anterior := na.locf(sentido, na.rm = FALSE), by = .(vehicle_id, dia)]


# Encontrar os índices das linhas que serão as âncoras
indices_ancora <- gps_analise_dt[is.na(sentido), {
  
  # A CONDIÇÃO CORRIGIDA:
  # Adicionamos a verificação !is.na() para ignorar os grupos 
  # onde o sentido anterior é desconhecido (o início do dia).
  if (!is.na(.BY$sentido_anterior) && .BY$sentido_anterior == "I" && any(ida_stop_seq == max_seq_ida, na.rm = TRUE)) {
    .(anchor_row = .I[which.min(ida_dist)])
  } else if (!is.na(.BY$sentido_anterior) && .BY$sentido_anterior == "V" && any(volta_stop_seq == max_seq_volta, na.rm = TRUE)) {
    .(anchor_row = .I[which.min(volta_dist)])
  }
  
}, by = .(vehicle_id, dia, sentido_anterior, na_block_id)]$anchor_row


# Atribuir o sentido correto APENAS nos pontos âncora (código inalterado)
gps_analise_dt[indices_ancora, sentido := sentido_anterior]


# --- PASSO 3: Preenchimento Final para Trás (NOCB) ---

# 3.1 Agora que os finais de viagem estão ancorados, preenchemos o resto.
# Isso vai preencher os pontos de espera que levam à âncora e o início da próxima viagem.
gps_analise_dt[, sentido := na.locf(sentido, fromLast = TRUE, na.rm = FALSE), by = .(vehicle_id, dia)]

# 3.2 Preenchimento Final para Frente (LOCF) - O PASSO ADICIONADO
# Após o passo anterior, os únicos NAs restantes só podem estar no fim do dia.
# Esta linha preenche esses últimos pontos com o último sentido válido conhecido.
gps_analise_dt[, sentido := zoo::na.locf(sentido, na.rm = FALSE), by = .(vehicle_id, dia)]

# Limpeza das colunas auxi1liares
# não realizado aqui para debugg
# gps_analise_dt[, c("na_block_id", "sentido_anterior", "progresso_ida", "progresso_volta", "ida_ok", "volta_ok") := NULL]

# Uma nova viagem começa quando o 'sentido' (I/V) muda.
gps_analise_dt[, mudou_sentido := sentido != shift(sentido, type = "lag"), by = .(vehicle_id, dia)]
gps_analise_dt[is.na(mudou_sentido), mudou_sentido := TRUE] # O primeiro registro é sempre um início


#--- Criação da codificação da viagem

# PASSO 1: Criar o ID numérico sequencial para a viagem
# Isso garante que a coluna 'trip_seq_dia' exista e seja numérica.
gps_analise_dt[, trip_seq_dia := cumsum(mudou_sentido), by = .(vehicle_id, dia)]


# PASSO 2: Criar o ID final de texto em uma NOVA coluna
# Usamos um nome diferente, como 'viagem_id', para evitar o conflito de tipos.
# Usamos paste() com um separador "_" para melhor legibilidade.
gps_analise_dt[, viagem_id := paste(
  linha,                          
  vehicle_id,
  format(as.Date(timestamp), "%Y%m%d"),
  sentido,
  trip_seq_dia,
  sep = "_"
)]


#-------remover viagens muito curtas -> provavel manobras para garagem

# --- PASSO 1: Calcular o Comprimento de Cada Viagem Identificada ---

# Primeiro, precisamos juntar os IDs de viagem de volta ao objeto espacial
gps_viagens_sf <- merge(gps_sf, 
                        gps_analise_dt[, .(vseq, vehicle_id, dia, viagem_id, sentido)], 
                        by = c("vseq", "vehicle_id", "dia"))

rm(gps_sf); gc()
# Agora, para cada viagem, criamos uma linha (LINESTRING) e calculamos seu comprimento
# Este processo agrupa os pontos de cada viagem em uma única geometria de linha
# trip_lengths <- gps_viagens_sf %>%
#   group_by(viagem_id, sentido) %>%
#   arrange(timestamp) %>%
#   summarise(
#     geometry = st_combine(geometry) %>% st_cast("LINESTRING"),
#     .groups = 'drop'
#   ) %>%
#   mutate(
#     comprimento_viagem = st_length(geometry)
#   )
trip_lengths <- gps_viagens_sf %>%
  group_by(viagem_id, sentido) %>%
  # PASSO ADICIONADO: Mantém apenas os grupos com mais de 1 ponto
  filter(n() > 1) %>% 
  arrange(timestamp) %>%
  summarise(
    geometry = st_combine(geometry) %>% st_cast("LINESTRING"),
    .groups = 'drop'
  ) %>%
  mutate(
    comprimento_viagem = st_length(geometry)
  )

rm(gps_viagens_sf)
# --- PASSO 2: Obter o Comprimento das Rotas (GTFS) ---

# Calculamos o comprimento de uma rota de Ida e uma de Volta a partir dos shapes do GTFS
# Pegamos a primeira, pois geralmente são idênticas em comprimento
comprimento_shape_ida <- st_length(gtfstools::convert_shapes_to_sf(gtfs_linha) %>% filter(shape_id %like% "I"))
comprimento_shape_volta <- st_length(gtfstools::convert_shapes_to_sf(gtfs_linha) %>% filter(shape_id %like% "V"))


# --- PASSO 3: Identificar e Filtrar as Viagens Válidas ---

# Converter para data.table para a lógica de filtro
trip_lengths_dt <- as.data.table(trip_lengths)

# Comparamos o comprimento de cada viagem com o comprimento oficial do seu sentido
trip_lengths_dt[, comprimento_oficial := fifelse(sentido == "I", comprimento_shape_ida, comprimento_shape_volta)]
trip_lengths_dt[, is_valida := comprimento_viagem >= (0.30 * comprimento_oficial)]

# Pegamos os IDs apenas das viagens que passaram no teste
viagens_validas_ids <- trip_lengths_dt[is_valida == TRUE, viagem_id]

# Filtramos o data frame principal para manter apenas os registros dessas viagens
gps_final_dt <- gps_analise_dt[viagem_id %in% viagens_validas_ids]

rm(gps_analise_dt)
# --- PASSO 4: Renumerar as Viagens Válidas ---

# Após o filtro, a sequência de viagens (ex: 1, 3, 4, 7...) precisa ser corrigida.
setorder(gps_final_dt, vehicle_id, dia, timestamp)

# A 'mudança de viagem' agora é simplesmente quando o 'viagem_id' antigo muda
gps_final_dt[, mudou_viagem := viagem_id != shift(viagem_id, type="lag"), by = .(vehicle_id, dia)]
gps_final_dt[is.na(mudou_viagem), mudou_viagem := TRUE]

# Criamos uma nova coluna com a sequência correta (ex: 1, 2, 3, 4...)
gps_final_dt[, trip_seq_final := cumsum(mudou_viagem), by = .(vehicle_id, dia)]

# Finalmente, criamos o ID final e limpo com a nova sequência
gps_final_dt[, viagem_id_final := paste(
  linha,
  vehicle_id,
  format(as.Date(timestamp), "%Y%m%d"),
  sentido,
  trip_seq_final,
  sep = "_"
)]

#-- Alguns testes para verificar a qualidade dos resultados:

# teste00 <- gps_final_dt[timestamp <= as.POSIXct("2025-03-01 23:59:59", tz = "America/Fortaleza") & timestamp >= as.POSIXct("2025-03-01 00:00:00", tz = "America/Fortaleza")]
# 
# # teste <- gps_analise_dt %>% distinct(viagem_id, .keep_all = T)
# 
# 
# # teste0 <- teste0 %>% mutate(hora = lubridate::hour(timestamp)) %>% distinct(viagem_id, .keep_all = T)
# 
# # teste2 <- teste0 %>% group_by(hora) %>%
#   # summarise(n = n())
# 
# # unique(teste0$viagem_id)
# 
# 
# # teste3 <- teste0 %>% filter(viagem_id == "75_58007_20250302_NA_18")
# # teste4 <- teste0 %>% filter(is.na(sentido)==T)
# 
# # teste5 <- teste0 %>% filter(vehicle_id == 58007)
# 
# 
# teste_plot <- teste00 %>% st_as_sf(coords = c("longitude", "latitude"), crs = 4326)
# 
# # verificar <- teste_plot %>% select(timestamp, vseq, ida_stop_id, ida_stop_seq, ida_dist,
# #                                    volta_stop_id, volta_stop_seq, volta_dist,
# #                                    progresso_ida, progresso_volta, sentido, sentido_anterior,
# #                                    mudou_sentido, viagem_id)
# 
# unique(teste_plot$viagem_id_final)
# 
# teste_plot %>% filter(vehicle_id == 43703) %>% distinct(viagem_id_final)
# # trip_view <- teste_plot %>% filter(viagem_id_final == "75_58007_20250301_V_1")
# #
# # trip_view2 <- teste_plot %>% filter(viagem_id_final == "75_58007_20250301_I_4")
# # trip_view3 <- teste_plot %>% filter(viagem_id_final == "75_58007_20250301_V_7")
# 
# ida <- gtfstools::convert_shapes_to_sf(gtfs_linha) %>%
#   filter(shape_id == paste0("shape",str_pad(linha_certa, width = 4, side = "left", pad = "0"),"-","I"))
# volta <- gtfstools::convert_shapes_to_sf(gtfs_linha) %>%
#   filter(shape_id == paste0("shape",str_pad(linha_certa, width = 4, side = "left", pad = "0"),"-","V"))
# 
# trip_view <- teste_plot %>% filter(viagem_id_final == "507_43295_20250301_I_3")
# trip_view2 <- teste_plot %>% filter(viagem_id_final == "507_43295_20250301_V_4")
# 
# mapview(ida)+ mapview(trip_view, zcol = "vseq") + mapview(stops_ida_sf, color = "darkred") +
# 
# mapview(volta)+ mapview(trip_view2, zcol = "vseq") + mapview(stops_volta_sf, color = "darkred")
# 
# # mapview(ida)+ mapview(volta)+ mapview(trip_view, zcol = "vseq") + mapview(trip_view2, zcol = "vseq") + mapview(trip_view3, zcol = "vseq")
# 
# #testes com os dados completos de um veículo para um dia específico
# 
# #   veiculo_completo <- gps_sf %>% filter(vehicle_id == 20250301) %>%
# #   filter(timestamp <= as.POSIXct("2025-03-01 06:44:59", tz = "America/Fortaleza") & timestamp >= as.POSIXct("2025-03-01 00:00:00", tz = "America/Fortaleza"))
# # mapview(veiculo_completo, zcol = "vseq") + mapview(ida)+ mapview(volta)


#SELEÇÃO DAS COLUNAS A SEREM MANTIDAS NO DATAFRAME PARA SALVAMENTO


# Limpeza das colunas auxi1liares

gps_final_dt[, stop_id := ifelse(sentido == "I",
                                 ida_stop_id,
                                 volta_stop_id)]
gps_final_dt[, stop_sequence := ifelse(sentido == "I",
                                       ida_stop_seq,
                                       volta_stop_seq)]
gps_final_dt[, stop_distance := ifelse(sentido == "I",
                                       ida_dist,
                                       volta_dist)]

# names(gps_final_dt)

gps_final_dt[, c("ida_stop_id",
                 "ida_stop_seq",
                 "ida_dist",
                 "volta_stop_id",
                 "volta_stop_seq",
                 "volta_dist",
                 "dir_ida",
                 "dir_volta",
                 "tendencia_ida",
                 "tendencia_volta",
                 "ida_ok",
                 "volta_ok",
                 "na_block_id",
                 "sentido_anterior",
                 "mudou_sentido",
                 "trip_seq_dia",
                 "viagem_id",
                 "mudou_viagem",
                 "trip_seq_final") := NULL]

mes <- lubridate::month(gps_final_dt$timestamp[1])

dir.create(sprintf("data-raw/gps_linha_tratado/%s", mes), recursive = T, showWarnings = F)

cat(sprintf("Salvando GPS tratado da linha %s do mês %s \n", linha_certa, mes))
write_parquet(gps_final_dt,sprintf("data-raw/gps_linha_tratado/%s/gps_%s.parquet",
                                   mes,linha_certa))
return(paste0("Successful ", linha))

}

sentido_safe <- possibly(.f = sentido, otherwise = paste0("Failed ",linha))
# safely()

# sentido_safe(linha = 11)


future::plan(multisession(workers = 12))

lista_linhas <- sort(as.numeric(na.omit(str_match(list.dirs("data-raw/gps_linha/"),
                                     "routecode=(\\d+)")[, 2])))

feitos <- sort(as.numeric(na.omit(str_match(list.files("data-raw/gps_linha_tratado/3/"),
                                            "gps_(\\d+)")[, 2])))

feitos_externo <- data.frame(num_linha = feitos) %>%
  left_join(tabela_linhas %>% dplyr::select(num_linha, cod_integracao_externo) %>%
              mutate(num_linha = as.numeric(num_linha)),
            by = "num_linha") %>% pull(cod_integracao_externo) %>% sort()

a_fazer <- lista_linhas[lista_linhas %nin% feitos_externo]

# furrr::future_walk(.f = sentido_safe, .x = lista_linhas)
# furrr::future_walk(.f = sentido_safe, .x = a_fazer)
walk(.x = a_fazer[2:length(a_fazer)], .f = sentido_safe)



