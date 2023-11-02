library(dplyr)
library(lubridate)

####
# Function to compute warm spells
trle <- function(x, l, v){
  rle_list <- rle(x >= v)

  rle_length <- rle_list$lengths

  res <- length(rle_length[rle_length >= l])

  return(res)
}
###

teste <- data.frame(
  x = c(1,1,2,3,3,4,5,5)
)

trle(x = teste$x, l = 2, v = 3)



######################

tmax <- arrow::open_dataset(sources = "../brclim2/output_data/parquet/2m_temperature_max.parquet")

tmax %>% head() %>% collect()

# TX90p climatological normal
tx90p <- tmax %>%
  filter(name == "2m_temperature_max_mean") %>%
  filter(date >= as.Date("1961-01-01") & date <= as.Date("1990-12-31")) %>%
  collect() %>%
  group_by(code_muni) %>%
  arrange(date) %>%
  mutate(mvga = zoo::rollmean(value, k = 5, fill=NA, align = "right")) %>%
  summarise(tx90p = quantile(mvga, .9, na.rm = TRUE, names = FALSE)) %>%
  ungroup() %>%
  arrange(code_muni)


# Warm speel
tmax %>%
  filter(name == "2m_temperature_max_mean") %>%
  head(10000) %>%
  collect() %>%
  group_by(code_muni) %>%
  arrange(date) %>%
  summarise(hw = trle(value, 6, 305)) %>%
  ungroup()


tmax_groups <- tmax %>%
  filter(name == "2m_temperature_max_mean") %>%
  select(code_muni, date, value) %>%
  head(10000) %>%
  collect() %>%
  group_by(code_muni)

normals <- left_join(group_keys(tmax_groups), tx90p, by = "code_muni")

data_and_normals <- list(
  x = group_split(tmax_groups),
  l = 6,
  v = normals$tx90p
)

teste <- purrr::pmap(.l = data_and_normals, .f = function(x,l,v){
  trle(x = purrr::pluck(x, 1)$value, l = l, v = v)
})




# Others
teste <- tmax %>%
  filter(name == "2m_temperature_max_mean") %>%
  mutate(
    year = year(date),
    month = month(date)
  ) %>%
  group_by(code_muni, year, month) %>%
  summarise(
    SU = sum(value >= 25+273.15),
    ID = sum(value <= 0+273.15)
  ) %>%
  ungroup() %>%
  collect()






############

trle2 <- function(x, i, l, v){
  rle_list <- rle(x)

  rle_df <- data.frame(
    length = rle_list$lengths,
    value = rle_list$values
  )

  rle_df$pos_2 <- cumsum(rle_df$length)
  rle_df$pos_1 <- rle_df$pos_2 - rle_df$length + 1

  rle_df <- data.frame(
    pos_1 = i[rle_df$pos_1],
    pos_2 = i[rle_df$pos_2],
    length = rle_df$length,
    value = rle_df$value
  )

  res <- subset(rle_df, length >= l & value >= v)

  return(res)
}
