library(dplyr)
library(dbplyr)
library(stringr)
library(purrr)
library(tidytext)
library(ggplot2)
library(ftbbqr)
library(udpipe)
library(topicmodels)
library(textmineR)
library(readr)
connection <- ftdata_connection(billing = "ft-data-science")
options(scipen = 20)
#source(here::here("src", "R"  ,"cluster_labels_research_funcs.R"))

run_date <- Sys.Date()
time_window <- 90
start_date <- run_date - time_window
word_threshold <- 100


# We first gather article text data and link it with cluster/text

# Gather cluster-article data
data_cluster_article <- ftbbqr::source_bq_tbl(
  connection = connection,
  dataset = "ft-data-science",
  schema = "latest",
  table = "article_cluster_dist_matrix") %>%
  mutate(run_date = as.Date(inserted_dtm)) %>% 
  filter(between(.data$run_date, start_date, run_date)) %>% 
  filter(cluster_rank == 1) %>% 
  distinct(article_uuid, cluster_id, run_date)
# Get article text of related clusters
article_text_cluster <- data_cluster_article %>% 
  mutate(article_uuid = lower(article_uuid)) %>% 
  mutate(article_uuid = gsub(" ", "", article_uuid)) %>% 
  ungroup() %>% 
  left_join(
    ftbbqr::source_bq_tbl(
      connection = connection,
      dataset = "ft-data",
      schema = "dwpresentation",
      table = "dim_content_latest") %>% 
      mutate(content_id = lower(content_id)) %>% 
      mutate(content_id = gsub(" ", "", content_id)),
    by = c("article_uuid" = "content_id")) %>%
  ungroup() %>% 
  collect() %>% 
  ungroup() %>% 
  # remove columnist contact/follow details
  mutate(article_fulltext = stringr::str_remove_all(
    article_fulltext,
    "<p><em>Email.+$|\\>.+\\@ft.com.+$")) %>% 
  filter(
    !is.na(article_uuid),
    article_uuid != "",
    article_uuid != " ") %>% 
  select(cluster_id, article_uuid, article_fulltext, title)

write_csv(article_text_cluster, "clusters_text.csv")