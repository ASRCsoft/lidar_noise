## move intermediate datasets to /data

library(usethis)

cnr_stats = read.csv('cnr_stats.csv')
hmrf_stats = read.csv('hmrf_stats.csv')

usethis::use_data(cnr_stats, hmrf_stats, overwrite = TRUE)
