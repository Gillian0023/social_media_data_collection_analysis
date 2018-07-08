library('bitops')
library('RCurl')
library('rjson')
#library('RJSONIO')
library('anytime')
library('streamR')

if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "/data/spark")
  #Sys.setenv(SPARK_HOME = "/usr/local/Cellar/apache-spark/2.2.0/libexec")
}
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "50g",spark.executorEnv.JAVA_HOME="/data/lin/lib/jvm/java-8-openjdk-amd64/jre"))
#sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "50g"))
source("/data/LinJi_thesis/R/func.R")

get_detail_location <- function(location_id){
  location_url <- paste("https://www.instagram.com/explore/locations/",location_id,"/?__a=1",sep = "")
  tryCatch(
    {
    location_get <-RCurl::getURL(url = location_url)
    location_json <- rjson::fromJSON(location_get) 
    return (location_json$graphql$location)
  }, error=function(e)
  { print(e)
    return(NULL)}
  )
}

write_parquet_location <- function(location){
  location_data = data.frame(
  location_id = unlistWithNA(location,'id'),
  location_name = unlistWithNA(location,'name'),
  lat = unlistWithNA(location,'lat'),
  lon = unlistWithNA(location,'lng'),
  country_id = unlistWithNA(location,c('directory','country','id')),
  country_name = unlistWithNA(location,c('directory','country','name')),
  city_id = unlistWithNA(location,c('directory','city','id')),
  city_name = unlistWithNA(location,c('directory','city','name')))
  instagram_location_spark <- as.DataFrame(location_data)
  current.time <- format(Sys.time(), "%Y_%m_%d_%H_%M")
  #location_path <- paste("Data/instagramdata/instagram_location",current.time,'.parquet',sep = '')
  location_path <- paste("/data/instagramdata/instagram_location",current.time,'.parquet',sep = '')
  write.df(instagram_location_spark, path = location_path, source = "parquet", mode = "append")
}

file_name_media <- "/data/instagramda*/instagram_media2018*.parquet"
location_data_frame <- read_parquet(file_name_media)
location_id <- location_data_frame$location_id
print(length(location_id))
location_id <- notreplica_item(location_id)
print(length(location_id))
current_location_id_df <- read.df('/data/instagramdata/instagram_location2018*.parquet', "parquet", mergeSchema = "true")
current_location_id_dataframe <- collect(current_location_id_df)
current_location_id <- current_location_id_dataframe[[1]]
location_id <- notreplica_vect(location_id,current_location_id)

#location_id <- location_id[1:10]
location <- spark.lapply(location_id,get_detail_location)
location <- location[unlist(lapply(location,function(x) !is.null(x)),use.names = TRUE)]
print(length(location))
write_parquet_location(location)

sparkR.session.stop()

