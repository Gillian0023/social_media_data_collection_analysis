library('bitops')
library('RCurl')
library('rjson')
#library('RJSONIO')
library('anytime')
library('streamR')

#load spark library into R
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  #Sys.setenv(SPARK_HOME = "/data/spark")
  Sys.setenv(SPARK_HOME = "/usr/local/Cellar/apache-spark/2.2.0/libexec")
}
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
#sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "50g",spark.executorEnv.JAVA_HOME="/data/lin/lib/jvm/java-8-openjdk-amd64/jre"))
sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "50g"))
source("R/func.R")


#get detailed media information using one media url

get_detail_media <- function(twitter_pointer){
  #media_url <- paste("https://www.instagram.com/p/",short_code,"/?__a=1",sep = "")
  #use tryCatch to handle errors which result from getURL().e.g.Can’t open file(con,”f”) connection or SSLRead() return error -9806 
  tryCatch({
    media_url <- paste(twitter_pointer,"?__a=1",sep = "")
    media_get <-RCurl::getURL(url = media_url)
    media_json <- rjson::fromJSON(media_get) 
    return (media_json$graphql$shortcode_media)
  }, error=function(e)
  { print(e)
    return(NULL)}
  )
}


#get detailed user information using one user_name

get_detail_user <- function(user_name){
  user_url <- paste("https://www.instagram.com/",user_name,"/?__a=1",sep = "")
  tryCatch({
    user_get <-RCurl::getURL(url = user_url)
    user_json <- rjson::fromJSON(user_get) 
    return (user_json$user)
  }, error=function(e)
  { print(e)
    return(NULL)}
  )
}

#parse json format data to parquet format

write_parquet_user <- function(user){
  user_data = data.frame(
  user_id = unlistWithNA(user,'id'),
  user_name = unlistWithNA(user,'username'),
  full_name = unlistWithNA(user,'full_name'),
  user_description = unlistWithNA(user,'biography'),
  followers_count = unlistWithNA(user,c('followed_by','count')),
  follows_count = unlistWithNA(user,c('follows','count')),
  status_all_count = unlistWithNA(user,c('media','count'))
  )
  
  instagram_user_spark <- as.DataFrame(user_data)
  current.time <- format(Sys.time(), "%Y_%m_%d_%H_%M")
  user_path <- paste("/data/instagramdata/instagram_user",current.time,'.parquet',sep = '')
  #user_path <- paste("Data/instagramdata/user",current.time,'.parquet',sep = '')
  write.df(instagram_user_spark, path = user_path, source = "parquet", mode = "overwrite")
}

#read json format tweet data

read_tweet <- function(file_name_tweet){
  readTweet <- streamR::readTweets(file_name_tweet)
  url <- unlistWithNA(readTweet,c('entities', 'urls', 1, 'expanded_url'))
  instagram_url <- url[grepl("instagram",url)]
  #instagram_url <- sub("https://www.instagram.com/p/.*/",".*",instagram_url)
  instagram_url <- paste(instagram_url,'?__a=1',sep = '')
  #lapply(instagram_url,get_instagram_data)
  return (instagram_url)
}



user_name_all_df <- read.df('/data/instagramdata/user_name_all.parquet', "parquet", mergeSchema = "true")
user_name_all_dataframe <- collect(user_name_all_df)
query_user_name_all_df <- read.df('/data/instagramdata/query_user_name_all.parquet', "parquet", mergeSchema = "true")
query_user_name_all_dataframe <- collect(query_user_name_all_df)
user_name_all <- user_name_all_dataframe[[1]]
query_user_name_all <- query_user_name_all_dataframe[[1]]

instagram_url <- read_parquet("/data/twitterdata/instagram_url.parquet")
instagram_url <- instagram_url$expanded_url
print(length(instagram_url))
instagram_url1 <- base::sample(instagram_url,5000)

media <- spark.lapply(instagram_url1,get_detail_media)


print(length(media))
user_name <- unlist(lapply(media,function(x) x$owner$username))
print(length(user_name))
user_name <- notreplica_item(user_name)
print(length(user_name))
user_name <- notreplica_vect(user_name,query_user_name_all)
print(length(user_name))

user <- spark.lapply(user_name,get_detail_user)
user <- user[unlist(lapply(user,function(x) !is.null(x) && x[['is_private']]==FALSE),use.names = TRUE)]

print(length(user))
write_parquet_user(user)

query_user_name <- unlist(lapply(user,function(x) x[['username']]),use.names = TRUE)
query_user_name_all <- append(query_user_name_all,query_user_name)
print(length(query_user_name_all))
user_name_all <- append(user_name_all,user_name)
##user_name_all <- notreplica_vect(user_name,user_name_all)
print(length(user_name_all))

user_name_all <- as.list(user_name_all)
user_name_all_df <- as.DataFrame(user_name_all)
query_user_name_all <- as.list(query_user_name_all)
query_user_name_all_df <- as.DataFrame(query_user_name_all)
##current.time <- format(Sys.time(), "%Y_%m_%d_%H_%M")
user_name_all_path <- "/data/instagramdata/user_name_all.parquet"
query_user_name_all_path <- "/data/instagramdata/query_user_name_all.parquet"

write.df(user_name_all_df, path = user_name_all_path, source = "parquet", mode = "overwrite")
write.df(query_user_name_all_df, path = query_user_name_all_path, source = "parquet", mode = "overwrite")

sparkR.session.stop()










