#using cronjob to automatically connect with twitter streaming API once an hour
library("methods")
library("bitops")
library("RCurl")
library("rjson")
library("streamR")
library("ROAuth")

#load authentification data coming from streamtwitter.R

load("R/my_oauth.Rdata")
current.time <- format(Sys.time(), "%Y_%m_%d_%H_%M")
file.name <- paste("/data/twitterdata/worldtweet", current.time, ".json", sep="")

#scrape tweets from all the world
#getting bounding box metadata from this link http://boundingbox.klokantech.com
tweets <- filterStream( file=file.name,locations=c(-180.0,-85.0,180.0,85.0), timeout=60, oauth=my_oauth )

#load spark library into R
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  #Sys.setenv(SPARK_HOME = "/data/spark")
  Sys.setenv(SPARK_HOME = "/usr/local/Cellar/apache-spark/2.2.0/libexec")
}

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
#sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "50g",spark.executorEnv.JAVA_HOME="/data/lin/lib/jvm/java-8-openjdk-amd64/jre"))

#start spark session
sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "50g"))
source("R/func.R")


#parse json format data to parquet format

write_parquet_user <- function(raw_tweet){
  readTweet <- streamR::readTweets(raw_tweet)
  twitter_data = data.frame(
    user_id = unlistWithNA(readTweet,c('user','id_str')),
    user_name = unlistWithNA(readTweet,c('user','screen_name')),
    full_name = unlistWithNA(readTweet,c('user','name')),
    user_description = unlistWithNA(readTweet,c('user','description')),
    user_location = unlistWithNA(readTweet,c('user','location')),
    followers_count = unlistWithNA(readTweet,c('user','followers_count')),
    follows_count = unlistWithNA(readTweet,c('user','friends_count')),
    status_all_count = unlistWithNA(readTweet,c('user','statuses_count')),
    listed_count = unlistWithNA(readTweet, c('user', 'listed_count')),
    utc_offset = unlistWithNA(readTweet,c('user','utc_offset')),
    time_zone = unlistWithNA(readTweet,c('user','time_zone')),
    user_lang = unlistWithNA(readTweet,c('user','lang')),
    geo_enabled = unlistWithNA(readTweet,c('user','geo_enabled')),
    user_protected = unlistWithNA(readTweet,c('user','protected')),
    verified = unlistWithNA(readTweet, c('user', 'verified')),
    media_id = unlistWithNA(readTweet,'id_str'),
    text = unlistWithNA(readTweet,'text'),
    create_time = unlistWithNA(readTweet,'created_at'),
    lang= unlistWithNA(readTweet,'lang')
)
  twitter_data$country_code <- unlistWithNA(readTweet, c('place', 'country_code'))
  twitter_data$country <- unlistWithNA(readTweet, c('place', 'country'))
  twitter_data$place_type <- unlistWithNA(readTweet, c('place', 'place_type'))
  twitter_data$place_full_name <- unlistWithNA(readTweet, c('place', 'full_name'))
  twitter_data$place_name <- unlistWithNA(readTweet, c('place', 'name'))
  twitter_data$place_id <- unlistWithNA(readTweet, c('place', 'id'))
  place_lat_1 <- unlistWithNA(readTweet, c('place', 'bounding_box', 'coordinates', 1, 1, 2))
  place_lat_2 <- unlistWithNA(readTweet, c('place', 'bounding_box', 'coordinates', 1, 2, 2))
  twitter_data$place_lat <- sapply(1:length(readTweet), function(x)
    mean(c(place_lat_1[x], place_lat_2[x]), na.rm=TRUE))
  place_lon_1 <- unlistWithNA(readTweet, c('place', 'bounding_box', 'coordinates', 1, 1, 1))
  place_lon_2 <- unlistWithNA(readTweet, c('place', 'bounding_box', 'coordinates', 1, 3, 1))
  twitter_data$place_lon <- sapply(1:length(readTweet), function(x) 
    mean(c(place_lon_1[x], place_lon_2[x]), na.rm=TRUE))
  #Object geo deprecated Nullable use the coordinates field instead.
  #Tweets with a Point coordinate come from GPS enabled devices, and represent the exact GPS location of the Tweet in question
  twitter_data$lat <- unlistWithNA(readTweet, c('coordinates', 'coordinates', 2))
  twitter_data$lon <- unlistWithNA(readTweet, c('coordinates', 'coordinates', 1))
  #twitter_data$lat <- unlistWithNA(readTweet, c('geo', 'coordinates', 1))
  #twitter_data$lon <- unlistWithNA(readTweet, c('geo', 'coordinates', 2))
  twitter_data$expanded_url <- unlistWithNA(readTweet, c('entities', 'urls', 1, 'expanded_url'))
  twitter_data$url <- unlistWithNA(readTweet, c('entities', 'urls', 1, 'url'))
  
  twitter_df <- as.DataFrame(twitter_data)
  current.time <- format(Sys.time(), "%Y_%m_%d_%H_%M")
  twitter_path <- paste("/data/twitterdata/twitter",current.time,'.parquet',sep = '')
  write.df(twitter_df, path = twitter_path, source = "parquet", mode = "overwrite")
  print("successfully parse data to parquet file")
}

write_parquet_user(file.name)
sparkR.session.stop()
