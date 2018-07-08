library('bitops')
library('RCurl')
library('rjson')
#library('RJSONIO')
library('anytime')
library('streamR')

if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  #Sys.setenv(SPARK_HOME = "/data/spark")
  Sys.setenv(SPARK_HOME = "/usr/local/Cellar/apache-spark/2.2.0/libexec")
}
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "50g"))
source("R/func.R")

read_parquet_user <- function(file_name_user){
  Sys.sleep(0.1)
  user_df <- read.df(file_name_user, "parquet", mergeSchema = "true")
  ##
  user_df <- filter(user_df, length(user_df$user_id)!=0)
  #user_df <- read.df("Data/user2017_12_25_13_26.parquet", "parquet", mergeSchema = "true")
  user_data_frame <- collect(user_df)
  #user_id_all <- user_data_frame$user_id
  return(user_data_frame)
}


query_media <- function(user_id,end_cursor=''){
  Sys.sleep(0.1)
  query_media_url <- paste("https://www.instagram.com/graphql/query/?query_id=17888483320059182&id=",user_id,"&first=100&after=",end_cursor,sep = "")
  tryCatch( 
    {query_media_get <- RCurl::getURL(url = query_media_url)
    query_media_json <- rjson::fromJSON(query_media_get)
    #####deal with status not ok
    if(query_media_json$status=="ok"){
      medias <- query_media_json$data$user$edge_owner_to_timeline_media
      return(medias)
    }
    else {(return(query_media(user_id,end_cursor)))}
    },
    error = function(e)
    {
      #print("there is something wrong with this user")
      print(e)
      return(NULL)
      #Sys.sleep(300)
      #return(query_media(user_id,end_cursor))
    }
  )
}
get_media_from_user <- function(user_id,end_cursor='',instagram_url=c()){
  #lineprof::pause(0.1)
  Sys.sleep(0.1)
  medias <- query_media(user_id,end_cursor)
  sumPosts <- medias$count
  end_cursor <- medias$page_info$end_cursor
  #when end_cursor is null, edge is edge[]
  #when user is private, edge is edge[],but end_cursor is not null
  if (!is.null(end_cursor)){
    if(length(medias$edges)==0){print("This is private user")}
    else{
      for(i in 1:length(medias$edges)){
        timestamp <- medias$edges[[i]]$node$taken_at_timestamp
        if (anytime::anytime(timestamp) > as.POSIXct("2017-10-14") && anytime::anytime(timestamp)< as.POSIXct("2017-12-15")){
          media_code <- medias$edges[[i]]$node$shortcode
          url <- paste("https://www.instagram.com/p/",media_code,"/?__a=1",sep = "")
          instagram_url <- append(instagram_url,url)
          #user_media_code <- append(user_media_code,media_code)
        }
        
      }
      return(get_media_from_user(user_id, end_cursor,instagram_url))
    }
  }
  else {print("end_cursor is null")}
  print(paste("collecting ",length(instagram_url)," media_code from all ",sumPosts," medias",sep = ""))
  return(instagram_url)
}

get_detail_media <- function(twitter_pointer){
  Sys.sleep(0.1)
  #media_url <- paste("https://www.instagram.com/p/",short_code,"/?__a=1",sep = "")
  #use tryCatch to handle errors which result from getURL().e.g.Can’t open file(con,”f”) connection or SSLRead() return error -9806 
  tryCatch({
    media_get <-RCurl::getURL(url = twitter_pointer)
    media_json <- rjson::fromJSON(media_get) 
    return (media_json$graphql$shortcode_media)
  }, error=function(e)
  { print(e)
    return(NULL)}
  )
}

write_parquet_media <- function(media){
  Sys.sleep(0.1)
  ########location_id = unlistWithNA(media,c('location','id'))
  ########media <- media[which(unlist(spark.lapply(location_id,function(x) !is.na(x)),use.names = TRUE))]
  ##########edge_media_to_tagged_user and edge_media_to_caption
  media_data = data.frame(
  media_id = unlistWithNA(media,'shortcode'),
  user_id = unlistWithNA(media, c('owner','id')),
  location_id = unlistWithNA(media,c('location','id')),
  text = unlistWithNA(media,c('edge_media_to_caption','edges',1,'node','text')),
  create_time_stamp= unlistWithNA(media,'taken_at_timestamp'))
  
  media_data$url <- paste("https://www.instagram.com/p/",media_data$media_id,"/",sep = '')
  instagram_media_spark <- as.DataFrame(media_data)
  current.time <- format(Sys.time(), "%Y_%m_%d_%H_%M")
  #media_path <- paste("Data/instagramdata/media",current.time,'.parquet',sep = '')
  media_path <- paste("/data/instagramdata/instagram_media",current.time,'.parquet',sep = '')
  write.df(instagram_media_spark, path = media_path, source = "parquet", mode = "append")
}

write_parquet_user_status <- function(user){
  Sys.sleep(0.1)
  instagram_user_spark <- as.DataFrame(user)
  #current.time <- format(Sys.time(), "%Y_%m_%d_%H_%M")
  #user_path <- paste("Data/instagramdata/userfrommedia",current.time,'.parquet',sep = '')
  user_path <- "/data/instagramdata/userfrommedia.parquet"
  write.df(instagram_user_spark, path = user_path, source = "parquet", mode = "append")
}

file_name_user <- "/data/instagramdata/instagram_user.parquet"
user_data_frame <- read_parquet_user(file_name_user)
user_id_all <- user_data_frame$user_id
#user_id <- base::sample(user_id_all,5000)

user_id <- user_id_all[1:100]
media_all <- list()
for (i in 1:length(user_id)){
  sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "50g"))
  media_url <- unlist(lapply(user_id[i],get_media_from_user))
  #media_url_all <- notreplica_vect(media_url,media_url_all)
  ##if the user is private,media_url is null
  if(!is.null(media_url)){
  sample_media_all <- spark.lapply(media_url,get_detail_media)
  sample_media_all <- sample_media_all[unlist(lapply(sample_media_all,function(x) !is.null(x)),use.names = TRUE)]
  
  #sample_media_all must come before location_id
  location_id = unlistWithNA(sample_media_all,c('location','id'))
  media_with_location <- sample_media_all[which(unlist(lapply(location_id,function(x) !is.na(x)),use.names = TRUE))]
  #write_parquet_media(media_with_location)
  media_all <- append(media_all,sample_media_all)
  user_data_frame$sample_status_count[i] <- length(sample_media_all)
  print(length(sample_media_all))
  user_data_frame$status_with_location_count[i]<- length(media_with_location)
  print(length(media_with_location))
  print(length(media_all))
  write_parquet_media(sample_media_all)
  }
  else{
    #there is something wrong with user(eg.unrecognized escape character) or user is private
    user_data_frame$sample_status_count[i] <- NA
    user_data_frame$status_with_location_count[i] <- NA
  }
  write_parquet_user_status(user_data_frame)
}

sparkR.session.stop()


#######think about null of sample_media_all and media_with_location
###These are for all the users
#length(sample_media_all)
#length(media_with_location)



