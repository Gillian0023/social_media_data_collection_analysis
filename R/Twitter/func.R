read_parquet <- function(file_name){
  df <- read.df(file_name, "parquet", mergeSchema = "true")
  ##
  #df <- filter(user_df, length(user_df$user_id)!=0)
  #user_df <- read.df("Data/user2017_12_25_13_26.parquet", "parquet", mergeSchema = "true")
  data_frame <- collect(df)
  # user_id_all <- user_data_frame$user_id
  return(data_frame)
}

notreplica_vect <- function(vect1,vect2){
  if(!is.null(vect2)){
    repetition <- which(unlist(lapply(vect1,function(x) x  %in% vect2)))
    if(length(repetition)!=0){
      vect1 <- vect1[-repetition]}
  }
  return (vect1)
  #for(i in 1:length(vect1))
  #{vect2 <- append(vect2,vect1[i])}
  #return (vect2)
}

notreplica_item<- function(vect1){
  vect2 <- c()
  if(length(vect1)!=0){
  for (i in 1:length(vect1)){
    if (vect1[i] %in% vect2)
    {vect2 <- vect2}
    else{vect2 <- append(vect2,vect1[i])}
  }
  }
  return (vect2)
}

#replace null attribute data with NA(unlist() only return not null items without this function)
unlistWithNA <- function(lst,field){
  if (length(field)==1){
    notnulls <- unlist(lapply(lst, function(x) !is.null(x[[field]])),use.names = TRUE)
    vect <- rep(NA, length(lst))
    vect[notnulls] <- unlist(lapply(lst[notnulls],function(x) x[[field]]),use.names = TRUE)
  }
  if (length(field)==2){
    
    notnulls <- unlist(lapply(lst, function(x) !is.null(x[[field[1]]][[field[2]]])),use.names = TRUE)
    vect <- rep(NA, length(lst))
    vect[notnulls] <- unlist(lapply(lst[notnulls], function(x) x[[field[1]]][[field[2]]]),use.names = TRUE)
    
  }
  if (length(field)==3 & field[1]!="coordinates"){
    notnulls <- unlist(lapply(lst, function(x) !is.null(x[[field[1]]][[field[2]]][[field[3]]])),use.names = TRUE)
    vect <- rep(NA, length(lst))
    vect[notnulls] <- unlist(lapply(lst[notnulls], function(x) x[[field[1]]][[field[2]]][[field[3]]]),use.names = TRUE)
  }
  if (field[1]=="coordinates"){
    notnulls <- unlist(lapply(lst, function(x) !is.null(x[[field[1]]][[field[2]]])),use.names = TRUE)
    vect <- rep(NA, length(lst))
    vect[notnulls] <- unlist(lapply(lst[notnulls], function(x) x[[field[1]]][[field[2]]][[as.numeric(field[3])]]),use.names = TRUE)
  }
  
  if (length(field)==4 && field[2]!="urls"){
    notnulls <- unlist(lapply(lst, function(x) length(x[[field[1]]][[field[2]]][[field[3]]][[field[4]]])>0),use.names = TRUE)
    vect <- rep(NA, length(lst))
    vect[notnulls] <- unlist(lapply(lst[notnulls], function(x) x[[field[1]]][[field[2]]][[field[3]]][[field[4]]]),use.names = TRUE)
  }
  if (length(field)==4 && field[2]=="urls"){
    notnulls <- unlist(lapply(lst, function(x) length(x[[field[1]]][[field[2]]])>0),use.names = TRUE)
    vect <- rep(NA, length(lst))
    vect[notnulls] <- unlist(lapply(lst[notnulls], function(x) x[[field[1]]][[field[2]]][[as.numeric(field[3])]][[field[4]]]),use.names = TRUE)
  }
  if (length(field)==5){
    notnulls <- unlist(lapply(lst, function(x) length(x[[field[1]]][[field[2]]])>0),use.names = TRUE)
    vect <- rep(NA, length(lst))
    vect[notnulls] <- unlist(lapply(lst[notnulls], function(x) x[[field[1]]][[field[2]]][[as.numeric(field[3])]][[field[4]]][[field[5]]]),use.names = TRUE)
  }
  if (length(field)==6 && field[2]=="bounding_box"){
    notnulls <- unlist(lapply(lst, function(x) length(x[[field[1]]][[field[2]]])>0),use.names = TRUE)
    vect <- rep(NA, length(lst))
    vect[notnulls] <- unlist(lapply(lst[notnulls], function(x)
      x[[field[1]]][[field[2]]][[field[3]]][[as.numeric(field[4])]][[as.numeric(field[5])]][[as.numeric(field[6])]]),use.names = TRUE)
  }
  return(vect)
}
