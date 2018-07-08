library("methods")
library("bitops")
library("RCurl")
library("rjson")
library("streamR")
library("ROAuth")

requestURL <- "https://api.twitter.com/oauth/request_token"
accessURL <- "https://api.twitter.com/oauth/access_token"
authURL <- "https://api.twitter.com/oauth/authorize"

consumerKey <- "xxxxxx"
consumerSecret <- "xxxxxx"
access_token <- "xxxxxx"
access_token_secret <- "xxxxxx"


#If you are a Windows user, you need to get “cacert.pem” file
##download.file(url="http://curl.haxx.se/ca/cacert.pem",destfile="cacert.pem")
my_oauth <- OAuthFactory$new(consumerKey=consumerKey, consumerSecret=consumerSecret,
                             requestURL=requestURL,accessURL=accessURL, authURL=authURL)

#my_oauth$handshake(cainfo = system.file("CurlSSL", "cacert.pem", package = "RCurl"))
my_oauth$handshake()
##save my_oauth after typing PIN to make authentification
save(my_oauth, file = "my_oauth.Rdata")

  
