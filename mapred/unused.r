library(ggplot2)
library(plyr)
#library(plotrix)



library(ggplot2)
library(plyr)
#library(plotrix)




PlotMapReduceJobDataRate <- function(mapred.summary,delta_in_secs) {
  data.sort  <- mapred.summary[order(mapred.summary$"End.Time"),]
  start_time <- data.sort[1,]$"End.Time" 
  end_time   <- data.sort[nrow(data.sort),]$"End.Time"
  count      <- nrow(data.sort)
  group_id   <- array(dim=count)
  for (t in 1:count) {
    group_id[t] <- 0
  }
  for (r in 1:count) {
    time <- data.sort[r,]$"End.Time"
    t0 <- strptime(start_time,"%m/%d/%Y %H:%M:%S")
    t1 <- strptime(time,"%m/%d/%Y %H:%M:%S")
    delta <- as.numeric(difftime(t1,t0,units="secs"))
    group <- as.integer(delta/delta_in_secs)
    group_id[r] <- group + 1 
  }
  n <- max(group_id)
  jobrate.actual <- array(dim=n)
  sizerate.actual <- array(dim=n)
  time.actual <- array(dim=n)
  for (t in 1:n) {
    jobrate.actual[t] <- 0
    sizerate.actual[t] <- 0
  }
  t0 <- strptime(start_time,"%m/%d/%Y %H:%M:%S")
  for (r in 1:count) {
    id <- group_id[r]
    jobrate.actual[id] <- jobrate.actual[id] + 1
    input.size <- as.numeric(data.sort[r,]$"HDFS.bytes.read.Map")
    sizerate.actual[id] <- input.size + sizerate.actual[id]
    time.actual[id] <- as.character(t0 + delta_in_secs*id)
    time.actual[id] <- unlist(strsplit(time.actual[id],split=" "))[2]
    
  }

  scaling <- 1/(1024*1024*1024) #in GBs
  sizerate.actual <- sizerate.actual*scaling

  plot(x=seq(1:n),y=jobrate.actual,xlab="",ylab="",xaxt="n",yaxt=NULL,type="b",col="blue")
  title(main="Jobs/Min Throughput",xlab="",
        ylab="Jobs Throughput (per min)")
  axis(1, at=seq(1:n),labels = time.actual,las=2)

  plot(x=seq(1:n),y=sizerate.actual,xlab="",ylab="",xaxt="n",yaxt=NULL,type="b",col="blue")
  title(main="HDFS Data Processed Throughput (GBs per min)",xlab="",
        ylab="Data Throughput (GBs/min)")
  axis(1, at=seq(1:n),labels = time.actual,las=2)

}



delta_in_secs=60
mapred.summary=read.csv("/tmp/data.csv",h=T)
PlotMapReduceJobDataRate(mapred.summary,delta_in_secs) 

