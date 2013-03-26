library(ggplot2)
library(plyr)
#library(plotrix)

source("options.r")
source("plot.r")

# global variables/lists 
# Tasks are map, shuffle, sort, reduce
# maptask.count[[1]] = concurrent running tasks on host1 etc normalized to time window
maptask.count        <- list()
shuffletask.count    <- list()
sorttask.count        <- list()
reducetask.count     <- list()
io.categories        <- c("HDFS.Bytes.read","HDFS.Bytes.written",
                           "File.Bytes.read","File.Bytes.written",
                           "Bytes.Shuffled")
timer.cols           <- c("Start.Time","Sort.Finish", 
                          "Shuffle.Finished","End.Time")
timercols.epoch      <- c("StartTime.epoch","SortFinish.epoch", 
                          "ShuffleFinished.epoch","EndTime.epoch")
jobstart.marker      <- "Start.Time"
jobend.marker        <- "End.Time"
jobstartmarker.epoch <- "StartTime.epoch"
jobendmarker.epoch   <- "EndTime.epoch"

FillMapReduceJobDetails <- function() {
  if (file.exists(jobdetail.csv)) {
    mapred.details <- read.csv(jobdetail.csv,h = TRUE,strip.white = TRUE)
    hostnames <- sort(unique(mapred.details$hostname))
    nnodes <<- length(hostnames)
    node.name <<- hostnames
    } else {
        mapred.details <- NULL
    }
    return (mapred.details)
}

FillMapReduceJobSummary <- function() {
  if (file.exists(jobsummary.csv)) {
    mapred.summary <- read.csv(jobsummary.csv,h = TRUE,strip.white = TRUE)
    } else {
        mapred.summary <- NULL
    }
    return (mapred.summary)
}

mapred.details <- FillMapReduceJobDetails()
mapred.summary <- FillMapReduceJobSummary()

GetRandomColorList <- function(n) {
  alpha <- 255
  col <- list()
  for (i in 1:n) {
    v <- sample(colors(), 1)
    v <- col2rgb(v)
    col[[i]] <- rgb(as.numeric(v[1]),as.numeric(v[2]),as.numeric(v[3]),alpha,m=255)
  }
  return (col)
}

GetRandomColorArray <- function(n) {
  col <- rgb(runif(n),runif(n),runif(n),1/4)
  return (col)
}


# Start epoch time of all completed jobs
JobStartEpochTime <- function(data,timer.cols) {
  start.time <- NA
  for (col in timer.cols) {
    start.time <- min(data[,col], start.time,na.rm=TRUE)
  }
  return (start.time)
}

# End epoch time of all completed jobs
JobEndEpochTime <- function(data,timer.cols) {
  start.time <- NA
  for (col in timer.cols) {
    start.time <- max(data[,col], start.time,na.rm=TRUE)
  }
  return (start.time)
}


# return array of total concurrent running tasks 
# (running window = taskend.col->taskstart.col)
# offset time by runtime start to start index from 0
# data.full = data from entire cluster, data.filtered = filtered per node per task etc
FindTaskCountVsTimeMap <- function(data.full,data.filtered,taskstart.col,taskend.col) {
  epoch.start <- JobStartEpochTime(data.full,jobstartmarker.epoch)
  epoch.end   <-  JobEndEpochTime(data.full,jobendmarker.epoch)
  count       <-  epoch.end-epoch.start+1
  countvstimearray <- array(dim=count)
  for (t in 1:count)
    countvstimearray[t] <- 0
  for (row in 1:nrow(data.filtered)) {
    task.start <- as.numeric(data.filtered[row,][taskstart.col])
    task.end   <- as.numeric(data.filtered[row,][taskend.col])
    offset <- epoch.start
    task.start <- task.start- epoch.start+ 1
    task.end <- task.end - epoch.start + 1
    countvstimearray[task.start:task.end] <-
                    as.numeric(countvstimearray[task.start:task.end])+1
  }
  return (countvstimearray)
} 


# mapred.details contains the entire data parsed from log files
FillConcurrentTasks <- function(mapred.details) {
  for (i in 1:nnodes) {

    host <- node.name[i]
    host_upper <- toupper(node.name[i])
    data <- subset(mapred.details, hostname == host | hostname == host_upper)

    datafilter.map <- subset(data,Task.Type=="MAP")
    datafilter.reduce <- subset(data,Task.Type=="REDUCE")

    maptask.count[[i]]     <<- FindTaskCountVsTimeMap(mapred.details,
                                                      datafilter.map,
                                                      "StartTime.epoch",
                                                      "EndTime.epoch")
    shuffletask.count[[i]] <<- FindTaskCountVsTimeMap(mapred.details,
                                                      datafilter.reduce,
                                                      "StartTime.epoch",
                                                      "ShuffleFinished.epoch")
    sorttask.count[[i]]    <<- FindTaskCountVsTimeMap(mapred.details,datafilter.reduce,
                                                       "ShuffleFinished.epoch",
                                                       "SortFinish.epoch")
    reducetask.count[[i]]  <<- FindTaskCountVsTimeMap(mapred.details,datafilter.reduce,
                                                     "SortFinish.epoch","EndTime.epoch")

  }

}


PlotTaskConcurrency <- function(mapred.details) {
# parallel.task build concurrent.{maps,reduces..},hostname table
  epoch.start <- JobStartEpochTime(mapred.details,jobstartmarker.epoch)
  epoch.end  <-  JobEndEpochTime(mapred.details,jobendmarker.epoch)
  count <- epoch.end-epoch.start+1
  epoch.times <- as.vector(epoch.start:epoch.end)
  timeline <- format(as.POSIXlt(epoch.times,origin="1970-01-01"),'%H:%M:%S')
  timeline <- rep(timeline,nnodes)
  timeline.epoch <- c(epoch.start:epoch.end)
  timeline.epoch <- rep(timeline.epoch,nnodes)
  
  delta <- epoch.end-epoch.start+1
  timeline.index <- timeline.epoch

  FillConcurrentTasks(mapred.details)
  parallel.task <<- data.frame(time=as.vector(timeline.epoch),
                               concurrent.maps=as.vector(maptask.count[[1]]),
                               concurrent.shuffles=as.vector(shuffletask.count[[1]]),
                               concurrent.sorts=as.vector(sorttask.count[[1]]),
                               concurrent.reduces=as.vector(reducetask.count[[1]]),
                               hostname = node.name[[1]])

  for (i in 2:nnodes) {
    parallel.task <<- rbind(parallel.task, 
                            data.frame(time=as.vector(timeline.epoch),
                                       concurrent.maps = as.vector(maptask.count[[i]]),
                                       concurrent.shuffles = 
                                              as.vector(shuffletask.count[[i]]),
                                       concurrent.sorts = as.vector(sorttask.count[[i]]),
                                       concurrent.reduces = 
                                              as.vector(reducetask.count[[i]]),
                                       hostname = node.name[[i]]))
  }

  task.names <- c("concurrent.maps","concurrent.shuffles",
                   "concurrent.sorts","concurrent.reduces")
  task.tag   <-  c("ConcurrentMaps","ConcurrentShuffles",
                  "ConcurrentSorts","ConcurrentReduces")
  

  nchunk <- 10
  total <- length(timeline.index)/nnodes
   if (total > nchunk) {
    markers <-seq(1,total,as.integer(total/nchunk))
    breaks <- sort(timeline.index[markers])
    labels <- format(as.POSIXlt(breaks,origin="1970-01-01"),'%H:%M:%S')
  } else {
    breaks <- timeline.index
    labels <- format(as.POSIXlt(breaks,origin="1970-01-01"),'%H:%M:%S')
  }

  parallel.plots = list()
  for (i in 1:length(task.names)) {
    p <- ggplot(data=parallel.task,
                 aes_string(x="time",y=task.names[i],
                 color="hostname",group="hostname"))
    title <- paste("# of ",task.tag[i],sep="")
    p <- p +  geom_point() + ####geom_line() +
              xlab("Timeline") + ylab (task.tag[i]) + ggtitle(title) +
              scale_x_continuous(breaks=breaks,labels=labels) + 
            theme(axis.text.x  = element_text(angle=90, vjust=0.5, size=16,face="bold"))
    p <- p + theme(axis.title.x   = element_text(size=24,face="bold"))
    p <- p + theme(axis.title.y  = element_text(size=24,face="bold"))
    p <- p + theme(plot.title  = element_text(size=24,face="bold"))
    p <- p + theme(axis.text.x  = element_text(size=15,face="bold"))
    p <- p + theme(axis.text.y  = element_text(size=15,face="bold"))
    p <- p + theme(legend.text  = element_text(size=20))
    parallel.plots[[i]] = p
  }

  multiplot(parallel.plots[[1]],parallel.plots[[2]],cols=1)
  multiplot(parallel.plots[[3]],parallel.plots[[4]],cols=1)

}

#PlotTaskConcurrency(data.full)

GetIOTimeinEpoch <- function(data.line,io.type) {
  val <- data.line[io.type]
  if (is.na(val))
    return (NA)
  if (as.numeric(val) <= 0) {
    return (NA)
  }

   starttime.epoch <- data.line$"StartTime.epoch"
   endtime.epoch <- data.line$"EndTime.epoch"
   sortfinish.epoch <- data.line$"SortFinish.epoch"
   shufflefinish.epoch <- data.line$"ShuffleFinished.epoch"
  if (data.line$"Task.Type" == "MAP" || data.line$"Task.Type" == "CLEANUP") {
    if (io.type == "HDFS.Bytes.read") {
      return (starttime.epoch)
    }
    else if (io.type == "HDFS.Bytes.written") {
      return (endtime.epoch)
    }
    else if (io.type == "File.Bytes.read") {
      return (endtime.epoch)
    }
    else if (io.type == "File.Bytes.written") {
#      This is an approximation. Don't know time when the local files are written!
      if (is.na(starttime.epoch) || is.na(endtime.epoch))
        return (NA)
      middle.epoch = (starttime.epoch/2) + (endtime.epoch/2)
      if (!is.na(middle.epoch))
        return (middle.epoch)
    }
    else if (io.type == "Bytes.Shuffled") {
      return (endtime.epoch)
    }
  } 
  else if (data.line$"Task.Type" == "REDUCE") {
    if (io.type == "HDFS.Bytes.read") {
      return (shufflefinish.epoch)
    }
    else if (io.type == "HDFS.Bytes.written") {
      return (endtime.epoch)
    }
    else if (io.type == "File.Bytes.read") {
      return (shufflefinish.epoch)
    }
    else if (io.type == "File.Bytes.written") {
#      This is an approximation. Don't know time when the local files are written!
      if (is.na(starttime.epoch) || is.na(endtime.epoch))
        return (NA)
      middle.epoch = (starttime.epoch/2) + (endtime.epoch/2)
      if (!is.na(middle.epoch))
        return (middle.epoch)
    }
    else if (io.type == "Bytes.Shuffled") {
      return (shufflefinish.epoch)
    }
  }
  return (NA)
}


GetIODataArray <- function(data.filtered,data.full,io.type) {
  epoch.start <- JobStartEpochTime(data.full,jobstartmarker.epoch)
  epoch.end  <-  JobEndEpochTime(data.full,jobendmarker.epoch)
  count <- epoch.end-epoch.start+1
# count <- max(count,nrow(data.filtered))  # don't recall why I did this!
  timevsiodata <- array(dim=count)
  for (t in 1:count)
    timevsiodata[t] <- 0
  for (row in 1:nrow(data.filtered)) {
    io.time <- GetIOTimeinEpoch(data.filtered[row,],io.type)
    if (!is.na(io.time) && io.time != "0"){
      io.time <- io.time-epoch.start
      timevsiodata[io.time] <- as.numeric(timevsiodata[io.time]) + 
                               as.numeric(data.filtered[row,][io.type])
    }
  }
  
  return(timevsiodata)
}

GetIODataArrayforHost <- function(data.full,io.type,host) {
  host_upper = toupper(host)
  data.filtered <- subset(data.full,hostname == host | hostname == host_upper)
  return (GetIODataArray(data.filtered,data.full,io.type))
}


PlotMapReduceIOTimeline <- function(mapred.details) {
  epoch.start <- JobStartEpochTime(mapred.details,jobstartmarker.epoch)
  epoch.end  <-  JobEndEpochTime(mapred.details,jobendmarker.epoch)
  count <- epoch.end-epoch.start+1
  epoch.times <- as.vector(epoch.start:epoch.end)
  timeline <- format(as.POSIXlt(epoch.times,origin="1970-01-01"),'%H:%M:%S')
  timeline <- rep(timeline,nnodes)
  timeline.epoch <- c(epoch.start:epoch.end)
  delta <- epoch.end-epoch.start+1
  data.full <- mapred.details

  io.data <<- data.frame(time=as.vector(timeline.epoch),
             HDFS.Bytes.read  = GetIODataArrayforHost(data.full,"HDFS.Bytes.read",
                                                      node.name[1]),
             HDFS.Bytes.written = GetIODataArrayforHost(data.full,"HDFS.Bytes.written",
                                                      node.name[1]),
             File.Bytes.read = GetIODataArrayforHost(data.full,"File.Bytes.read",
                                                      node.name[1]),
             File.Bytes.written = GetIODataArrayforHost(data.full,"File.Bytes.written",
                                                      node.name[1]),
             Bytes.Shuffled =  GetIODataArrayforHost(data.full,"Bytes.Shuffled",
                                                      node.name[1]),
                               hostname = node.name[[1]])

  for (i in 2:nnodes) {
    io.data <<- rbind(io.data,data.frame(time=as.vector(timeline.epoch),
             HDFS.Bytes.read  = GetIODataArrayforHost(data.full,"HDFS.Bytes.read",
                                                      node.name[i]),
             HDFS.Bytes.written = GetIODataArrayforHost(data.full,"HDFS.Bytes.written",
                                                      node.name[i]),
             File.Bytes.read = GetIODataArrayforHost(data.full,"File.Bytes.read",
                                                      node.name[i]),
             File.Bytes.written = GetIODataArrayforHost(data.full,"File.Bytes.written",
                                                      node.name[i]),
             Bytes.Shuffled =  GetIODataArrayforHost(data.full,"Bytes.Shuffled",
                                                      node.name[i]),
                               hostname = node.name[[i]]))
  }

  scale = 1/(1024*1024)
  task.names <- io.categories
  task.tag   <- io.categories

  for (type in io.categories)
    io.data[type] = io.data[type]*scale
  
  timeline.index <- timeline.epoch

  nchunk <- 10
  total = length(timeline.index)
   if (total > nchunk) {
    markers <-seq(1,total,as.integer(total/nchunk))
    breaks <- sort(timeline.index[markers])
    labels <- format(as.POSIXlt(breaks,origin="1970-01-01"),'%H:%M:%S')
  } else {
    breaks <- timeline.index
    labels <- format(as.POSIXlt(breaks,origin="1970-01-01"),'%H:%M:%S')
  }

  parallel.plots = list()
  for (i in 1:length(task.names)) {
    p <- ggplot(data=io.data,
                 aes_string(x="time",y=task.names[i],
                 color="hostname",group="hostname"))
    title = paste(task.tag[i], " (MBs)",sep="")
    p <- p +  geom_point() + 
              xlab("Timeline") + ylab (task.tag[i]) + ggtitle(title) +
              scale_x_continuous(breaks=breaks,labels=labels) + 
            theme(axis.text.x  = element_text(angle=90, vjust=0.5, size=16,face="bold"))
    p <- p + theme(axis.title.x   = element_text(size=24,face="bold"))
    p <- p + theme(axis.title.y  = element_text(size=24,face="bold"))
    p <- p + theme(plot.title  = element_text(size=24,face="bold"))
    p <- p + theme(axis.text.x  = element_text(size=15,face="bold"))
    p <- p + theme(axis.text.y  = element_text(size=15,face="bold"))
    p <- p + theme(legend.text  = element_text(size=20))
    parallel.plots[[i]] = p
  }

  multiplot(parallel.plots[[1]],parallel.plots[[2]],cols=1)
  multiplot(parallel.plots[[3]],parallel.plots[[4]],cols=1)
}



PlotMapReduceIOClusterBarplot <- function(mapred.details) {

  mapred.details["node"] = mapred.details["hostname"]
  io.datasizes = list()  
  io.datasizes[[1]] = subset(mapred.details,HDFS.Bytes.read > 0)
  io.datasizes[[2]] = subset(mapred.details,HDFS.Bytes.written > 0)
  io.datasizes[[3]] = subset(mapred.details,File.Bytes.read > 0)
  io.datasizes[[4]] = subset(mapred.details,File.Bytes.written > 0)
  io.datasizes[[5]] = subset(mapred.details,Bytes.Shuffled > 0)

  parallel.plots = list()
  for (i in 1:5) {
    tag = io.categories[[i]]
    scale = 1/(1024*1024)
    xlabel = paste(io.categories[[i]], " in MB")
    ylabel = paste(io.categories[[i]], " in MB")
    title = paste("Distribution of ",io.categories[[i]]," Vs Tasks",sep="")
    node.matrix = GetClusterBarplotData(io.datasizes[[i]],io.categories[[i]],
                                          "node")
    node.matrix["bin.val"] = round(node.matrix["bin.val"]*scale,2)
    p = ggplot(data=node.matrix,aes(x=factor(bin.val),y=value,fill=node))
    p <- p + geom_bar(stat="identity",position=position_dodge())
    p <- p + ggtitle (paste("Clustered Barplot of ",tag," in MBs",sep=""))
    p <- p + xlab(ylabel) + ylab("% of Tasks")
    p <- p + theme(axis.title.x  = element_text(size=24,face="bold"))
    p <- p + theme(axis.title.y  = element_text(size=24,face="bold"))
    p <- p + theme(plot.title  = element_text(size=24,face="bold"))
    p <- p + theme(axis.text.x  = element_text(size=15,face="bold"))
    p <- p + theme(axis.text.y  = element_text(size=15,face="bold"))
    p <- p + theme(legend.text  = element_text(size=20))
    
    parallel.plots[[i]] = p
  }

  multiplot(parallel.plots[[1]],parallel.plots[[2]],cols=1)
  multiplot(parallel.plots[[3]],parallel.plots[[4]],cols=1)
  multiplot(parallel.plots[[4]],cols=1,rows=1)
}

PlotMapReduceJobRate <- function(mapred.summary,delta_in_secs) {
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
    group_id[r] <- group
  }
  n <- max(group_id)
  jobrate.actual <- array(dim=n)
  sizerate.actual <- array(dim=n)
  for (t in 1:n) {
    jobrate.actual[t] <- 0
    sizerate.actual[t] <- 0
  }
  for (r in 1:count) {
    id <- group_id[r]
    jobrate.actual[id] <- jobrate.actual[id] + 1
    input.size <- as.numeric(data.sort[r,]$"HDFS.bytes.read.Map")
    sizerate.actual[id] <- input.size + sizerate.actual[id]
  }

}

PlotMapReduceTaskTimes <- function(mapred.summary) {
# bad bad hack. Fix python when reduce jobs don't exist!
  clock.threshold <- 3600*4
  mapred.summary$REDUCE_TIME_CLOCK.s[which(mapred.summary$REDUCE_TIME_CLOCK.s>clock.threshold)] = 0

  job.tmp <- mapred.summary$"Job.ID"
  job.tmp <- unlist(strsplit(as.character(job.tmp),split="_"))[seq(3,length(job.tmp),3)]
  job.tmp <- as.character(job.tmp)
  mapred.summary["JobID.Trimmed"] <- job.tmp
  map    = data.frame(as.vector(mapred.summary$"JobID.Trimmed"),
                      as.vector(mapred.summary$MAP_TIME_CLOCK.s),"MAP")
  reduce = data.frame(as.vector(mapred.summary$"JobID.Trimmed"),
                      as.vector(mapred.summary$REDUCE_TIME_CLOCK.s),"REDUCE")
  names(map)    = c("JobID.Trimmed","Time","Task")
  names(reduce) = c("JobID.Trimmed","Time","Task")
  data          = rbind(map,reduce)


  p <- ggplot(data,aes(x=factor(JobID.Trimmed),y=Time,fill=factor(Task))) 

  p <- p + geom_bar(stat="identity")
  p <- p + ggtitle ("Responsiveness of Jobs (seconds)")
  p <- p + xlab("JobID") + ylab("Time (Seconds)")
  p <- p + theme(axis.title.x  = element_text(size=24,face="bold"))
  p <- p + theme(axis.title.y  = element_text(size=24,face="bold"))
  p <- p + theme(plot.title  = element_text(size=24,face="bold"))
# p <- p + theme(axis.text.x  = element_text(size=15,face="bold"))
  p <- p + theme(axis.text.x  = element_text(angle=90, vjust=0.5, size=10,face="bold"))
  p <- p + theme(axis.text.y  = element_text(size=15,face="bold"))
  tick.pos <- seq(1,length(job.tmp),10)
  p <- p +  scale_x_discrete(breaks=job.tmp[tick.pos])
  print(p)
  #multiplot(p,cols=1,rows=1)

}

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

if (!is.null(mapred.details)) {    
  pdf(mapred.pdf,height = 15,width = 15)
  par(mfrow=c(2,1))
  PlotTaskConcurrency(mapred.details)
  PlotMapReduceIOTimeline(mapred.details)
  PlotMapReduceIOClusterBarplot(mapred.details)
  par(mfrow=c(1,1))
  PlotMapReduceTaskTimes(mapred.summary)
  par(mfrow=c(2,1))
  delta_in_secs=60
  PlotMapReduceJobDataRate(mapred.summary,delta_in_secs) 
  dev.off()
}



