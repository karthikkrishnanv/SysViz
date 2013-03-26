InstallPackage <- function(x) {
  old.repos <- getOption("repos")
  on.exit(options(repos = old.repos)) 
  new.repos <- old.repos
  new.repos["CRAN"] <- "http://cran.stat.ucla.edu" #set your favorite CRAN
  options(repos = new.repos)
  x <- as.character(substitute(x))
  if(isTRUE(x %in% .packages(all.available=TRUE))) {
    eval(parse(text=paste("require(", x, ")", sep="")))
  } else {
    update.packages() 
    eval(parse(text=paste("install.packages('", x, "')", sep="")))
    eval(parse(text=paste("require(", x, ")", sep="")))
  }
} 

InstallPackage(ggplot2)
InstallPackage(plyr)
InstallPackage(plotrix)
