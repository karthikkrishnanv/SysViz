#!/bin/bash 

# If you are applying filter to shrink time window, execute this at top dir after data collection
# Also during data collection disable the vizualizer to save time
# options.r needs to be modified to set time window
# builds sar pdf only!

PATH=/bin:/sbin:/usr/bin:/usr/sbin:$PATH:.

if [ ! -d "log" ]; then
  echo "log dir does not exist! Exiting.."
else
  echo "Building sar vizualiser pdf.."
  R CMD BATCH visualizeR.r
fi
