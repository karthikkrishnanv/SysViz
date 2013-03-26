#!/bin/bash  

check_R_exists() {
  if_exists=1
  command -v R >/dev/null 2>&1 || { echo >&2 "R not installed."; if_exists=0; }
  if [ ${if_exists} == "0" ]; then
    echo "0"
  else
    if_exists=$(R --version | grep "R Foundation for Statistical Computing" | wc -l)
    if [ ${if_exists} == "0" ]; then
      echo "R not installed."
      echo "0"
    else
      echo "1"
    fi
  fi
}

check_redhat() {
  if_redhat=$(cat /proc/version | grep redhat | wc -l)
  echo ${if_redhat}
}

install_R() {
  wget http://cran.r-project.org/bin/linux/redhat/el5/x86_64/libRmath-devel-2.10.0-2.el5.x86_64.rpm
  wget http://cran.r-project.org/bin/linux/redhat/el5/x86_64/libRmath-2.10.0-2.el5.x86_64.rpm
  wget http://cran.r-project.org/bin/linux/redhat/el5/x86_64/R-devel-2.10.0-2.el5.x86_64.rpm
  wget http://cran.r-project.org/bin/linux/redhat/el5/x86_64/R-core-2.10.0-2.el5.x86_64.rpm
  wget http://cran.r-project.org/bin/linux/redhat/el5/x86_64/R-2.10.0-2.el5.x86_64.rpm

  rpm -ivh libRmath-devel-2.10.0-2.el5.x8664.rpm
  rpm -ivh libRmath-2.10.0-2.el5.x8664.rpm
  rpm -ivh R-devel-2.10.0-2.el5.x8664.rpm
  rpm -ivh R-core-2.10.0-2.el5.x8664.rpm
  rpm -ivh R-2.10.0-2.el5.x8664.rpm
}

if_redhat=$(check_redhat)
if_R=$(check_R_exists)

if [ ${if_R} == "0" ]; then
  echo "Install R from http://www.r-project.org/ and retry"
  exit 1
fi

echo "Found R package. Installing necessary packages.."

if [ ${USER} == "root" ]; then  
  R CMD BATCH prereq.r
else
  echo "Not a root! Check author for instructions"
  exit 1
fi
