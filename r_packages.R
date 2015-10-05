install.packages('devtools', repos='http://cran.us.r-project.org')
library(devtools)
install_github('apache/spark@v1.4.0', subdir='R/pkg')
