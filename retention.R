# FunNow
funnow programs
library("RODBC")
library(ggplot2)
library(tidyverse)
setwd('C:\\Users\\user\\Desktop\\myR')
dbData <- read.csv('OrderAmount.csv')

cohort <- dbData %>%
  spread(OrderM, totalAmount)

#cosmetics, ercentages

shiftrow <- function(v){
  #put a vector in, strip off leading NA values, and place that amount at the end
  first_na_index <- min(which(!is.na(v)))
  
  #return that bit to the end, and pad with NAs.
  c(v[first_na_index:length(v)], rep(NA, first_na_index - 1))
}
cohort
shifted <- data.frame(
  cohort = cohort$SignUpMonth,
  t(apply( select(as.data.frame(cohort), 2:ncol(cohort)), # 2nd column to the end
           1, shiftrow ))
)
shifted
colnames(shifted) <- c("cohort", sub("","month.", str_pad(1:(ncol(shifted)-1),2,pad = "0")))
shifted
signUpData <- read.csv('signUpAmount.csv')
signUpData
shifted$month.00 <- signUpData$TotalUserAmount
shifted
shifted_pct <- data.frame(
  cohort = shifted$cohort, # first column
  shifted[,2:ncol(shifted)] / shifted[["month.00"]]#divide by week.01
)
shifted_pct
plotdata_abs <- gather(shifted, "cohort_age", "people", 2:ncol(shifted))
plotdata_pct <- gather(shifted_pct, "cohort_age", "percent" ,2:ncol(shifted_pct))
plotdata_abs
plotdata_pct
#add data with pretty labels
#labelnames <- c( plotdata_abs$people[1:(ncol(shifted)-1)],
#                plotdata_pct$percent[(ncol(shifted)):(nrow(plotdata_pct))])
labelnames <- c(plotdata_pct$percent[1: (nrow(plotdata_pct) - nrow(shifted))], shifted$month.00)
labelnames
pretty_print <- function(n){
  case_when(n <= 1 ~ sprintf("%1.0f%%", n*100),
            n > 1 ~ as.character(n),
            TRUE ~" "
            )#For NA values, skip the label
}

plotdata <- data.frame(
  cohort = plotdata_pct$cohort,
  cohort_age = plotdata_pct$cohort_age,
  percentage = plotdata_pct$percent,
  label = pretty_print(labelnames)
)
plotdata

#plot

ggplot(plotdata, aes(x = cohort_age, y = reorder(cohort, desc(cohort)))) +
  geom_raster(aes(fill = percentage)) +
  scale_fill_continuous(guide = FALSE) + # no legend
  geom_text(aes(label = label), color = "white") +
  xlab("cohort age") + ylab("cohort") + 
  ggtitle(paste("FunNow Retention table monthly (Order)"))

