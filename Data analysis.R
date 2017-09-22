####################################### BIPLOT DATA ANALYSIS ####################################
## Step 1: data extraction
## Step 2: analysis (code control function when possible)
## Step 3 reporting if it was possible to do the analysis
## Step 4: save the results

## Step 1: data extraction
setwd('C:\\Users\\Ignacio S-P\\Desktop\\R Data processing\\Houston Texans\\HoustonTables\\')
my_files <- list.files(pattern = "\\.csv$")

Table.reference <- read.table("Table.list.txt", header = FALSE, sep ="\t", stringsAsFactors = FALSE)
colnames(Table.reference) = c("Table", "Variable")

## Read all at once
my_data <- lapply(my_files, read.csv, stringsAsFactors = FALSE)

## Step 2: analysis (code control function when possible)
## Removing columns with too many NA. Criteria 70% NA
F.Imputation <- function(x) {
    NAvariables <- c()
    for (z in 2:length(x)) {
      if ((sum(is.na(x[,z]))/length(x[,z])) > 0.7) {
        NAvariables <- c(NAvariables, z)
      }
    }
    if (length(NAvariables)>0) {
      Pre2_NAfiltered = x[,-NAvariables]
      print(paste("Final table:", NROW(Pre2_NAfiltered), "rows X ", length(Pre2_NAfiltered), "columns"))
      print(paste("% of NAs", 100*sum(is.na(Pre2_NAfiltered))/(length(Pre2_NAfiltered)*NROW(Pre2_NAfiltered))))
    } else {
      print("No cols with > 70% NA")
      Pre2_NAfiltered = x
    }
  
## Removing row with too many NA. Criteria 20% NA
    NArows = c()
    print(paste("rows:", nrow(Pre2_NAfiltered)))
    for (z in 1:nrow(Pre2_NAfiltered)) {
      if ((sum(is.na(Pre2_NAfiltered[z,]))/length(Pre2_NAfiltered[z,])) > 0.2) {
        NArows = c(NArows, z)
      }
    }
    if (length(NArows) > 0) {
      Pre_NAfiltered = Pre2_NAfiltered[-NArows, ]
      print("Rows with > 20% NA")
      print(c(NArows))
      print(paste("Final table:", NROW(Pre_NAfiltered), "rows X ", length(Pre_NAfiltered), "columns"))
      print(paste("% of NAs", 100*sum(is.na(Pre_NAfiltered))/(length(Pre_NAfiltered)*NROW(Pre_NAfiltered))))
      print(sapply(Pre_NAfiltered, function(x) { sum(x == 0 | is.na(x))}))
    } else {
      print("No rows with > 20% NA")
      Pre_NAfiltered = Pre2_NAfiltered
    }

  ## REMOVING COLUMNS AGAIN
    NAvariables <- c()
    for (z in 2:length(Pre_NAfiltered)) {
      if ((sum(is.na(Pre_NAfiltered[,z]))/length(Pre_NAfiltered[,z])) > 0.7) {
        NAvariables <- c(NAvariables, z)
      }
    }
    if (length(NAvariables)>0) {
      NAfiltered = Pre_NAfiltered[,-NAvariables]
      print(paste("Final table:", NROW(NAfiltered), "rows X ", length(NAfiltered), "columns"))
      print(paste("% of NAs", 100*sum(is.na(NAfiltered))/(length(NAfiltered)*NROW(NAfiltered))))
    } else {
      print("No cols with > 70% NA")
      NAfiltered = Pre_NAfiltered
    }
  
  ## Check NA values functions
  # library(VIM)
  # md.pattern(data)
  # aggr_plot <- aggr(data, col=c('navyblue','red'), numbers=TRUE, sortVars=TRUE, labels=names(data), cex.axis=.7, gap=3, ylab=c("Histogram of missing data","Pattern"))
  # marginplot(data[c(1,2)])
  
  ## Imputation of the rest of NA values
  # methods(mice)
  Pre_NAImputed = c()
  NAImputed = c()
    if (sum(sapply(NAfiltered, function(x) { sum(is.na(x)) })) > 0 ) {
      Pre_NAImputed = mice(NAfiltered[,-c(1,2,3)], m = 5, maxit = 5, meth = 'cart', seed = 500, ridge = 0.001)
      stripplot(Pre_NAImputed, pch = 20, cex = 1.2)
      temp1 = mice::complete(Pre_NAImputed)
      print(paste("NAs: ", sum(sapply(temp2, function(x) { sum(is.na(x)) }))))
      
      ## Alternative method of imputation (running if any NA remains)
      if (sum(sapply(complete(Pre_NAImputed), function(x) { sum(is.na(x)) })) > 0 ) {
        NAImputed = mice(temp1, m = 5, maxit = 50, meth = 'pmm', seed = 500)
        stripplot(NAImputed, pch = 20, cex = 1.2)
        temp2 = mice::complete(NAImputed)
        NAImputed = c()
        NAImputed = cbind(NAfiltered$ContactId, temp2)
      } else {
        NAImputed = cbind(NAfiltered$ContactId, temp1)
      }
    
      ## Second alternative method of imputation (running if any NA remains)
      if (sum(sapply(NAImputed, function(x) { sum(is.na(x)) })) > 0 ) {
        NAImputed = mice(NAImputed, m = 5, maxit = 50, meth = 'mean', seed = 500)
        stripplot(NAImputed, pch = 20, cex = 1.2)
        NAImputed = cbind(NAfiltered$ContactId, mice::complete(NAImputed))
      }
    } else {
      NAImputed = NAfiltered[,-c(1,3)]
    }
  print(NAImputed)
# Check there are no more NAs/ Imputation
# prueba = mice.impute.sample(NAImputed, ry = FALSE, x = NULL)
# sum(sapply(prueba, function(x) { sum(is.na(x)) }))
colnames(NAImputed)[1] = "ContactId"

# Adding row labels
 Pre_Analysis.tables = c()
 Analysis.tables = c()
 Pre_Analysis.tables = NAImputed
 Analysis.tables = merge(Pre_Analysis.tables, df.contacts, by = "ContactId")
 Analysis.tables$PlayerName = paste(Analysis.tables$FirstName, Analysis.tables$LastName)
 Analysis.tables = cbind(Analysis.tables[,"PlayerName"], Analysis.tables[,c(2:(length(Analysis.tables)-3))], stringsAsFactors = FALSE)
 colnames(Analysis.tables)[1] = "PlayerName"
 return(Analysis.tables)
}

## Biplot looping function (alltime files)
F.CycleBiplot <- function(x) {
  colnames(x)[c(1,2)] = c("X", "ContactId")
  g.list = list()
  for (z in 1:(length(unique(Table.reference$Table)))) {
    Table.reference.cols = make.names(Table.reference[which(Table.reference$Table == unique(Table.reference$Table)[z]),"Variable"])
    Analysis.Columns = which(colnames(x) %in% Table.reference.cols)
    if (!is.null(Analysis.Columns) && length(Analysis.Columns) > 3) {
      Biplot.table = F.Imputation(x[, c(Analysis.Columns)])
## First team
      Including.rows = Firstteam
      Including.col.list = sapply(Biplot.table$PlayerName, function(x)
        if (x %in% Including.rows) {
          return(TRUE)
        } else {
          return(FALSE)
        })
      Biplot.table = Biplot.table[Including.col.list, ]
      g = PCA.Biplot(Biplot.table[,-1], alpha = 2, dimension = 3, Scaling = 5, sup.rows = NULL, sup.cols = NULL)
## Saving image
      Biplot.Name = paste("Biplot","_", z, "_", unique(Table.reference$Table)[z], NROW(Biplot.table),  ".png", sep = "")
      png(filename = Biplot.Name)
      plot(g, IndLabels = Biplot.table[,1])
      dev.off()
      g.list[[z]] <- g
    }
  }
  return(g.list)
}

Results <- lapply(my_data, FUN = F.CycleBiplot)

## Saving text results
lapply(Results, function(x)
  for (z in 1:length(Results)) {
  BiplotName = paste("Table", "Biplot", unique(Lista)[z], system.time, ".png")
  output <- file(BiplotName, "w")
  write(Results[[z]], file = output)
  close(output)
  })