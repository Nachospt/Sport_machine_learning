########################################## DATA JOINING #####################################################

## 0.Step: Load libraries and database connection

## Libraries
library(data.table)
library(dplyr)
library(RODBC)
library(sqldf)
library(MultBiplotR)
library(ggplot2)
library(scales)
library(mice)
library(lubridate)
library(lattice)

## Database connection
ch <- odbcConnect("hou", uid = "texans_daas", pwd = "Password123")
DB.info = sqlTables(ch)

## Extracting the tables of interest
DB.info = subset(DB.info, (TABLE_SCHEM == "e10") & (TABLE_TYPE == "VIEW"))
Pre.tablenames = DB.info$TABLE_NAME
indx = grepl("Latest", Pre.tablenames)
Tablenames = Pre.tablenames[!indx]

Table.list <- lapply(1:length(Tablenames), function(z) {
  query = paste("SELECT * FROM ", "[e10].[", Tablenames[z], "]", sep="")
  print(query)
  return(sqlQuery(ch, query, stringsAsFactors = FALSE))
})
names(Table.list) = Tablenames

## (Contact names - Id list)
df.contacts = sqlQuery(ch, "SELECT * FROM [e10].[Contacts]", stringsAsFactors = FALSE)[,c(1,3,4)]

## Table Selection (Conditions are defined in F.ColumnConditions and checked in F.TableConditions)
## Dates for date conditions
Start.date = as.POSIXct(as.Date('2016-09-01'))
End.date = as.POSIXct(as.Date('2017-02-01'))

## Function to check if a column is a date
is.POSIXct <- function(x) inherits(x, "POSIXct")

## Assign is used to modify the control variables in the outer scope
F.ColumnConditions <- function(z,y){
  if (grepl("SessionId", z)) {
    assign("Session.condition", TRUE, envir = parent.frame(2))
  }
  if ((grepl("ContactId", z)) && length(unique(y)) > 5) {
    assign("Player.condition", TRUE, envir = parent.frame(2))
  }
  if (grepl("Start", z)  == TRUE && is.POSIXct(y) == TRUE) {
    if (as.numeric(difftime(min(y), Start.date, units = "secs"))<0) {
      assign("Start.date.condition", TRUE, envir = parent.frame(2))
    }
    if (as.numeric(difftime(max(y), End.date, units = "secs"))>0) {
      assign("End.date.condition", TRUE, envir = parent.frame(2))
    }
  }
}

F.TableConditions <- function(x) {
  Start.date.condition <- FALSE
  End.date.condition <- FALSE
  Session.condition <- FALSE
  Player.condition <- FALSE
  mapply(z = names(x), y = x, FUN = F.ColumnConditions)
#  print(names(x))
#  print(Session.condition)
#  print(Player.condition)
#  print(Start.date.condition)
#  print(End.date.condition)
  if (Session.condition == TRUE && Player.condition == TRUE 
      && Start.date.condition == TRUE && End.date.condition == TRUE && (length(names(x)) < 1000)) {
    return(TRUE)
  } else {
    return(FALSE)
  }
}
List.tablesused = sapply(FUN = F.TableConditions, Table.list)
Final.tables = Table.list[List.tablesused]

## Cutting tables to dates of interest
Start.on = as.Date('2010-09-01')
Start.off = as.Date('2018-02-01')
for (z in 1:length(Final.tables)) {
  print(z)
  Final.tables[[z]] = Final.tables[[z]][intersect(which(Final.tables[[z]][,"Start"] > Start.on), which(Final.tables[[z]][,"Start"] < Start.off)),]
  print(intersect(which(Final.tables[[z]][,"Start"] > Start.on), which(Final.tables[[z]][,"Start"] < Start.off)))
}

## (Just for analysis purpouses) Getting list of all table and variables
TotalColNum = 0
for (z in 1:length(Final.tables)) {
    TotalColNum = TotalColNum + length(Final.tables[[z]])
}
NamesTable = matrix(c("Table", "Variable"), c(1,2))
for (z in 1:length(Final.tables)) {
Pre.NamesTable = cbind(c(rep(names(Final.tables[z]), length(Final.tables[[z]]))), c(colnames(Final.tables[[z]])))
NamesTable = rbind(NamesTable, Pre.NamesTable)
}

output <- file("Table-Variable.csv", "w")
write.csv(NamesTable, file = output)
close(output)

## Making a list of unique Session-player combination and all unique variables
Pre.sescon.list = c()
Pre.variables.list = c()
for (z in 1:length(Final.tables)) {
  Pre.sescon.list = c(Pre.sescon.list,
                       paste(as.character(Final.tables[[z]][,"SessionId"]),
                             as.character(Final.tables[[z]][,"ContactId"]),
                             sep = "."))
  Pre.variables.list = c(Pre.variables.list, colnames(Final.tables[[z]]))
}

## Just unique values remain
Sescon.vector = unique(Pre.sescon.list)
Variables.vector = unique(Pre.variables.list)

## Final table creation
Full.table = matrix(rep(NA,length(Variables.vector)*length(Sescon.vector)),
                    nrow = length(Sescon.vector),
                    ncol = length(Variables.vector))
colnames(Full.table) = Variables.vector
Full.table = as.data.frame(Full.table)
Full.table$SessionId = Sescon.vector

## Loops that transfer the values
## (somehow slow but couldnt think of how to use apply methods which are used to return something, not to change global enviroment entities)
for (p in 1:length(Final.tables)) {
  x = Final.tables[[p]]
  print(names(Final.tables[p]))
## This transformation avoids losing the date format in the Full.table
  x$sescon <- paste(as.character(x$SessionId), as.character(x$ContactId), sep = ".")
  for (w in 1:nrow(x)) {
    for (z in 2:(ncol(x)-1)) {
      if (is.POSIXct(x[w, z])) {
        Full.table[which(Full.table[,1] == x[w, "sescon"]), colnames(x)[z]] <- format.Date(x[w, z], format = "%Y/%m/%d %I:%M:%S")
      } else if (is.factor(x[w, z])) {
        Full.table[which(Full.table[,1] == x[w, "sescon"]), colnames(x)[z]] <- as.character(x[w, z])
      } else {
        Full.table[which(Full.table[,1] == x[w, "sescon"]), colnames(x)[z]] <- x[w, z]
      }
    }
  }
}