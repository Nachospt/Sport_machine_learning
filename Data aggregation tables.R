####################################### DATA AGGREGATION ###########################################
## Table requirements: first column must be the player identifier. Date must be "%Y/%m/%d %I:%M:%S" string

## 1.Step: Dim function
## 2.Step: Transform date and remove nominal variables
## 3.Step: Aggregate the data
## 4.Step: Save table and free RAM
## 5.Step: Return to 2 or end if all aggregations tables were created

## 1.Step: Dim function
F.Dataggregate <- function(M, Date, ..., year.week = "y", year.month = "y", week = "y", month = "y", year = "y", season = "y", alltime = "y", seasonstart = 8) {
 
## 2.Step: Transform date
 M$Date = as.POSIXct(strptime(M[, Date], "%Y/%m/%d %I:%M:%S"))
 Start.date = min(M[,"Date"])
 End.date = max(M[,"Date"])
 
## 2.Step: Remove the non-numeric variables (but date)
 Pre.Num.Columns = list()
 for (z in 1:length(M)) {
   Pre.Num.Columns[z] = is.numeric(M[,z])
 }
 Num.Columns <- unlist(Pre.Num.Columns)
 M = cbind(M[1], M[, Num.Columns], M[, "Date"])
 
 ## Small details to fix
 ## Date Lost his name in the process, we add it
 names(M)[length(M)]="Date"
 ## SessionId is in fact a concatenation of SessionId and ContactId. We need to separate it for the aggregations
 M$SessionId = substr(M$SessionId, 38, 73)
 
 table = "beginning"
 repeat {
## Table aggregation control conditions
## Every step sets target table. If "n" is given as argument for the 4 kinds of aggregations, it will be
## forwarded to the next data table type.
  if (table == "beginning" && year.week == "y") {
      table = "year.week"
      process = "y"
  } else if (table == "beginning" && year.week == "n") {
    table = "year.week"
    process = "n"
  }

  if (table == "year.week" && year.month == "y" && process == "n") {
    table = "year.month"
    process = "y"
  } else if (table == "year.week" && year.month == "n" && process == "n") {
    table = "year.month"
    process = "n"
  }

  if (table == "year.month" && week == "y" && process == "n") {
    table = "week"
    process = "y"
  } else if (table == "year.month" && week == "n" && process == "n") {
    table = "week"
    process = "n"
  }

   if (table == "week" && month == "y" && process == "n") {
     table = "month"
     process = "y"
   } else if (table == "week" && month == "n" && process == "n") {
     table = "month"
     process = "n"
   }
   
   if (table == "month" && year == "y" && process == "n") {
     table = "year"
     process = "y"
   } else if (table == "month" && year == "n" && process == "n") {
     table = "year"
     process = "n"
   }
   
   if (table == "year" && season == "y" && process == "n") {
     table = "season"
     process = "y"
   } else if (table == "year" && season == "n" && process == "n") {
     table = "season"
     process = "n"
   }
   
  if (table == "season" && alltime == "y" && process == "n") {
    table = "alltime"
    process = "y"
  } else if (table == "season" && alltime == "n" && process == "n") {
    table = "alltime"
    process = "n"
  }
   
   
   print(table)
   print(process)

## Identify target table condition and step 2 starts
## 3.Step: Aggregate the data
   
  if (process == "y") {
    
## YEAR WEEK (1-52)  
   if (table == "year.week") {
     endtable = M
     endtable$week = week(endtable[, "Date"])
     endtable2 = endtable %>%
       group_by(endtable[,1], endtable$week) %>%
       summarize(W = min(endtable[, "Date"], na.rm = TRUE))
     

## A typical aggregation formula can not easily be used with an undetermined number of columns. A loop has been created for that purpouse
## As indexing in the summarize function with a vector containing the name of the variable does not work,
## the name is stored in a vector, then changed, refered with the changed name in the function and returned to original in endtable
     for (z in 2:(length(endtable)-1)) {
       KeepName = names(endtable)[z]
       names(endtable)[z] = "fixed"
       temp = endtable %>%
         group_by(endtable[,1], endtable$week) %>%
         summarize(W = mean(fixed, na.rm = TRUE))
       temp2 = as.data.frame(temp[[3]])
       names(endtable)[z] = KeepName
       names(temp2)[1] = KeepName
       endtable2 = cbind(as.data.frame(endtable2), temp2)
     }
     
    
## YEAR MONTH (1-12)  
     } else if (table == "year.month") {
       endtable = M
       endtable$month = month(endtable[, "Date"])
       endtable2 = endtable %>%
         group_by(endtable[,1], endtable$month) %>%
         summarize(W = min(endtable[,"Date"], na.rm = TRUE))
       
       for (z in 2:(length(endtable)-1)) {
         KeepName = names(endtable)[z]
         names(endtable)[z] = "fixed"
         temp = endtable %>%
           group_by(endtable[,1], endtable$month) %>%
           summarize(W = mean(fixed, na.rm = TRUE))
         temp2 = as.data.frame(temp[[3]])
         names(endtable)[z] = KeepName
         names(temp2)[1] = KeepName
         endtable2 = cbind(as.data.frame(endtable2), temp2)
       }
    
## WEEK
    } else if (table == "week") {
      endtable = M
      endtable$week = paste(year(endtable[, "Date"]), "-", month(endtable[, "Date"]), "-week->", week(endtable[, "Date"]))
      endtable2 = endtable %>%
        group_by(endtable[,1], endtable$week) %>%
        summarize(W = min(endtable[,"Date"], na.rm = TRUE))
      for (z in 2:(length(endtable)-1)) {
        KeepName = names(endtable)[z]
        names(endtable)[z] = "fixed"
        temp = endtable %>%
          group_by(endtable[,1], endtable$week) %>%
          summarize(W = mean(fixed, na.rm = TRUE))
        temp2 = as.data.frame(temp[[3]])
        names(endtable)[z] = KeepName
        names(temp2)[1] = KeepName
        endtable2 = cbind(as.data.frame(endtable2), temp2)
      }
    
## MONTH
    } else if (table == "month") {
      endtable = M
      endtable$month = paste(year(endtable[, "Date"]), "-", month(endtable[, "Date"]))
      endtable2 = endtable %>%
        group_by(endtable[,1], endtable$month) %>%
        summarize(W = min(endtable[,"Date"], na.rm = TRUE))
      
      for (z in 2:(length(endtable)-1)) {
        KeepName = names(endtable)[z]
        names(endtable)[z] = "fixed"
        temp = endtable %>%
          group_by(endtable[,1], endtable$month) %>%
          summarize(W = mean(fixed, na.rm = TRUE))
        temp2 = as.data.frame(temp[[3]])
        names(endtable)[z] = KeepName
        names(temp2)[1] = KeepName
        endtable2 = cbind(as.data.frame(endtable2), temp2)
      }
      
      
    
## YEAR
      } else if (table == "year") {
        endtable = M
        endtable$year = year(endtable[, "Date"])
        endtable2 = endtable %>%
          group_by(endtable[,1], endtable$year) %>%
          summarize(W = min(endtable[,"Date"], na.rm = TRUE))
        
        for (z in 2:(length(endtable)-1)) {
          KeepName = names(endtable)[z]
          names(endtable)[z] = "fixed"
          temp = endtable %>%
            group_by(endtable[,1], endtable$year) %>%
            summarize(W = mean(fixed, na.rm = TRUE))
          temp2 = as.data.frame(temp[[3]])
          names(endtable)[z] = KeepName
          names(temp2)[1] = KeepName
          endtable2 = cbind(as.data.frame(endtable2), temp2)
        }
        
## SEASON
      } else if (table == "season") {
## Season calculation: first we create the end of current season then we calculate the difference in years.
## (1 will be latest season, 2 second latest...)
        seasonend = as.POSIXlt(floor_date(now(), unit = "day"))
        seasonend = update(seasonend, month = seasonstart, mday = 1)
        if (seasonstart > month(Sys.time())) {
          seasonend$year = seasonend$year + 1
        }
        
        endtable = M
        endtable$season = as.integer(difftime(seasonend, endtable[, "Date"], units = "days")/365)
        endtable2 = endtable %>%
          group_by(endtable[,1], endtable$season) %>%
          summarize(W = min(endtable[,"Date"], na.rm = TRUE))
        
        for (z in 2:(length(endtable)-1)) {
          KeepName = names(endtable)[z]
          names(endtable)[z] = "fixed"
          temp = endtable %>%
            group_by(endtable[,1], endtable$season) %>%
            summarize(W = mean(fixed, na.rm = TRUE))
          temp2 = as.data.frame(temp[[3]])
          names(endtable)[z] = KeepName
          names(temp2)[1] = KeepName
          endtable2 = cbind(as.data.frame(endtable2), temp2)
        }
        
## ALLTIME
  } else if (table == "alltime") {
    endtable = M
    endtable2 = endtable %>%
      group_by(endtable[,1]) %>%
      summarize(W = min(endtable[, "Date"], na.rm = TRUE))
    
    for (z in 2:(length(endtable)-1)) {
      KeepName = names(endtable)[z]
      names(endtable)[z] = "fixed"
      temp = endtable %>%
        group_by(endtable[,1]) %>%
        summarize(W = mean(fixed, na.rm = TRUE))
      temp2 = as.data.frame(temp[[2]])
      names(endtable)[z] = KeepName
      names(temp2)[1] = KeepName
      endtable2 = cbind(as.data.frame(endtable2), temp2)
    }
  }
    
## 4.Step: Save table and free RAM
    
    Table.Name = paste0("Table","_", table,".csv")
    output <- file(Table.Name, "w")
    write.csv(endtable2, file = output)
    close(output)
    
    rm(endtable)
    rm(endtable2)
   
   process = "n"
   
## 5.Step: Return to 2 or end if all aggregations tables were created
   if (table == "alltime") {
     break
   }
  }
 }
}

## And... running
F.Dataggregate(Full.table, Date = "Start")