options(scipen=999)
library(doParallel,quietly=T)
library(foreach,quietly=T)
registerDoParallel(30)

myargs = commandArgs(trailingOnly=TRUE)
input <- myargs[1]
output <- myargs[2]

print(paste0('Output: ', output))
print(paste0('Input: ', input))

dir.create(paste0(output,'/project_plots'),showWarnings=F)

# Load storage statistics
read_stats_csv <- function(stat, colClass, min_size = 15, input) {

    filelist <- list.files(input,pattern=paste0('*',stat,'\\.csv'))
    filelist <- filelist[filelist != paste0('all.',stat,'.csv')]
    filelist <- filelist[file.size(paste0(input,'/',filelist)) > min_size]
    projectlist <- list()
    # Add all projects to a list
    projectlist <- foreach(i = 1:length(filelist)) %dopar%  {
    #for(i in 1:length(filelist)) {
        curfile <- filelist[i]
        cat(i,'/', length(filelist),  '\t',curfile,'\n')
        projectlist[[i]] <- read.table(paste0(input, '/', curfile), 
                                        sep='\t', 
                                        header=T, 
                                        stringsAsFactors=F, 
                                        colClasses=c(colClass,rep('numeric',2)), 
                                        col.names=c(stat, 'size', 'freq'), 
                                        encoding='UTF-8', 
                                        comment.char="" )
    }
    names(projectlist) <- sub(paste0('.', stat, '.csv'),'',filelist)
    return(projectlist)

}

#  _____      _                 _
# | ____|_  _| |_ ___ _ __  ___(_) ___  _ __  ___
# |  _| \ \/ / __/ _ \ '_ \/ __| |/ _ \| '_ \/ __|
# | |___ >  <| ||  __/ | | \__ \ | (_) | | | \__ \
# |_____/_/\_\\__\___|_| |_|___/_|\___/|_| |_|___/
#
# Extensions
# Load 
#file <- "/crex/proj/staff/bjornv/filesize/out.dahlo220531/perl/all.exts.csv"
file <- paste0(input,"/all.exts.csv")
all_extsum <- read.table(file,sep='\t',header=T,stringsAsFactors=F, colClasses=c('character','numeric','numeric'))
all_extsum$sizeTB <- round(all_extsum$size / 1024^4 ,2)

# For each project
extlist <- read_stats_csv(stat = 'exts', colClass = 'character', min_size = 0, input=input)
extsum <- data.frame(project=names(extlist),
                      size=sapply(extlist,function(x) {ifelse(nrow(x) != 0, sum(x[2]),0)}),
                      Freq=sapply(extlist,function(x) {ifelse(nrow(x) != 0, sum(x[3]),0)}),
                      stringsAsFactors=F,
                      row.names=NULL)
extsum <- extsum[order(extsum$size,decreasing=T),]
extsum$sizeTB <- round(extsum$size / 1024^4,2) 


# __   __
# \ \ / /__  __ _ _ __
#  \ V / _ \/ _` | '__|
#   | |  __/ (_| | |
#   |_|\___|\__,_|_|
#
# Year
yearlist <- read_stats_csv(stat = 'years', colClass = 'integer', min_size = 15, input=input)
yearsum <- data.frame(project=names(yearlist),stringsAsFactors=F)
years <- sort(unique(unlist(sapply(yearlist,function(x) {unique(x$year)}))))
for(year in years) {
    yearsum <- cbind(yearsum,sapply(yearlist,function(x) {sum(x$size[x$year == year])} ))
}
colnames(yearsum) <- c('project',years)


#  _                    _   _
# | |    ___   ___ __ _| |_(_) ___  _ __  ___
# | |   / _ \ / __/ _` | __| |/ _ \| '_ \/ __|
# | |__| (_) | (_| (_| | |_| | (_) | | | \__ \
# |_____\___/ \___\__,_|\__|_|\___/|_| |_|___/
#
# Locations
locationlist <- read_stats_csv(stat = 'locations', colClass = 'character', min_size = 19, input=input)
locationsum <- data.frame(project=names(locationlist),stringsAsFactors=F)
for(location in c('backup','nobackup')) {
    locationsum <- cbind(locationsum,sapply(locationlist,function(x) {sum(x$size[x$location == location])} ))
}
colnames(locationsum) <- c('project','backup','nobackup')

# Merge
proj_sum <- merge(extsum,yearsum,by=1,all.x=T,sort=F)
proj_sum <- merge(proj_sum,locationsum,by=1,all.x=T,sort=F)


prettysum <- function(X) {
    sub(perl=T,'^\\s+','',format(sum(X),big.mark=' '))
}
pretty <- function(X) {
    sub(perl=T,'^\\s+','',format(X,big.mark=' '))
}

#  ____  _       _
# |  _ \| | ___ | |_
# | |_) | |/ _ \| __|
# |  __/| | (_) | |_
# |_|   |_|\___/ \__|
#
# Plot pie chart
png(paste0(output,'/all_projects_piechart.png'), width=3800, height=2000,pointsize=26)

#projsizeTB <- round(sum(projsum$sizeTB),2)
#extsizeTB <- round(sum(extsum$sizeTB),2)

#par(mfrow=c(1,1), mar=c(0,4,4,4), mai=c(0,3,0,3))
par(mfrow=c(2,3), mar=c(0,6,0,6), mai=c(0,4,2,4))
par(mar=c(0,8,4,8))
title_line <- -2
main_line <- -3

# 1st pie. Project size on uppmax
pie(proj_sum$size,labels=paste0(proj_sum$project, ', ', round(proj_sum$sizeTB)),
    cex=1.2, col=RColorBrewer::brewer.pal(name='Set1',9),clockwise=T, init.angle=0,lty=0)
#title(main=paste0("Project size , Tot: ", prettysum(round(proj_sum$sizeTB)), ' TB'), 
title(main=paste0("Project size , Tot: ", pretty(round(sum(proj_sum$size) / 1024^4)), ' TB'), 
      line=title_line,cex.main=2)

# 2nd pie. Project number of files on uppmax
projsum_pie_Freq <- proj_sum[order(proj_sum$Freq),]
pie(projsum_pie_Freq$Freq,labels=paste0(projsum_pie_Freq$project, ', ', pretty(projsum_pie_Freq$Freq)),
    cex=1.2, col=RColorBrewer::brewer.pal(name='Set1',9),clockwise=F, init.angle=0,lty=0)
title(main=paste0("Number of files per project, Tot: ", prettysum(projsum_pie_Freq$Freq)),
      line=title_line,cex.main=2)

# 3rd pie. Size per year
year_sum <- rev(apply(proj_sum[,5:(ncol(proj_sum)-2)],2,sum,na.rm=T))
pie(year_sum,clockwise=T, 
    labels=paste0(names(year_sum), ', ', round(year_sum / 1024^5,1)) ,
    init.angle=0, 
    col= RColorBrewer::brewer.pal(name='Set1',length(years)))
title(main="File size by file date (PB)", sub='Based of file modification date', 
      line=title_line,cex.main=2)

# 4th pie. Size of extensions
all_extsum_pie_size <- all_extsum[order(all_extsum$size,decreasing=T),]
sizeix <- all_extsum_pie_size$size > 1e12
pie(all_extsum_pie_size$size[sizeix],
    labels=paste0(all_extsum_pie_size$ext[sizeix],', ', pretty(round(all_extsum_pie_size$sizeTB[sizeix]))),
    col=RColorBrewer::brewer.pal(name='Set1',9),clockwise=T, 
    cex=1.2, init.angle=0,lty=0)
#title(main=paste0("Extension size , Tot: ",round(sum(all_extsum_pie_size$sizeTB)), ' TB'),
title(main=paste0("Extension size , Tot: ", pretty(round(sum(all_extsum_pie_size$size)/ 1024^4)), ' TB'),
      line=title_line,cex.main=2)

# 5th pie. Size of extensions
all_extsum_pie_freq <- all_extsum[order(all_extsum$freq,decreasing=T),]
all_extsum_pie_freq$ext[is.na(all_extsum_pie_freq$ext)] <- 'NA'
freqix <- all_extsum_pie_freq$freq > 1e5
pie(all_extsum_pie_freq$freq[freqix],
    labels=paste0(all_extsum_pie_freq$ext[freqix], ', ', pretty(round(all_extsum_pie_freq$freq[freqix]))) , 
    col=RColorBrewer::brewer.pal(name='Set1',9),clockwise=T, 
    cex=1.2, init.angle=0,lty=0)
title(main=paste0("Number of files by extension , Tot: ",prettysum(all_extsum_pie_freq$freq)),
      line=title_line,cex.main=2)

# 6th pie. Backup nobackup
backup_sum <- rev(apply(proj_sum[,(ncol(proj_sum)-1):(ncol(proj_sum))],2,sum,na.rm=T))
pie(backup_sum,
    clockwise=T, labels=paste0(names(backup_sum), ', ', round(backup_sum / 1024^5,1)), 
    init.angle=0, col= RColorBrewer::brewer.pal(name='Set1',3)[1:2])
title(main="Backup vs nobackup size (PB)", line=title_line,cex.main=2)

title(main=paste0('Storage statistics: '),
      line=main_line,outer=T,cex.main=2)

dev.off()


# __        __    _ _         _           __ _ _
# \ \      / / __(_) |_ ___  | |_ ___    / _(_) | ___
#  \ \ /\ / / '__| | __/ _ \ | __/ _ \  | |_| | |/ _ \
#   \ V  V /| |  | | ||  __/ | || (_) | |  _| | |  __/
#    \_/\_/ |_|  |_|\__\___|  \__\___/  |_| |_|_|\___|
#
# Write to file

# Project summary
write.table(proj_sum,file=paste0(output,'/all_projects_size.csv'), quote=F,sep='\t',row.names=F,col.names=T)
proj_sum_gb <- proj_sum
proj_sum_gb[,c(2,seq(5,ncol(proj_sum)))] <- round(proj_sum[,c(2,seq(5,ncol(proj_sum)))] / 1024^3)
write.table(proj_sum_gb,file=paste0(output,'/all_projects_size_in_GB.csv'), quote=F,sep='\t',row.names=F,col.names=T)

# Extension summary
write.table(all_extsum_pie_size,file=paste0(output,'/all_extensions_size.csv'), quote=F,sep='\t',row.names=F,col.names=T)



#  ____                             _           _
# |  _ \ ___ _ __   _ __  _ __ ___ (_) ___  ___| |_
# | |_) / _ \ '__| | '_ \| '__/ _ \| |/ _ \/ __| __|
# |  __/  __/ |    | |_) | | | (_) | |  __/ (__| |_
# |_|   \___|_|    | .__/|_|  \___// |\___|\___|\__|
#                  |_|           |__/
# Per project

a <- foreach(project =  proj_sum$project) %dopar% {
#for(project in proj_sum$project) {
    # Get indexies for each project
    ix <- which(proj_sum$project == project)
    ixextlist <-  which(names(extlist) == project)
    proj_extsum <- extlist[[ixextlist]]
    proj_extsum$sizeGB <- round(proj_extsum$size / 1024^3,2) 

    cat(ix,'/', nrow(proj_sum),  '\t',project,'\n')
    
    if(!any(names(yearlist) %in% project)) return()
    ixyearlist <-  which(names(yearlist) == project)
    proj_yearsum <- yearlist[[ixyearlist]]
    proj_yearsum <- proj_yearsum[order(proj_yearsum$years,decreasing=T),]

    ixlocationlist <-  which(names(locationlist) == project)
    proj_locationsum <- locationlist[[ixlocationlist]]
    proj_locationsum <- proj_locationsum[order(proj_locationsum$locations,decreasing=T),]
    # Add a 0 if backup/nobackup doesn't exist
    if(nrow(proj_locationsum) == 1) {
        ifelse(proj_locationsum$location == 'backup',
               proj_locationsum[2,] <- c('nobackup',0,0),
               proj_locationsum[2,] <- c('backup',0,0))
    }

    # Pieplot fewer pies
    proj_extsum_pie_size <- proj_extsum[order(proj_extsum$size,decreasing=T),]
    if(nrow(proj_extsum_pie_size) > 50) proj_extsum_pie_size <- proj_extsum_pie_size[1:50,] 
    if(nrow(proj_extsum_pie_size) < 2 | 
       var(proj_extsum_pie_size$size) == 0 | 
       var(proj_extsum_pie_size$freq) == 0) return() 

    # Start plotting
    png(paste0(output,'/project_plots/',project, '.png'),width=2500,height=2500,pointsize=30)
    par(mfrow=c(2,2), mar=c(0,4,4,4), mai=c(0,1,2,1))
    title_line <- -2
    main_line <- -3

    # 1th pie. Size of extensions
    proj_extsum_pie_size <- proj_extsum[order(proj_extsum$size,decreasing=T),]
    if(nrow(proj_extsum_pie_size) > 50) proj_extsum_pie_size <- proj_extsum_pie_size[1:50,] 
    if(nrow(proj_extsum_pie_size) < 2) return() 

    pie(proj_extsum_pie_size$size,
        labels=paste0(proj_extsum_pie_size$ext,', ', pretty(round(proj_extsum_pie_size$sizeGB))),
        col=RColorBrewer::brewer.pal(name='Set1',9),clockwise=T, 
        cex=1.2, init.angle=0,lty=0)
    title(main=paste0("Extension size , Tot: ",pretty(round(sum(proj_extsum$size) / 1024^3,1)), ' GB'), line=title_line,cex.main=2)

    # 2nd pie. number of files
    proj_extsum_pie_freq <- proj_extsum[order(proj_extsum$freq,decreasing=T),]
    proj_extsum_pie_freq$ext[is.na(proj_extsum_pie_freq$ext)] <- 'NA'
    if(nrow(proj_extsum_pie_freq) > 50) proj_extsum_pie_freq <- proj_extsum_pie_freq[1:50,] 
    pie(proj_extsum_pie_freq$freq,
        labels=paste0(proj_extsum_pie_freq$ext, ', ', pretty(round(proj_extsum_pie_freq$freq))) , 
        col=RColorBrewer::brewer.pal(name='Set1',9),clockwise=T, 
        cex=1.2, init.angle=0,lty=0)
    title(main=paste0("Number of files by extension , Tot: ",sum(proj_extsum$freq)), line=title_line,cex.main=2)

    # 3rd pie. Size per year
    pie(proj_yearsum$size,clockwise=T, 
        labels=paste0(proj_yearsum$years, ', ', pretty(round(proj_yearsum$size / 1024^3,1))) ,
        init.angle=0, 
        col= RColorBrewer::brewer.pal(name='Set1',length(years)))
    title(main="File size by file date (GB)", sub='Based of file modification date', line=title_line,cex.main=2)

    # 4th pie. Backup nobackup
    pie(as.numeric(proj_locationsum$size),clockwise=T, 
        labels=paste0(proj_locationsum$locations, ', ', pretty(round(as.numeric(proj_locationsum$size) / 1024^3,1))) ,
        init.angle=0, col= RColorBrewer::brewer.pal(name='Set1',3)[1:2])
    title(main="Backup vs nobackup size (GB)", line=title_line,cex.main=2)

    title(main=paste0(project,' storage statistics:'),line=main_line,outer=T,cex.main=2)
    dev.off()
}


print('Finished. Thank you and have a nice day.')










#
#filelist <- list.files('.','*exts\\.csv')
#filelist <- filelist[filelist != c('all.exts.csv')]
#statslist <- list()
## Add all projects to a list
#for(i in 1:length(filelist)) {
#    curfile <- filelist[i]
#    cat(i,'/', length(filelist),  '\t',curfile,'\n')
#    statslist[[i]] <- read.table(curfile, 
#                                 sep='\t', 
#                                 header=T, 
#                                 stringsAsFactors=F, 
#                                 colClasses=c('character',rep('numeric',2)), 
#                                 col.names=c('ext', 'size', 'freq'), 
#                                 encoding='UTF-8', 
#                                 comment.char="" )
#}
#
## Sum of all sizes 
#projsum <- data.frame(project=filelist,
#                      size=sapply(statslist,function(x) {ifelse(nrow(x) != 0, sum(x[2]),0)}),
#                      Freq=sapply(statslist,function(x) {ifelse(nrow(x) != 0, sum(x[3]),0)}))
#projsum$project <- sub('.exts.csv','',projsum$project)
#projsum <- projsum[order(projsum$size,decreasing=T),]
#projsum$sizeTB <- round(projsum$size / 1024^4,2) 
#
## __   __
## \ \ / /__  __ _ _ __
##  \ V / _ \/ _` | '__|
##   | |  __/ (_| | |
##   |_|\___|\__,_|_|
##
## Year
#
#filelist <- list.files('.','*years\\.csv')
#filelist <- filelist[filelist != c('all.years.csv')]
#filelist <- filelist[file.size(filelist) > 15]
#yearslist <- list()
## Add all projects to a list
#for(i in 1:length(filelist)) {
#    curfile <- filelist[i]
#    cat(i,'/', length(filelist),  '\t',curfile,'\n')
#    yearslist[[i]] <- read.table(curfile, 
#                                 sep='\t', 
#                                 header=T, 
#                                 stringsAsFactors=F, 
#                                 colClasses=c('integer',rep('numeric',2)), 
#                                 col.names=c('year', 'size', 'freq'), 
#                                 encoding='UTF-8', 
#                                 comment.char="" )
#}
#
## Sum of all years 
#yearsum <- data.frame(project=filelist,stringsAsFactors=F)
#yearsum$project <- sub('.years.csv','',yearsum$project)
#years <- sort(unique(unlist(sapply(yearslist,function(x) {unique(x$year)}))))
#for(year in years) {
#    yearsum <- cbind(yearsum,sapply(yearslist,function(x) {sum(x$size[x$year == year])} ))
#}
#colnames(yearsum) <- c('project',years)
#
#
##  _                    _   _
## | |    ___   ___ __ _| |_(_) ___  _ __  ___
## | |   / _ \ / __/ _` | __| |/ _ \| '_ \/ __|
## | |__| (_) | (_| (_| | |_| | (_) | | | \__ \
## |_____\___/ \___\__,_|\__|_|\___/|_| |_|___/
##
## Locations
#filelist <- list.files('.','*locations\\.csv')
#filelist <- filelist[filelist != c('all.locations.csv')]
#filelist <- filelist[file.size(filelist) > 19]
#locationlist <- list()
## Add all projects to a list
#for(i in 1:length(filelist)) {
#    curfile <- filelist[i]
#    cat(i,'/', length(filelist),  '\t',curfile,'\n')
#    locationlist[[i]] <- read.table(curfile, 
#                                 sep='\t', 
#                                 header=T, 
#                                 stringsAsFactors=F, 
#                                 colClasses=c('character',rep('numeric',2)), 
#                                 col.names=c('location', 'size', 'freq'), 
#                                 encoding='UTF-8', 
#                                 comment.char="" )
#}
#
## Sum of backup and no backup 
#locationsum <- data.frame(project=filelist,stringsAsFactors=F)
#locationsum$project <- sub('.locations.csv','',locationsum$project)
#for(location in c('backup','nobackup')) {
#    locationsum <- cbind(locationsum,sapply(locationlist,function(x) {sum(x$size[x$location == location])} ))
#}
#colnames(locationsum) <- c('project','backup','nobackup')



