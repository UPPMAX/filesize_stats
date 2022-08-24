options(scipen=999)
library(RColorBrewer)
setwd('/proj/staff/bjornv/filesize/')

#  _                    _       _       _
# | |    ___   __ _  __| |   __| | __ _| |_ __ _
# | |   / _ \ / _` |/ _` |  / _` |/ _` | __/ _` |
# | |__| (_) | (_| | (_| | | (_| | (_| | || (_| |
# |_____\___/ \__,_|\__,_|  \__,_|\__,_|\__\__,_|
#
# Load data
filelist <-  list.files('/proj/staff/bjornv/filesize/out220506','*exts.csv$',full.names=T)

for(i in 1:length(filelist)) {
    curfile <- filelist[i]
#     fails:    /proj/staff/bjornv/filesize/out/uppstore2017130.csv
#    if(curfile=="/proj/staff/bjornv/filesize/out/all_projects_size.csv" || curfile=="/proj/staff/bjornv/filesize/out/all.csv" || curfile=="/proj/staff/bjornv/filesize/out/uppstore2017170.csv"){
#        next
#    }
    cat(i,'/', length(filelist), ' (', round(i/length(filelist)*100,1),'%)',  '\t',curfile,sep='')
    if(file.size(curfile) == 14) { cat("\t Empty file \n")  ; next }

    data <- read.table(curfile, sep='\t', header=T, stringsAsFactors=F, colClasses=c('character',rep('numeric',2)),col.names=c('ext', 'size', 'freq'), encoding='UTF-8', comment.char="" )

    #  ____  _       _
    # |  _ \| | ___ | |_
    # | |_) | |/ _ \| __|
    # |  __/| | (_) | |_
    # |_|   |_|\___/ \__|
    #
    # Plot pie chart
    # Don't plot 0 values
    data_pie_size <- data
    data_pie_size <- data_pie_size[order(data_pie_size$size,decreasing=T),]
    data_pie_freq <- data
    data_pie_freq <- data_pie_freq[order(data_pie_freq[,'freq'],decreasing=T),]

    if(nrow(data) > 1000) {
        cat(paste0('\t', 'nrow(data) > 1000 plotting fewer points'))
        data_pie_size[data_pie_size$size > 1e8,]
        data_pie_freq[data_pie_freq$freq > 10,]
    }

    sizeGB <- round(sum(data$size) / (1024^3),1)

    # skip projects with only 1 line
    if(nrow(data_pie_size) < 2) {
        cat(paste0('\t', 'nrow(data_pie_size < 2'))
        next
    }

    # Settings 1 row 2 columns
    png(paste0('out/',sub('\\.csv' ,'', basename(curfile)), '.png'),width=4000,height=2000,pointsize=30)
    par(mfrow=c(1,2), mar=c(0,4,4,4), mai=c(0,1,2,1))
    
    # 1st pie
    par(mar=c(0,4,4,4))
    pie(data_pie_size$size,labels=paste0(data_pie_size$ext, ', ', round(data_pie_size$size / (1024^3),1)),cex=1.2, col=RColorBrewer::brewer.pal(name='Set1',9),clockwise=T, init.angle=0,lty=0)
    # sum over data not pie_size
    title(main=paste0("Size (GB), Tot: ",sizeGB), line=-3,cex.main=1.5)

    # 2nd pie
    pie(data_pie_freq$freq,labels=paste0(data_pie_freq$ext, ', ', data_pie_freq$freq),cex=1.2, col=RColorBrewer::brewer.pal(name='Set1',9),clockwise=T, init.angle=0,lty=0)
    title(main=paste0("Number of files, Tot: ",sum(data$freq)),line=-3,cex.main=1.5)

    #title(main=basename(curfile),line=-3,outer=T,cex.main=2)
    title(main=paste0('Project summary: ', sub('\\.csv' ,'', basename(curfile))),line=-3,outer=T,cex.main=2)
    dev.off()
    cat('\n')
}






gc()
