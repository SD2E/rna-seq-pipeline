#! /usr/bin/env Rscript
#' ---
#' title: "Raw Read Counts for RNAseq data - Paired or Unpaired sequences"
#' author: "usaxena"
#' date: paste(Sys.Date(), "_and_",Sys.time(),sep="")
#' ---
#'
#Uma Saxena, MIT/Broad Foundry, 8th May 2018
#Performs feature counts on paired read sequences. Input data is path to bam file directory
# usage: Rscript --vanilla count_features.R --bamfilespath user/study/bamfiles_path --annotation /user/reference/annotation.gff
suppressPackageStartupMessages(require(optparse))
library("optparse")
library("Rsubread")
library("Rsamtools")
library("genomeIntervals")


option_list = list(
  make_option(c("-bpath", "--bamfilespath"), type="character", action="store_true", default=FALSE, 
              help="Directory path to bam files", metavar="character"),
  make_option(c("-anno", "--annotation"), type="character", action="store_true", default=FALSE,
              help="Annotation file in gtf/gff/gff3 format", metavar="character")
  ); 


opt_parser = OptionParser(usage = "count_features.R [options]", option_list=option_list);
opt = parse_args(opt_parser)


#Check if directory exists
dir.exists<-function (x)
{
  res <- file.exists(x) & file.info(x)$isdir
  setNames(res, x)
}

#Reading bam files from user's input location
print(opt$bamfiles)
print(opt$annotation)
files_location=file.path(opt$bamfiles)
paste(Sys.Date(), "_and_",Sys.time(),sep="")

# Start writing to an output file
logfile=paste("files_location", "_log.txt",sep="")
sink(logfile)


if(dir.exists(files_location)){
  message("Bam file path exists")
}

files_location=opt$bamfiles
print(files_location)
bam.files <- list.files(files_location, pattern = "*.bam$",full.names=TRUE, recursive = TRUE)
bamFileList <- BamFileList(bam.files)
print(bam.files)
# Working through annotation file passed by user
gtf <- opt$annotation
gInterval<-readGff3(gtf, quiet=TRUE)

#Calculate read count data frame
output <- "ReadCountMatrix_preCAD"
fc <- featureCounts(files=bam.files,annot.ext=gtf,isGTFAnnotationFile=TRUE,GTF.featureType="gene",isPairedEnd=TRUE,requireBothEndsMapped=FALSE)
write.table(fc$counts,paste(files_location,"/",output,".tsv",sep=""),quote=F,sep="\t",append=F)
counts=fc$counts

#system(sprintf("multiqc %s", files_location))
system(paste("multiqc", files_location, sep=""))
sink()