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
library("rjson")


option_list = list(
  make_option(c("-b", "--bamfilespath"), type="character", action="store", default=FALSE,
              help="JSON dictionary of sampleID: bam file location", metavar="character"),
  make_option(c("-a", "--annotation"), type="character", action="store", default=FALSE,
              help="Annotation file in gtf/gff/gff3 format", metavar="character"),
  make_option(c("-v", "--verbose"), action="store_true", default=TRUE,
              help="Should the program print extra stuff out? [default %default]")
  );


opt_parser = OptionParser(usage = "count_features.R [options]", option_list=option_list);
opt = parse_args(opt_parser)

if (opt$v) {
  # you can use either the long or short name
  # so opt$bpath and opt$bamfilespath are the same.
  cat("bamfilespath:\n")
  cat(opt$bamfilespath)
  cat("\n\nbpath:\n")
  cat(opt$bpath)

  # show the user what anoo is
  cat("\n\nanno:\n")
  cat(opt$annotation)
}

#Print Error

if(!is.na(opt$bamfilespath) & !is.na(opt$annotation)) {
  cat("Here are strings bpath and anno together at last:\n")
  cat(paste(opt$bpath,opt$anno,sep=''))
  cat("\n\n")
} else {
  cat("You didn't specify both variables bamfilepath and annotation file\n", file=stderr()) # print error messages to stderr
}


#Check if directory exists
dir.exists<-function (x)
{
  res <- file.exists(x) & file.info(x)$isdir
  setNames(res, x)
}

#Reading bam files from user's input location
print(opt$bamfiles)
print(opt$annotation)
files_json <- fromJSON(file = opt$bamfiles)
#files_df <- as.data.frame(files_json)
files_location=file.path(files_json)
# Start writing to an output file
logfile=paste("files_location", "_log.txt",sep="")
sink(logfile)

###### Read Count Aggregation ##################
paste(Sys.Date(), "_and_",Sys.time(),sep="")


file_checks <- 0
for (afile in files_location){
  if(file.exists(afile)){
    file_checks <- file_checks + 0
  } else {
    file_checks <- file_checks - 1
    print(afile)
  }
}

if(file_checks == 0){
  message("Bam file paths exist")
} else {
  message("Not all Bams exist")
}

#bam.files <- list.files(files_location, pattern = "*/*/*RG.bam$",full.names=TRUE, recursive = TRUE)
bamFileList <- BamFileList(files_location)
print(files_location)
# Working through annotation file passed by user
gtf <- opt$annotation
gInterval<-readGff3(gtf, quiet=TRUE)

#Calculate read count data frame
output <- "ReadCountMatrix_preCAD"
#fc <- featureCounts(files=bam.files,annot.ext=gtf,isGTFAnnotationFile=TRUE,GTF.featureType="gene",GTF.attrType="gene",isPairedEnd=TRUE,requireBothEndsMapped=FALSE,countMultiMappingReads=TRUE)
fc <- featureCounts(files=files_location,annot.ext=gtf,isGTFAnnotationFile=TRUE,GTF.featureType="gene",GTF.attrType="gene_id",isPairedEnd=TRUE,requireBothEndsMapped=FALSE,nthreads=150)
#prefix=paste('X', gsub('[/,-]','.',files_location), '.', sep = '')
#colnames(fc$counts) <- gsub(prefix,'',colnames(fc$counts))
## TODO: stop string parsing and just use the new input sample: path dictionary
#colnames(fc$counts) <- gsub('.*.sample','sample',colnames(fc$counts))
#colnames(fc$counts) <- gsub('.MG1655.*','',colnames(fc$counts))
sample_path_pairs <- list(0)
for (i in names(files_json)) {
    sample_id <- i
    sample_path <- files_json[i]
    sample_path <- gsub('[-,_,/]','.',sample_path)
    sample_path <- gsub('.*.sample','sample',sample_path)
    sample_path <- gsub("\\.\\.","\\.",sample_path)
    sample_path_pairs[sample_path] <- sample_id
}

# First tries to use sample ID that was passed in the sample_path.json
# if they keys don't match it does string parsing on the sample path that was provided
for (column in colnames(fc$counts)) {
    if (!is.na(names(sample_path_pairs[column]))) {
    colnames(fc$counts)[colnames(fc$counts)==column] <- sample_path_pairs[column]
    print("found")
  } else {
    name <- gsub('.*.sample','sample',column)
    name <- gsub('.B.*', '', name)
    name <- gsub('.Ba*', '', name)
    name <- gsub('.MG1655.*','', name)
    print("parsing: ")
    print(name)
    colnames(fc$counts)[colnames(fc$counts)==column] <- name
  }
}

counts <- fc$counts
counts <- as.data.frame(counts)
counts <- cbind(gene_id=rownames(counts),counts)


write.table(counts,paste(output,".tsv",sep=""),col.names=TRUE,row.names=FALSE,quote=F,sep="\t",append=F)

transposed_counts <- as.data.frame(t(counts[,-1]))
#colnames(transposed__counts) <- counts[,1]
transposed_counts <- cbind(sample_id=rownames(transposed_counts),transposed_counts)
write.table(transposed_counts,paste(output,"_transposed.tsv",sep=""),col.names=TRUE,row.names=FALSE,quote=F,sep="\t",append=F)


# Author: Andy Saurin (andrew.saurin@univ-amu.fr)
#
# Simple RScript to calculate RPKMs and TPMs
# based on method for RPKM/TPM calculations shown in http://www.rna-seqblog.com/rpkm-fpkm-and-tpm-clearly-explained/
#
# The input file is the output of featureCounts
#

fpkm <- function(counts, lengths) {
  pm <- sum(counts) /1e6
  rpm <- counts/pm
  rpm/(lengths/1000)
}

tpm <- function(counts, lengths) {
  rpk <- counts/(lengths/1000)
  coef <- sum(rpk) / 1e6
  rpk/coef
}


ftr.cnt <- data.frame(fc$annotation,fc$counts,stringsAsFactors=FALSE)

if ( ncol(ftr.cnt) < 7 ) {
	cat(' The input file is not the raw output of featureCounts (number of columns > 6) \n')
	quit('no')
}

lengths = ftr.cnt[,6]

counts <- ftr.cnt[,7:ncol(ftr.cnt)]

cat('Performing FPKM calculations...')

fpkms <- apply(counts, 2, function(x) fpkm(x, lengths) )
ftr.fpkm <- cbind(gene_id=ftr.cnt[,1], fpkms)

write.table(ftr.fpkm, paste(output,"_FPKM.tsv",sep=""), sep="\t", row.names=FALSE, quote=FALSE)
cat(' Done.\n\tSaved as ')
paste(output,"_FPKM.tsv",sep="")

transposed_fpkm_counts <- as.data.frame(t(ftr.fpkm[,-1]))
transposed_fpkm_counts <- cbind(sample_id=rownames(transposed_fpkm_counts),transposed_fpkm_counts)
write.table(transposed_fpkm_counts,paste(output,"_FPKM_transposed.tsv",sep=""),col.names=TRUE,row.names=FALSE,quote=F,sep="\t",append=F)

cat('Performing TPM calculations...')

tpms <- apply(counts, 2, function(x) tpm(x, lengths) )

ftr.tpm <- cbind(gene_id=ftr.cnt[,1], tpms)

write.table(ftr.tpm, paste(output,"_TPM.tsv",sep=""), sep="\t", row.names=FALSE, quote=FALSE)
cat(' Done.\n\tSaved as ')
paste(output,"_TPM.tsv",sep="")

transposed_tpm_counts <- as.data.frame(t(ftr.tpm[,-1]))
transposed_tpm_counts <- cbind(sample_id=rownames(transposed_tpm_counts),transposed_tpm_counts)
write.table(transposed_tpm_counts,paste(output,"_TPM_transposed.tsv",sep=""),col.names=TRUE,row.names=FALSE,quote=F,sep="\t",append=F)
