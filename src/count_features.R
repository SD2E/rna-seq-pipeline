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
  make_option(c("-bpath", "--bamfilespath"), type="character", action="store", default=FALSE,
              help="Directory path to bam files", metavar="character"),
  make_option(c("-anno", "--annotation"), type="character", action="store", default=FALSE,
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
files_location=file.path(opt$bamfiles)
# Start writing to an output file
logfile=paste("files_location", "_log.txt",sep="")
sink(logfile)

###### Read Count Aggregation ##################
paste(Sys.Date(), "_and_",Sys.time(),sep="")



if(dir.exists(files_location)){
  message("Bam file path exists")
}

files_location=opt$bamfiles
print(files_location)
bam.files <- list.files(files_location, pattern = "*RG.bam$",full.names=TRUE, recursive = TRUE)
bamFileList <- BamFileList(bam.files)
print(bam.files)
# Working through annotation file passed by user
gtf <- opt$annotation
gInterval<-readGff3(gtf, quiet=TRUE)

#Calculate read count data frame
output <- "ReadCountMatrix_preCAD"
#fc <- featureCounts(files=bam.files,annot.ext=gtf,isGTFAnnotationFile=TRUE,GTF.featureType="gene",GTF.attrType="gene",isPairedEnd=TRUE,requireBothEndsMapped=FALSE,countMultiMappingReads=TRUE)
fc <- featureCounts(files=bam.files,annot.ext=gtf,isGTFAnnotationFile=TRUE,GTF.featureType="gene",GTF.attrType="gene",isPairedEnd=TRUE,requireBothEndsMapped=FALSE)
prefix=paste('X', gsub('[/,-]','.',files_location), '.', sep = '')
colnames(fc$counts) <- gsub(prefix,'',colnames(fc$counts))
colnames(fc$counts) <- gsub('_MG1655.*','',colnames(fc$counts))

counts <- fc$counts
counts <- as.data.frame(counts)
counts <- cbind(gene_id=rownames(counts),counts)


write.table(counts,paste(output,".tsv",sep=""),col.names=TRUE,row.names=FALSE,quote=F,sep="\t",append=F)




# Author: Andy Saurin (andrew.saurin@univ-amu.fr)
#
# Simple RScript to calculate RPKMs and TPMs
# based on method for RPKM/TPM calculations shown in http://www.rna-seqblog.com/rpkm-fpkm-and-tpm-clearly-explained/
#
# The input file is the output of featureCounts
#

rpkm <- function(counts, lengths) {
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

cat('Performing RPKM calculations...')

rpkms <- apply(counts, 2, function(x) rpkm(x, lengths) )
ftr.rpkm <- cbind(gene_id=ftr.cnt[,1], rpkms)

write.table(ftr.rpkm, paste(output,"_FPKM.tsv",sep=""), sep="\t", row.names=FALSE, quote=FALSE)
cat(' Done.\n\tSaved as ')
paste(output,"_FPKM.tsv",sep="")

cat('Performing TPM calculations...')

tpms <- apply(counts, 2, function(x) tpm(x, lengths) )

ftr.tpm <- cbind(gene_id=ftr.cnt[,1], tpms)

write.table(ftr.tpm, paste(output,"_TPM.tsv",sep=""), sep="\t", row.names=FALSE, quote=FALSE)
cat(' Done.\n\tSaved as ')
paste(output,"_RPKM.tsv",sep="")
