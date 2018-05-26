#! /usr/bin/env Rscript
library("knitr")
library("httr")
#set_config( config( ssl_verifypeer = 0L ) )
#library("ulimit")
#ulimit::memory_limit(40000)
#rmarkdown::render("/opt/src/count_features.R", "pdf_document")
#' ---
#' title: "Raw Read Counts for RNAseq data - Paired or Unpaired sequences"
#' author: "usaxena"
#' date: paste(Sys.Date(), "_and_",Sys.time(),sep="")
#' ---
#'
#Uma Saxena, MIT/Broad Foundry, 8th May 2018
#Performs feature counts on paired read sequences. Input data is path to bam file directory
# usage: Rscript --vanilla count_features.R -bpath user/study/bamfiles -a /user/reference/annotation.gff3 -o /user/data/studyname_readcount
suppressPackageStartupMessages(require(optparse))
library("BiocParallel")
library("optparse")
library("Rsubread")
library("scatterplot3d")
library("genomeIntervals")
library("Rsamtools")
library("parallel")


option_list = list(
  make_option(c("-bpath", "--bamfilespath"), type="character", default=NULL,
              help="Directory path to bam files", metavar="character"),
  make_option(c("-a", "--annotation"), type="character", default=NULL,
              help="annotation file in gtf/gff/gff3 format", metavar="character"),
  make_option(c("-o", "--out"), type="character", default=paste(opt$bamfilespath,"/",Sys.time(),"_readcount",sep=""),
              help="output file name [default= %default]", metavar="character")
);

opt_parser = OptionParser(option_list=option_list);
opt = parse_args(opt_parser)

if (is.null(opt$file)){
  print_help(opt_parser)
  stop("Please supply input files", call.=FALSE)
}

#Reading bam files from user's input location
files_location=opt$bamfiles
#bam.files <- list.files(files_location,pattern=".*bam$")
bam.files <- dir(system.file(files_location), pattern="*.bam$",full.names=TRUE)
#genomic.alignments <- mclapply(bam.files,readGAlignmentsFromBam)
bamFileList <- BamFileList(bam.files,yieldSize=10^6)

# Working through annotation file passed by user
gtf <- opt$annotation
gInterval<-readGff3(gtf, quiet=TRUE)
output <- opt$out

props <- propmapped(files=bam.files)

knitr::kable(head(props))
fc <- featureCounts(files=bam.files,annot.ext=gtf,isGTFAnnotationFile=TRUE,GTF.featureType="gene",isPairedEnd=TRUE,requireBothEndsMapped=FALSE)
#' Print out the stats from the featureCount object
knitr::kable(fcLim$stat)
write.table(fc$counts,paste(opt$bamfiles,"/",output,".tsv",sep=""),quote=F,sep="\t",append=F)

counts=fc$counts

#' Density plots of log-intensity distribution of each library can be superposed on a single graph for a better comparison between libraries and for identification of libraries with weird distribution. On the boxplots the density distributions of raw log-intensities are not expected to be identical but still not totally different.

#' density plot of raw read counts (log10)
png(file=paste(opt$bamfiles,"/",output,"_Raw_read_counts_per_gene.density.png", sep=""))
logcounts <- log(counts$counts[,1],10)
d <- density(logcounts)
plot(d,xlim=c(1,8),main="",ylim=c(0,.45),xlab="Raw read counts per gene (log10)", ylab="Density")
for (s in 2:length(samples)){
  logcounts <- log(counts$counts[,s],10)
  d <- density(logcounts)
  lines(d)
}
dev.off()

## box plots of raw read counts (log10)
png(file=paste(opt$bamfiles,"/",output,"_Raw_read_counts_per_gene.boxplot.png", sep=""))
logcounts <- log(counts$counts,10)
boxplot(logcounts, main="", xlab="", ylab="Raw read counts per gene (log10)",axes=FALSE)
axis(2)
axis(1,at=c(1:length(samples)),labels=colnames(logcounts),las=2,cex.axis=0.8)
dev.off()

#' In order to investigate the relationship between samples, hierarchical clustering is performed using the heatmap function from the stats package. In this example heatmap calculates a matrix of euclidean distances from the mapped read counts for the 100 most highly expressed genes.

#' selecting 100 most highly expressed genes
select = order(rowMeans(counts$counts), decreasing=TRUE)[1:100]
highexprgenes_counts <- counts$counts[select,]

#' heatmap with condition group as labels
colnames(highexprgenes_counts)<- group

#'Furthermore to understand the potential biological effect under the clustering, the data frame is re-ordered to similarity of 100 highly expressed profiles and grouped by sample names.
#'plot
png(file=paste(opt$bamfiles,"/",output,"_High_exprs_genes.heatmap.group.png", sep=""))
heatmap(highexprgenes_counts, col = topo.colors(50), margin=c(10,6))
dev.off()

#system(sprintf("multiqc %s", files_location))
system(paste("multiqc", files_location, sep=""))
