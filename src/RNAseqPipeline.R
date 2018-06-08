#! /usr/bin/env Rscript
############## Library dependencies for RNAseq pipeline ###################
suppressPackageStartupMessages(require(optparse))
library("knitr")
library("BiocParallel")
library("optparse")
library("Rsubread")
library("scatterplot3d")
library("genomeIntervals")
library("Rsamtools")
library("parallel")
library("limma")   # Linear models for differential expression
library("Glimma")  # Interactive plots for exploration
library("edgeR")   # Process count data from NGS experiments
library("gplots")
library("RColorBrewer")
library("R.utils")
library("dplyr")
library("DESeq2")
library("reshape2")
library("pathview")
library("gage")
library("gageData")

###########################################################################
#' RNASeqPipeline allows you to standardize the processing of your RNASeq data.
#' # ---
# title: "Raw Read Counts for RNAseq data - Paired or Unpaired sequences"
# author: "usaxena"
# date: paste(Sys.Date(), "_and_",Sys.time(),sep="")
# ---

# If directory exists
dir.exists<-function (x)
{
  res <- file.exists(x) & file.info(x)$isdir
  setNames(res, x)
}

