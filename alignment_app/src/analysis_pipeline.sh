#!/bin/bash
start=$(date +%s.%N)
# system config variables
NSLOTS=2
UCS=-XX:+UseCompressedStrings
USC=-XX:+UseStringCache
TL=-XX:GCTimeLimit
HFL=-XX:GCHeapFreeLimit
CL=-Dsamjdk.compression_level
ASIO=-Dsamjdk.use_async_io
PH=--phone_home

# Sequences
forward=$1
echo ${forward}
reverse=$2
echo ${reverse}
SAMP=$3
echo ${SAMP}

# Directories

#OUTDIR=$5
#OUTDIR="/scratch/05642/usaxena/YONG/BWA_samfiles"
OUTDIR='.'

#TEMPDIR="/scratch/05642/usaxena/YONG/BWA_samfiles"
TEMPDIR='.'

#These are now defined in the Dockerfile, but leaving here for reference
#GATK=/src/GenomeAnalysisTK.jar
#featureCounts=/opt/subread-1.6.2-Linux-x86_64/bin/featureCounts
#RNAseqQC=/opt/RNA-SeQC_v1.1.8.jar
#PICARDDIR=/opt/picard/picard.jar

#refFlat=/work/05642/usaxena/stampede2/NAND/annotation/modified.ecoli.MG1655.refFlat.txt

# reference files

REF=$4
echo $REF
TARGETS=$5
echo $TARGETS
GTF=$6
echo $GTF
refFlat=$7
echo $refFlat

BWAARGS="-q 5 -l 32 -k 2 -t ${NSLOTS} -o 1"
JAVAOPTS1="${CL}=1 ${TL}=50 ${HFL}=10 -Xmx4000m"
PICARDOPTS="TMP_DIR=${TEMPDIR} VALIDATION_STRINGENCY=LENIENT CREATE_INDEX=TRUE"
GATKOPTS="TMP_DIR=${TEMPDIR} VALIDATION_STRINGENCY=LENIENT CREATE_INDEX=TRUE"

#TEMPDIR=$5
#TEMPDIR="/scratch/05642/usaxena/37RUN"

echo ${forward}
echo ${reverse}



## BWA alignment
echo "This is BWA analysis processing"

outsample=${SAMP}.rnaseq.original.bwa
echo ${SAMP}.BWA.align | tee -a /dev/stderr
time bwa mem -t 40 ${REF} ${forward} ${reverse} > ${TEMPDIR}/${outsample}.sam
#The pipe tee /dev/stderr directs the echos to stderr in addition to stdout for debugging
echo ${SAMP}.BWA.sort "& done(${SAMP}.BWA.align))" | tee -a /dev/stderr


## Picard processing
time java ${JAVAOPTS1} -jar ${PICARDDIR} \
      SortSam SORT_ORDER=coordinate \
      INPUT=${TEMPDIR}/${outsample}.sam \
      OUTPUT=${TEMPDIR}/${outsample}.sorted.sam \
      TMP_DIR=${TEMPDIR} VALIDATION_STRINGENCY=LENIENT
echo ${SAMP}.BWA.addRG "& done(${SAMP}.BWA.sorted)" | tee -a /dev/stderr

time java ${JAVAOPTS1} -jar ${PICARDDIR} \
      AddOrReplaceReadGroups \
      ${PICARDOPTS} \
      RGLB=${outsample} \
      RGPL=Illumina \
      RGPU=${outsample} \
      RGSM=${outsample} \
      INPUT=${TEMPDIR}/${outsample}.sorted.sam \
      OUTPUT=${TEMPDIR}/${outsample}.RG.bam
echo ${SAMP}.BWA.mark_duplicates "& done(${SAMP}.BWA.addRG)" | tee -a /dev/stderr

time java ${JAVAOPTS1} -jar ${PICARDDIR} \
      MarkDuplicates \
      TMP_DIR=${TEMPDIR} \
      VALIDATION_STRINGENCY=LENIENT \
      CREATE_INDEX=TRUE REMOVE_DUPLICATES=FALSE \
      TAG_DUPLICATE_SET_MEMBERS=TRUE \
      CREATE_MD5_FILE=TRUE \
      INPUT=${TEMPDIR}/${outsample}.RG.bam \
      OUTPUT=${TEMPDIR}/${outsample}.aligned.duplicates_marked.bam \
      METRICS_FILE=${TEMPDIR}/${outsample}.duplicate_metrics.txt
echo ${SAMP}.BWA.indel_realigner "& done(${SAMP}.BWA.mark_duplicates)" | tee -a /dev/stderr

time java -Djava.io.tmpdir=${TEMPDIR} ${ASIO}=true ${TL}=50 ${HFL}=10 -Xmx5000m -jar ${GATK} \
      -T IndelRealigner -U -R ${REF} \
      -o ${TEMPDIR}/${outsample}.aligned.duplicates_marked.indel_cleaned.bam \
      -I ${TEMPDIR}/${outsample}.aligned.duplicates_marked.bam \
      -compress 1 \
      -targetIntervals ${TARGETS}
#echo ${SAMP}.BWA.base_recal "& done(${SAMP}.BWA.indel_realigner)"

#java ${TL}=50 ${HFL}=10 -Xmx4000m -jar ${GATK} -T BaseRecalibrator -R ${REF} -I ${TEMPDIR}/${outsample}.aligned.duplicates_marked.indel_cleaned.bam -o ${TEMPDIR}/${outsample}.baserecal_data.table
#echo ${SAMP}.BWA.analysecov "& done(${SAMP}.BWA.base_recal)"
#java ${ASIO}=true ${TL}=50 ${HFL}=10 -Xmx4000m -jar ${GATK} -T AnalyzeCovariates -R ${REF} -plots ${BASEDIR}/${outsample}.BQSR.pdf -BQSR ${TEMPDIR}/${outsample}.baserecal_data.table -csv ${BASEDIR}/${outsample}.BQSR.csv
#echo ${SAMP}.BWA.hs_metrics "& done(${SAMP}.BWA.analysecov)"

echo ${SAMP}.BWA.hs_metrics "& done(${SAMP}.BWA.indel_realigner)" | tee -a /dev/stderr
time java ${TL}=50 ${HFL}=10 -Xmx1500m -jar ${PICARDDIR} \
      CollectHsMetrics \
      TMP_DIR=${TEMPDIR} \
      VALIDATION_STRINGENCY=LENIENT \
      I=${TEMPDIR}/${outsample}.aligned.duplicates_marked.indel_cleaned.bam \
      O=${TEMPDIR}/${outsample}.hybrid_selection_metrics.txt \
      REFERENCE_SEQUENCE=${REF} \
      BAIT_INTERVALS=${TARGETS} \
      BAIT_SET_NAME=rnaseq_genome \
      TARGET_INTERVALS=${TARGETS}
echo ${SAMP}.Bedtools.genomecov "& done(${SAMP}.BWA.hs_metrics)" | tee -a /dev/stderr
#echo ${SAMP}.BWA.DepthOfCoverage "& done(${SAMP}.BWA.hs_metrics)" | tee -a /dev/stderr


# time java ${TL}=50 ${HFL}=10 -Xmx4000m -jar ${GATK} \
#       -T DepthOfCoverage \
#       -L ${TARGETS} \
#       -R ${REF} \
#       -I ${TEMPDIR}/${outsample}.aligned.duplicates_marked.indel_cleaned.bam \
#       -o ${TEMPDIR}/${outsample}.DepthOfCoverage.tsv
# echo ${SAMP}.BWA.FlagStat "& done(${SAMP}.BWA.DepthOfCoverage)" | tee -a /dev/stderr

time bedtools genomecov -ibam \
      ${TEMPDIR}/${outsample}.aligned.duplicates_marked.indel_cleaned.bam \
      -bg > ${TEMPDIR}/${outsample}_genome_coverage.bedgraph
echo ${SAMP}.BWA.FlagStat "& done(${SAMP}.Bedtools.genomecov)" | tee -a /dev/stderr

time java ${TL}=50 ${HFL}=10 -Xmx4000m -jar ${GATK} \
      -T FlagStat \
      -R ${REF} \
      -I ${TEMPDIR}/${outsample}.aligned.duplicates_marked.indel_cleaned.bam \
      -o ${TEMPDIR}/${outsample}.flagstat.txt
echo ${SAMP}.BWA.validate "& done(${SAMP}.BWA.FlagStat)" | tee -a /dev/stderr

time java ${TL}=50 ${HFL}=10 -Xmx4000m -jar ${PICARDDIR} \
      ValidateSamFile \
      ${PICARDOPTS} \
      CREATE_MD5_FILE=false \
      INPUT=${TEMPDIR}/${outsample}.aligned.duplicates_marked.indel_cleaned.bam \
      OUTPUT=/${TEMPDIR}/${outsample}.validation_metrics \
      REFERENCE_SEQUENCE=${REF} \
      MODE=SUMMARY \
      IS_BISULFITE_SEQUENCED=false
echo ${SAMP}.BWA.multiple_metrics "& done(${SAMP}.BWA.validate)" | tee -a /dev/stderr

time java ${TL}=50 ${HFL}=10 -Xmx4000m -jar ${PICARDDIR} \
      CollectMultipleMetrics \
      VALIDATION_STRINGENCY=LENIENT \
      PROGRAM=null \
      PROGRAM=MeanQualityByCycle \
      PROGRAM=QualityScoreDistribution \
      PROGRAM=CollectAlignmentSummaryMetrics \
      PROGRAM=CollectInsertSizeMetrics \
      REFERENCE_SEQUENCE=${REF} \
      INPUT=${TEMPDIR}/${outsample}.aligned.duplicates_marked.indel_cleaned.bam \
      ASSUME_SORTED=true \
      FILE_EXTENSION=.txt \
      OUTPUT=${TEMPDIR}/${outsample}.collect_multiple_metrics
echo ${SAMP}.BWA.rnaseqmetrics "& done(${SAMP}.BWA.multiple_metrics)" | tee -a /dev/stderr

time java ${TL}=50 ${HFL}=10 -Xmx4000m -jar ${RNAseqQC} \
      -r ${REF} \
      -s ${TEMPDIR}/${outsample}.aligned.duplicates_marked.indel_cleaned.bam \
      -t ${GTF} \
      -o ${TEMPDIR}
echo ${SAMP}.BWA.RnaSeqMetrics "& done(${SAMP}.BWA.rnaseqmetrics)" | tee -a /dev/stderr

java ${TL}=50 ${HFL}=10 -Xmx4000m -jar ${PICARDDIR} \
      CollectRnaSeqMetrics \
      INPUT=${TEMPDIR}/${outsample}.aligned.duplicates_marked.indel_cleaned.bam \
      REFERENCE_SEQUENCE=${REF} \
      STRAND_SPECIFICITY=NONE \
      OUTPUT=${TEMPDIR}/${outsample}.collect_hs_metrics.txt \
      REF_FLAT=${refFlat}
echo ${SAMP}.BWA.featureCounts "& done(${SAMP}.BWA.hsmetrics)" | tee -a /dev/stderr

time ${featureCounts} -T 10 -p -P -M \
      -a ${GTF} \
      ${TEMPDIR}/${outsample}.RG.bam \
      -o ${OUTDIR}/${outsample}.featureCounts.txt \
      -t gene \
      -g gene
mv ${OUTDIR}/${outsample}.featureCounts.txt.summary ${OUTDIR}/${outsample}.featureCounts.summary.tsv
#Processing to get vcfS
#echo ${SAMP}.BWA.haplotyping "& done(${SAMP}.BWA.featureCounts)"
echo ${SAMP}.BWA.genotyping "& done(${SAMP}.BWA.featureCounts)" | tee -a /dev/stderr
#java ${TL}=50 ${HFL}=10 -Xmx4000m -jar ${GATK} -T HaplotypeCaller -R ${REF} \
#      -I ${TEMPDIR}/${outsample}.aligned.duplicates_marked.indel_cleaned.bam -L ${TARGETS} \
#      -o ${OUTDIR}/${outsample}.snps.indels.g.vcf -stand_call_conf 20
#echo ${SAMP}.BWA.RegenotypeVariants "& done(${SAMP}.BWA.haplotyping)"
#java ${TL}=50 ${HFL}=10 -Xmx4000m -jar ${GATK} -T RegenotypeVariants \
#      -R ${REF} --variant ${OUTDIR}/${outsample}.snps.indels.g.vcf \
#      -o ${OUTDIR}/${outsample}.regenotypeVariants.vcf
#echo ${SAMP}.BWA.genotyping "& done(${SAMP}.BWA.RegenotypeVariants)"

time java ${TL}=50 ${HFL}=10 -Xmx4000m -jar ${GATK} \
      -T UnifiedGenotyper \
      -R ${REF} \
      -I ${TEMPDIR}/${outsample}.aligned.duplicates_marked.indel_cleaned.bam \
      -o ${OUTDIR}/${outsample}.genotypes.vcf \
      --output_mode EMIT_ALL_SITES
echo ${SAMP}.BWA.libcomplexity "& done(${SAMP}.BWA.genotyping)" | tee -a /dev/stderr
#echo ${SAMP}.BWA.rrregno "& done(${SAMP}.BWA.genotyping)"

#java ${TL}=50 ${HFL}=10 -Xmx4000m -jar ${GATK} -T RegenotypeVariants \
#      -R ${REF} --variant ${OUTDIR}/${outsample}.genotypes.vcf \
#      -o ${OUTDIR}/${outsample}.rgenotypevariants.vcf

#Library complexity
#echo ${SAMP}.BWA.libcomplexity "& done(${SAMP}.BWA.rrregno)"
time java ${JAVAOPTS1} -jar ${PICARDDIR} \
      EstimateLibraryComplexity \
      INPUT=${TEMPDIR}/${outsample}.aligned.duplicates_marked.indel_cleaned.bam \
      OUTPUT=${TEMPDIR}/${outsample}.lib_complex_metrics.txt
#echo ${SAMP}.BWA.comparemetrics
echo "done(${SAMP}.BWA.libcomplexity)" | tee -a /dev/stderr

#time java ${JAVAOPTS1} -jar ${PICARDDIR} CompareMetrics \
#      ${TEMPDIR}/${outsample}.lib_complex_metrics.txt \
#      ${TEMPDIR}/${outsample}.collect_hs_metrics.txt \
#      ${TEMPDIR}/${outsample}.collect_multiple_metrics.txt \
#      ${TEMPDIR}/${outsample}.duplicate_metrics > ${TEMPDIR}/${outsample}.comparemetrics.txt
#echo "& done(${SAMP}.BWA.comparemetrics)" | tee -a /dev/stderr

end=$(date +%s.%N)
runtime=$(python -c "print(${end} - ${start})")
echo "Runtime was $runtime"
