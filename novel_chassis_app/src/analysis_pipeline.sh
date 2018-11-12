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
out=$3
echo ${out}
samplename=$4
echo ${samplename}

# Directories

#OUTDIR=$5
OUTDIR="/scratch/05642/usaxena/YONG/BWA_samfiles"
echo $OUTDIR
BASEDIR=$5
echo $BASEDIR

RNAseqQC=/work/05642/usaxena/stampede2/RNAseq/RNA-SeQC_v1.1.8.jar

#executables
module load picard/2.11.0
module load bowtie/2.3.2
module load bwa/0.7.16a
module load fastqc/0.11.5
module load samtools/1.5
module load Rstats/3.4.0
module load gcc/7.1.0
module load python3/3.6.1

TEMPDIR="/scratch/05642/usaxena/YONG/BWA_samfiles"
GATK=/opt/gatk/gatk.jar
featureCounts=/work/05642/usaxena/stampede2/tools/subread-1.6.2-Linux-x86_64/bin/featureCounts
PICARDDIR=$TACC_PICARD_DIR/build/libs/picard.jar
REFFLAT=/work/05642/usaxena/stampede2/NAND/annotation/modifiedWIS_MG1655_v3_annotations.txt
refFlat=/work/05642/usaxena/stampede2/NAND/annotation/modified.ecoli.MG1655.refFlat.txt

# reference files
BOWTIE2REF=$6
echo $BOWTIE2REF
REF=$7
echo $REF
TARGETS=$8
echo $TARGETS
BAITS=$8
echo $BAITS
GTF=$9
echo $GTF
# shortcuts
ALIGNEDNAME=rnaseq.original.bowtie2
BWAALIGNEDNAME=rnaseq.original.bwa

BWAARGS="-q 5 -l 32 -k 2 -t ${NSLOTS} -o 1"
JAVAOPTS1="${CL}=1 ${TL}=50 ${HFL}=10 -Xmx4000m"
PICARDOPTS="TMP_DIR=${TEMPDIR} VALIDATION_STRINGENCY=LENIENT CREATE_INDEX=TRUE"
GATKOPTS="TMP_DIR=${TEMPDIR} VALIDATION_STRINGENCY=LENIENT CREATE_INDEX=TRUE"

#TEMPDIR=$5
#TEMPDIR="/scratch/05642/usaxena/37RUN"

SAMP=${samplename}
echo=${SAMP}
echo ${forward}
echo ${reverse}
outsample=${out}.${ALIGNEDNAME}
echo ${outsample}


## BWA alignment
echo "This is BWA analysis processing"

boutsample=${out}.${BWAALIGNEDNAME}
echo ${SAMP}.BWA.align "& done(${SAMP}.BWA.fastqc.reverse)"
bwa mem -t 80 ${REF} ${forward} ${reverse} > ${TEMPDIR}/${boutsample}.sam
echo ${SAMP}.BWA.sort "& done(${SAMP}.comparemetrics))"


## Picard processing
java ${JAVAOPTS1} -jar ${PICARDDIR} SortSam SORT_ORDER=coordinate INPUT=${TEMPDIR}/${boutsample}.sam \
      OUTPUT=${TEMPDIR}/${boutsample}.sorted.sam TMP_DIR=${TEMPDIR} VALIDATION_STRINGENCY=LENIENT
echo ${SAMP}.BWA.addRG "& done(${SAMP}.BWA.sorted)"

java ${JAVAOPTS1} -jar ${PICARDDIR} AddOrReplaceReadGroups ${PICARDOPTS} RGLB=${boutsample} RGPL=Illumina \
      RGPU=${boutsample} RGSM=${boutsample} INPUT=${TEMPDIR}/${boutsample}.sorted.sam OUTPUT=${TEMPDIR}/${boutsample}.RG.bam
echo ${SAMP}.BWA.mark_duplicates "& done(${SAMP}.BWA.addRG)"

java ${JAVAOPTS1} -jar ${PICARDDIR} MarkDuplicates TMP_DIR=${TEMPDIR} VALIDATION_STRINGENCY=LENIENT CREATE_INDEX=TRUE \
      REMOVE_DUPLICATES=FALSE TAG_DUPLICATE_SET_MEMBERS=TRUE CREATE_MD5_FILE=TRUE INPUT=${TEMPDIR}/${boutsample}.RG.bam \
      OUTPUT=${TEMPDIR}/${boutsample}.aligned.duplicates_marked.bam METRICS_FILE=${TEMPDIR}/${boutsample}.duplicate_metrics
echo ${SAMP}.BWA.indel_realigner "& done(${SAMP}.BWA.mark_duplicates)"

java -Djava.io.tmpdir=${TEMPDIR} ${ASIO}=true ${TL}=50 ${HFL}=10 -Xmx5000m -jar ${GATK} -T IndelRealigner -U -R ${REF}
      -o ${TEMPDIR}/${boutsample}.aligned.duplicates_marked.indel_cleaned.bam -I ${TEMPDIR}/${boutsample}.aligned.duplicates_marked.bam \
      -compress 1 -targetIntervals ${TARGETS}
echo ${SAMP}.BWA.base_recal "& done(${SAMP}.BWA.indel_realigner)"

#java ${TL}=50 ${HFL}=10 -Xmx4000m -jar ${GATK} -T BaseRecalibrator -R ${REF} -I ${TEMPDIR}/${boutsample}.aligned.duplicates_marked.indel_cleaned.bam -o ${TEMPDIR}/${boutsample}.baserecal_data.table
echo ${SAMP}.BWA.analysecov "& done(${SAMP}.BWA.base_recal)"
#java ${ASIO}=true ${TL}=50 ${HFL}=10 -Xmx4000m -jar ${GATK} -T AnalyzeCovariates -R ${REF} -plots ${BASEDIR}/${boutsample}.BQSR.pdf -BQSR ${TEMPDIR}/${boutsample}.baserecal_data.table -csv ${BASEDIR}/${boutsample}.BQSR.csv
echo ${SAMP}.BWA.hs_metrics "& done(${SAMP}.BWA.analysecov)"

java ${TL}=50 ${HFL}=10 -Xmx1500m -jar ${PICARDDIR} CollectHsMetrics TMP_DIR=${TEMPDIR} VALIDATION_STRINGENCY=LENIENT \
      I=${TEMPDIR}/${boutsample}.aligned.duplicates_marked.indel_cleaned.bam O=${TEMPDIR}/${boutsample}.hybrid_selection_metrics \
      REFERENCE_SEQUENCE=${REF} BAIT_INTERVALS=${TARGETS} BAIT_SET_NAME=rnaseq_genome TARGET_INTERVALS=${TARGETS}
echo ${SAMP}.BWA.DepthOfCoverage "& done(${SAMP}.BWA.hs_metrics)"

java ${TL}=50 ${HFL}=10 -Xmx4000m -jar ${GATK} -nt 20 -T DepthOfCoverage -L ${TARGETS} -R ${REF} \
      -I ${TEMPDIR}/${boutsample}.aligned.duplicates_marked.indel_cleaned.bam -O ${TEMPDIR}/${boutsample}.DepthOfCoverage
echo ${SAMP}.BWA.FlagStat "& done(${SAMP}.BWA.hs_metrics)"

java ${TL}=50 ${HFL}=10 -Xmx4000m -jar ${GATK} -nt 20 -T FlagStat -R ${REF} -I ${TEMPDIR}/${boutsample}.aligned.duplicates_marked.indel_cleaned.bam \
      -O ${TEMPDIR}/${boutsample}.flagstat.txt
echo ${SAMP}.BWA.validate "& done(${SAMP}.BWA.FlagStat)"

java ${TL}=50 ${HFL}=10 -Xmx4000m -jar ${PICARDDIR} ValidateSamFile ${PICARDOPTS} CREATE_MD5_FILE=false \
      INPUT=${TEMPDIR}/${boutsample}.aligned.duplicates_marked.indel_cleaned.bam OUTPUT=/${TEMPDIR}/${boutsample}.validation_metrics \
      REFERENCE_SEQUENCE=${REF} MODE=SUMMARY IS_BISULFITE_SEQUENCED=false
echo ${SAMP}.BWA.multiple_metrics "& done(${SAMP}.BWA.validate)"

java ${TL}=50 ${HFL}=10 -Xmx4000m -jar ${PICARDDIR} CollectMultipleMetrics VALIDATION_STRINGENCY=LENIENT PROGRAM=null \
      PROGRAM=MeanQualityByCycle PROGRAM=QualityScoreDistribution PROGRAM=CollectAlignmentSummaryMetrics PROGRAM=CollectInsertSizeMetrics \
      INPUT=${TEMPDIR}/${boutsample}.aligned.duplicates_marked.indel_cleaned.bam REFERENCE_SEQUENCE=${REF} \
      ASSUME_SORTED=true OUTPUT=${TEMPDIR}/${boutsample}.collect_multiple_metrics.txt
echo ${SAMP}.BWA.rnaseqmetrics "& done(${SAMP}.BWA.multiple_metrics)"

java ${TL}=50 ${HFL}=10 -Xmx4000m -jar ${RNAseqQC} -r ${REF} -s ${TEMPDIR}/${boutsample}.aligned.duplicates_marked.indel_cleaned.bam \
      -t ${GTF} -o ${TEMPDIR}
echo ${SAMP}.BWA.RnaSeqMetrics "& done(${SAMP}.BWA.rnaseqmetrics)"

java ${TL}=50 ${HFL}=10 -Xmx4000m -jar ${PICARDDIR} CollectRnaSeqMetrics \
      INPUT=${TEMPDIR}/${boutsample}.aligned.duplicates_marked.indel_cleaned.bam REFERENCE_SEQUENCE=${REF} \
      OUTPUT=${TEMPDIR}/${boutsample}.collect_hs_metrics.txt REF_FLAT=${refFlat}
echo ${SAMP}.BWA.featureCounts "& done(${SAMP}.BWA.hsmetrics)"

${featureCounts} -T 10 -p -P -M -a ${GTF} input_file1 ${TEMPDIR}/${boutsample}.aligned.duplicates_marked.indel_cleaned.bam \
      -o ${OUTDIR}/${boutsample}.featureCounts

#Processing to get vcfS
echo ${SAMP}.BWA.haplotyping "& done(${SAMP}.BWA.featureCounts)"
java ${TL}=50 ${HFL}=10 -Xmx4000m -jar ${GATK} -nt 20 -T HaplotypeCaller -R ${REF} \
      -I ${TEMPDIR}/${boutsample}.aligned.duplicates_marked.indel_cleaned.bam -L ${TARGETS} \
      -o ${OUTDIR}/${boutsample}.snps.indels.g.vcf -stand_call_conf 20
echo ${SAMP}.BWA.RegenotypeVariants "& done(${SAMP}.BWA.haplotyping)"
java ${TL}=50 ${HFL}=10 -Xmx4000m -jar ${GATK} -nt 20 -T RegenotypeVariants \
      -R ${REF} --variant ${OUTDIR}/${boutsample}.snps.indels.g.vcf \
      -o ${OUTDIR}/${boutsample}.regenotypeVariants.vcf
echo ${SAMP}.BWA.genotyping "& done(${SAMP}.BWA.RegenotypeVariants)"

java ${TL}=50 ${HFL}=10 -Xmx4000m -jar ${GATK} -nt 20 -T UnifiedGenotyper \
      -R ${REF} -I ${TEMPDIR}/${boutsample}.aligned.duplicates_marked.indel_cleaned.bam \
      -o ${OUTDIR}/${boutsample}.genotypes.vcf --output_mode EMIT_ALL_SITES
echo ${SAMP}.BWA.rrregno "& done(${SAMP}.BWA.genotyping)"

java ${TL}=50 ${HFL}=10 -Xmx4000m -jar ${GATK} -nt 20 -T RegenotypeVariants \
      -R ${REF} --variant ${OUTDIR}/${boutsample}.genotypes.vcf \
      -o ${OUTDIR}/${boutsample}.rgenotypevariants.vcf

#Library complexity
echo ${SAMP}.BWA.libcomplexity "& done(${SAMP}.BWA.rrregno)"
java ${JAVAOPTS1} -jar ${PICARDDIR} EstimateLibraryComplexity \
      INPUT=${TEMPDIR}/${boutsample}.aligned.duplicates_marked.indel_cleaned.bam \
      OUTPUT=${TEMPDIR}/${boutsample}.lib_complex_metrics.txt
echo ${SAMP}.BWA.comparemetrics "& done(${SAMP}.BWA.libcomplexity)"

java ${JAVAOPTS1} -jar ${PICARDDIR} CompareMetrics \
      ${TEMPDIR}/${boutsample}.lib_complex_metrics.txt \
      ${TEMPDIR}/${boutsample}.collect_hs_metrics.txt \
      ${TEMPDIR}/${boutsample}.collect_multiple_metrics.txt \
      ${TEMPDIR}/${boutsample}.duplicate_metrics > ${TEMPDIR}/${boutsample}.comparemetrics.txt
echo "& done(${SAMP}.BWA.comparemetrics)"

end=$(date +%s.%N)
runtime=$(python -c "print(${end} - ${start})")
echo "Runtime was $runtime"
