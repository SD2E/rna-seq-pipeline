#!/bin/bash


TL=-XX:GCTimeLimit
HFL=-XX:GCHeapFreeLimit
CL=-Dsamjdk.compression_level
JAVAOPTS1="${CL}=1 ${TL}=50 ${HFL}=10 -Xmx4000m"

fasta=$1
gff=$2
#force-save the fasta
#sed -i "s/${fasta}//g" .agave.log

ref_name=$(basename "${fasta%.fa}")
#mkdir ${ref_name}
#mv $fasta ${ref_name}/${ref_name}.fa
if [ -z "${gff}" ]
then :
else
  mv $gff ${ref_name}.gff
fi
#cd ${ref_name}
#ln -s $fasta ${ref_name}.fa
bwa index -a bwtsw ${fasta}
samtools faidx ${fasta}
# Create bed file from fasta index
awk 'BEGIN {FS="\t"}; {print $1 FS "0" FS $2}' ${ref_name}.fa.fai > ${ref_name}.bed

java ${JAVAOPTS1} -jar ${PICARDDIR} CreateSequenceDictionary \
    REFERENCE=${ref_name}.fa \
    OUTPUT=${ref_name}.dict

java ${JAVAOPTS1} -jar ${PICARDDIR} BedToIntervalList \
    SD=${ref_name}.dict \
    I=${ref_name}.bed \
    O=${ref_name}.interval_list
