#!/usr/bin/env bash

. _util/container_exec.sh

version=$(cat VERSION)
CONTAINER_IMAGE="jurrutia/rnaseq:$version"

# resets inputs to paths if inputs are null
if [ -z ${read1} ]; then
  echo "no input specified for read1, attempting to pull read1 from path parameter: path_read1"
  echo read1=${path_read1}
  r1=${path_read1}
else
  r1=${read1}
fi

if [ -z ${read2} ]; then
  echo "no input specified for read2, attempting to use path parameter: path_read2"
  echo read2=${path_read2}
  r2=${path_read2}
else
  r2=${read2}
fi

if [ -z ${filterseqs} ]; then
  echo "no input specified for filterseqs, attempting to use path parameter: path_filterseqs"
  echo filterseqs=${path_filterseqs}
  fseqs=${path_filterseqs}
else
  fseqs=${filterseqs}
fi

if [[ ${r1} =~ \.gz$ ]]; then
   echo READ1 is gzipped, upzipping
   echo zcat ${r1} > ${r1##*/}
   zcat ${r1} > ${r1##*/}
   r1=${r1##*/}
   mv ${r1} ${r1%.gz}
   r1=${r1%.gz}
else
   echo READ1 is not gzipped
fi
if [[ ${r2} =~ \.gz$ ]]; then
   echo READ2 is gzipped, upzipping
   zcat ${r2} > ${r2##*/}
   r2=${r2##*/}
   mv ${r2} ${r2%.gz}
   r2=${r2%.gz}
else
   echo READ2 is not gzipped
fi

echo the input parameters
echo read1 is ${r1}
echo read2 is ${r2}
echo trim is ${trim}
echo sortmerna is ${sortmerna}
echo minlen is ${minlen}
echo adaptersfile is ${adaptersfile}
echo filterseqs is ${fseqs}

echo DEBUG=1 container_exec ${CONTAINER_IMAGE} /opt/scripts/runsortmerna.sh ${r1} ${r2} ${trim} ${adaptersfile} ${minlen} ${sortmerna} ${fseqs}

DEBUG=1 container_exec ${CONTAINER_IMAGE} /opt/scripts/runsortmerna.sh ${r1} ${r2} ${trim} ${adaptersfile} ${minlen} ${sortmerna} ${fseqs}

rm $(basename $r1)
rm $(basename $r2)
