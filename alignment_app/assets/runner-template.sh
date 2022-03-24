# Import Agave runtime extensions
. _lib/extend-runtime.sh

# Allow CONTAINER_IMAGE over-ride via local file
if [ -z "${CONTAINER_IMAGE}" ]
then
    if [ -f "./_lib/CONTAINER_IMAGE" ]; then
        CONTAINER_IMAGE=$(cat ./_lib/CONTAINER_IMAGE)
    fi
    if [ -z "${CONTAINER_IMAGE}" ]; then
        echo "CONTAINER_IMAGE was not set via the app or CONTAINER_IMAGE file"
        CONTAINER_IMAGE="sd2e/base:ubuntu17"
    fi
fi

# Usage: container_exec IMAGE COMMAND OPTIONS
#   Example: docker run centos:7 uname -a
#            container_exec centos:7 uname -a


read1=${path_read1}
read2=${path_read2}
fasta=${path_fasta}
interval_file=${path_interval_file}
gff=${path_gff}
ref_flat=${path_ref_flat}

echo the input paramters
echo read1 is ${read1}
echo read2 is ${read2}
echo outname is ${outname}
echo fasta is ${fasta}
echo gff is ${gff}
echo interval_file is ${interval_file}
echo ref_flat is ${ref_flat}


echo container_exec ${CONTAINER_IMAGE} /src/analysis_pipeline.sh ${read1} ${read2} ${outname} ${fasta} ${interval_file} ${gff} ${ref_flat}
container_exec ${CONTAINER_IMAGE} /src/analysis_pipeline.sh ${read1} ${read2} ${outname} ${fasta} ${interval_file} ${gff} ${ref_flat}
