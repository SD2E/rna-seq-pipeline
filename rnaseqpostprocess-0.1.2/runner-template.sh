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

echo "bam directory is ${path_bam_dir}"
echo "gff is ${path_gff}"

echo DEBUG=1 container_exec ${CONTAINER_IMAGE} Rscript --vanilla /opt/src/count_features.R --bamfilespath ${path_bam_dir} --annotation ${path_gff}
DEBUG=1 container_exec ${CONTAINER_IMAGE} Rscript --vanilla /opt/src/count_features.R --bamfilespath ${path_bam_dir} --annotation ${path_gff}
