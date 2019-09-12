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

echo container_exec ${CONTAINER_IMAGE} python3 /src/test.py ${fs_remote_fp}
container_exec ${CONTAINER_IMAGE} python3 /src/test.py ${fs_remote_fp}

if [ -f ./status.txt ]; then
    s=$(head -1 ./status.txt)
    echo $s
    if [ $s == "pass" ]; then
        touch ./pooR1poo.fastq.gz
    else
        touch ./failfile.txt
    fi
fi

echo Directory contents are
ls
