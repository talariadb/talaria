#!/bin/bash -e

if [[ -f ${TALARIA_RC} ]]; then
echo "Loading env from ${TALARIA_RC}"
source ${TALARIA_RC} 
fi

echo "Starting talaria with TALARIA_URI = ${TALARIA_URI}"
