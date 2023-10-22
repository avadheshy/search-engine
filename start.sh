#!/bin/bash
pushd /home/ubuntu/search-engine
source /home/ubuntu/search/bin/activate
exec gunicorn -w 2 -k uvicorn.workers.UvicornWorker app:app
popd\
