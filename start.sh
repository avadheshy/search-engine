#!/bin/bash
pushd /home/ubuntu/search-engine
source /home/ubuntu/search/bin/activate
exec uvicorn app:app --reload
popd\
