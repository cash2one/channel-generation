#!/bin/bash

PYTHON_PATH='/home/video/guming_online_service/tools/Python-2.7.9/python'
export PATH=$PYTHON_PATH:$PATH
works_types_array=('movie' 'tv' 'comic' 'show')

failed=0
for works_type in "${works_types_array[@]}"
do
    python tag_content_gen.py $works_type
    if [$? -ne 0]; then
        failed=1
    fi
done

exit $failed

