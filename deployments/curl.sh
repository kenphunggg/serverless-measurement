#!/bin/sh

# hey -z 30s -c 50
for i in 1 2 3 4 5
do
    curl http://detection.default.svc.cluster.local/test_video/highway.mp4  &
    echo "Finish request"
done

# for i in 1 2 3 4 5 6
# do
#     curl http://hello.default.svc.cluster.local/  &
#     echo "Finish request"
# done