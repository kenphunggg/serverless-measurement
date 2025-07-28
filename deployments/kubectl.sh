#!/bin/sh
cp /etc/kubernetes/admin.conf $HOME/
chown $(id -u):$(id -g) $HOME/admin.conf
export KUBECONFIG=$HOME/admin.conf

if [ "$1" = "deploy" ]
then
status=`kubectl apply -f $2`
elif [ "$1" = "delete" ]
then
status=`kubectl delete -f $2`
else
echo "Error deployment"
fi
echo $status
