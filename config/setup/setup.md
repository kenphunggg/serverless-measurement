# serverless-measurement-doc
Documentation for Serverless Setup and Measurement

## Table of contents

- [Testbed design](#testbed-design)
- [Setting up a product ready Kubernetes cluster](#setting-up-a-product-ready-kubernetes-cluster)
- [Setting up a serverless cluster](#setting-up-a-serverless-cluster)
  - [Prerequisite](#prerequisite)
  - [Installing Knative Serving using YAML files](#installing-knative-serving-using-yaml-files)
    - [Install the Knative Serving component](#1-install-the-knative-serving-component)
    - [Install a networking layer](#2-install-a-networking-layer)
    - [Verify the installation](#3-verify-the-installation)
    - [Configure DNS](#4-configure-dns)
  - [Advance configuration](#advance-configuration)
    - [Kourier Gateway](#kourier-gateway)
    - [Activator](#activator)
    - [Check your configuration](#check-your-configuration)
- [Network configuration](#network-configuration)
- [Setting up Prometheus for measuring](#setting-up-prometheus-for-measuring)
  - [Install Prometheus](#install-prometheus)
  - [Running Prometheus](#running-prometheus)

## Testbed design

![testbed design](./images/testbed_des.png)

In this testbed, we build up three nodes which is `master-node`, and two worker nodes (`cloud-node` and `edge-node`). `mater-node` will be the `control-plane` for `Kubernetes` cluster. It will generate requests and the measurement will be run right there. We will emulate latency and bandwidth between `master-node` and two other worker nodes. Functions will be served in two worker nodes and we will run `Prometheus` server in these nodes for measurement.

## Setting up a product ready Kubernetes cluster
You can follow [this guild](https://github.com/kenphunggg/kubespray.git) to build your own K8s cluster using `Kubespray`

## Setting up a serverless cluster
In this testbed, we're using [Knative](https://knative.dev/docs/) for building a `Serverless cluster`

### Prerequisite
It is recommended to using [Metallb](https://metallb.io/) to expose a IPV4 address for cluster's `Loadbalancer` Service

If you’re using kube-proxy in IPVS mode, since Kubernetes v1.14.2 you have to enable strict ARP mode.

You can achieve this by editing kube-proxy config in current cluster:

```shell
# see what changes would be made, returns nonzero returncode if different
kubectl get configmap kube-proxy -n kube-system -o yaml | \
sed -e "s/strictARP: false/strictARP: true/" | \
kubectl diff -f - -n kube-system

# actually apply the changes, returns nonzero returncode on errors only
kubectl get configmap kube-proxy -n kube-system -o yaml | \
sed -e "s/strictARP: false/strictARP: true/" | \
kubectl apply -f - -n kube-system
```

Installation by manifest

```shell
kubectl apply -f https://raw.githubusercontent.com/metallb/metallb/v0.15.2/config/manifests/metallb-native.yaml
```

In order to assign an IP to the services, MetalLB must be instructed to do so via the `IPAddressPool` CR.

All the IPs allocated via `IPAddressPools` contribute to the pool of IPs that MetalLB uses to assign IPs to services.

```yaml
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: first-pool
  namespace: metallb-system
spec:
  addresses:
  - <your_ipAddresspool>  # Example: 192.168.17.1-192.168.17.250
```

### Installing Knative Serving using YAML files

#### 1. Install the Knative Serving component

To install the Knative Serving component:

```shell
# Install the required custom resources by running the command
kubectl apply -f https://github.com/knative/serving/releases/download/knative-v1.18.1/serving-crds.yaml

# Install the core components of Knative Serving by running the command
kubectl apply -f https://github.com/knative/serving/releases/download/knative-v1.18.1/serving-core.yaml
```

#### 2. Install a networking layer

```shell
# Install the Knative Kourier controller by running the command
kubectl apply -f https://github.com/knative/net-kourier/releases/download/knative-v1.18.0/kourier.yaml

# Configure Knative Serving to use Kourier by default by running the command:
kubectl patch configmap/config-network \
  --namespace knative-serving \
  --type merge \
  --patch '{"data":{"ingress-class":"kourier.ingress.networking.knative.dev"}}'
```

Fetch the External IP address or CNAME by running the command:

```shell
kubectl --namespace kourier-system get service kourier
```

#### 3. Verify the installation

Monitor the Knative components until all of the components show a STATUS of Running or Completed. You can do this by running the following command and inspecting the output:

```shell
kubectl get pods -n knative-serving
```

Example output:

```shell
NAME                                      READY   STATUS    RESTARTS   AGE
3scale-kourier-control-54cc54cc58-mmdgq   1/1     Running   0          81s
activator-67656dcbbb-8mftq                1/1     Running   0          97s
autoscaler-df6856b64-5h4lc                1/1     Running   0          97s
controller-788796f49d-4x6pm               1/1     Running   0          97s
webhook-859796bc7-8n5g2                   1/1     Running   0          96s
```

#### 4. Configure DNS

You can configure DNS to prevent the need to run curl commands with a host header.

Knative provides a Kubernetes Job called default-domain that configures Knative Serving to use sslip.io as the default DNS suffix.

```shell
kubectl apply -f https://github.com/knative/serving/releases/download/knative-v1.18.1/serving-default-domain.yaml
```

### Advance configuration

#### Kourier Gateway

```shell
# replicate 3scale-gateway pod to 3 replicas
kubectl -n kourier-system patch deploy 3scale-kourier-gateway --patch '{"spec":{"replicas":1}}'
kubectl -n kourier-system patch deploy 3scale-kourier-gateway --patch '{"spec":{"template":{"spec":{"affinity":{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"kubernetes.io/hostname","operator":"In","values":["master-node"]}]}]}}}}}}}'

# use local gateway for every request
kubectl -n kourier-system patch service kourier --patch '{"spec":{"internalTrafficPolicy":"Local","externalTrafficPolicy":"Local"}}'
kubectl -n kourier-system patch service kourier-internal --patch '{"spec":{"internalTrafficPolicy":"Local"}}'
```

#### Activator

```shell
# replicate activator pod to 3 replicas
kubectl -n knative-serving patch deploy activator --patch '{"spec":{"replicas":1}}'
kubectl -n knative-serving patch deploy activator --patch '{"spec":{"template":{"spec":{"affinity":{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"kubernetes.io/hostname","operator":"In","values":["master-node"]}]}]}}}}}}}'
```

#### Config-map

```shell
kubectl patch configmap config-features \
  -n knative-serving \
  --type merge \
  -p '{"data":{"kubernetes.podspec-nodeselector":"enabled"}}'

kubectl patch configmap config-features \
  -n knative-serving \
  --type merge \
  -p '{"data":{"kubernetes.podspec-fieldref":"enabled"}}' 

```

#### Check your configuration

```shell
thai@master-node ~/ken/serverless-measurement-doc
 $ kubectl -n knative-serving get pod -o wide | grep activator
activator-6f5448b646-9sfzp                1/1     Running     0          4m15s   10.233.113.141   master-node   <none>           <none>
```

```shell
thai@master-node ~/ken/serverless-measurement-doc
 $ kubectl -n kourier-system get pod -o wide
NAME                                      READY   STATUS    RESTARTS   AGE     IP               NODE          NOMINATED NODE   READINESS GATES
3scale-kourier-gateway-5886fc6dbd-tcf8v   1/1     Running   0          8m31s   10.233.113.139   master-node   <none>           <none>
```

> [!IMPORTANT]
> Make sure your cluster have exactly one activator and one gateway each node

## Network configuration

In this work, we use [traffic-control](https://github.com/kenphunggg/traffic-control.git) for emulating latency and bandwidth between nodes.

## Setting up Prometheus for measuring

### Install Prometheus

Precompiled binaries for released versions are available in the [download](https://prometheus.io/download/) section on [Prometheus](https://prometheus.io/). Using the latest production release binary is the recommended way of installing Prometheus. See the [Installing](https://prometheus.io/docs/prometheus/latest/installation/) chapter in the documentation for all the details.

Download the latest release of Prometheus for your platform

```shell
# Download Prometheus monitoring system and time series database.
wget https://github.com/prometheus/prometheus/releases/download/v3.5.0/prometheus-3.5.0.linux-amd64.tar.gz

# Download Prometheus Alertmanager
wget https://github.com/prometheus/alertmanager/releases/download/v0.28.1/alertmanager-0.28.1.linux-amd64.tar.gz

# Download Blackbox prober exporter
wget https://github.com/prometheus/blackbox_exporter/releases/download/v0.27.0/blackbox_exporter-0.27.0.linux-amd64.tar.gz

# Download Exporter for Consul metrics
wget https://github.com/prometheus/consul_exporter/releases/download/v0.13.0/consul_exporter-0.13.0.linux-amd64.tar.gz

# Download graphite_exporter
wget https://github.com/prometheus/graphite_exporter/releases/download/v0.16.0/graphite_exporter-0.16.0.linux-amd64.tar.gz

# Download memcached_exporter
wget https://github.com/prometheus/memcached_exporter/releases/download/v0.15.3/memcached_exporter-0.15.3.linux-amd64.tar.gz

# Download mysqld_exporter
wget https://github.com/prometheus/mysqld_exporter/releases/download/v0.17.2/mysqld_exporter-0.17.2.linux-amd64.tar.gz

# Download node_exporter
wget https://github.com/prometheus/node_exporter/releases/download/v1.9.1/node_exporter-1.9.1.linux-amd64.tar.gz

# Download promlens
wget https://github.com/prometheus/promlens/releases/download/v0.3.0/promlens-0.3.0.linux-amd64.tar.gz

# Download push gateway
wget https://github.com/prometheus/pushgateway/releases/download/v1.11.1/pushgateway-1.11.1.linux-amd64.tar.gz

# statsd exporter
wget https://github.com/prometheus/statsd_exporter/releases/download/v0.28.0/statsd_exporter-0.28.0.linux-amd64.tar.gz
```

Then extract it 

```shell
tar xvfz prometheus-*.tar.gz
tar xvf node_exporter-1.9.1.linux-amd64.tar.gz
sudo mv prometheus-3.5.0.linux-amd64/promtool /usr/local/bin/
sudo mv prometheus-3.5.0.linux-amd64/prometheus /usr/local/bin/
sudo mv node_exporter-1.9.1.linux-amd64/node_exporter /usr/local/bin
```

The Prometheus configuration is written in the YAML file, its default configuration is in the folder `prometheus-3.5.0.linux-amd64` with the name `prometheus.yml` we unzipped it above, let's take a look at it.

```shell
cat prometheus-3.5.0.linux-amd64/prometheus.yml
```

### Running Prometheus

We will now run Prometheus, but before that, we should move the configuration file to a more appropriate folder.

```shell
sudo mkdir -p /etc/prometheus
sudo mv prometheus-3.5.0.linux-amd64/prometheus.yml /etc/prometheus
```

Run Prometheus Exporter

```shell
node_exporter
```

Now we can run the `Prometheus` Server

```shell
prometheus --config.file "/etc/prometheus/prometheus.yml"
```

## Measurement

Follow our our guild for measuring [here](https://github.com/kenphunggg/serverless-measurement.git)









