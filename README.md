# serverless-measurement
Measurement code for Serverless-based applications

## Purpose

Built specifically to deploy and measure `Knative` function in terms of resource consumption and latency. However, it can be used for any K8S-related platform to conduct the same purpose. This code is able to collect:

```
Resource consumption -- CPU, GPU, RAM (%)
Duration (s) and response time (ms)
Frame Per Second (FPS) and bitrate (kbps) for app that exposes API to get this info
```

## Before running

This code is based on `python3.12.3`, before running, you must install `requirements.txt`

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

After that, you must adjust value in [config.json](config/config.json)

```js
{
    "prometheus": {
            "server_ip": "192.168.17.161",
            "port": 9090,
            "interface": "ens33"
        }
}
```
This is setting for `Prometheus` server
- `server_ip`: ip address that your Prometheus server is running on
- `port`: port of your Prometheus server
- `interface`: to measure network, you must specify interface to measure

### Web Measurement

Sample configuration for web services is shown in [config_web.json](config/config_web.json)

```js
{
    "test_case": "web",
    "repetition": 1,
    "replicas": [
        1
    ],
    "ksvc_name": "measure-web",
    "arch": "x86",
    "image": "docker.io/tienshawn/measure-web:fix-sigterm-1.0@sha256:f66498589b9dbcba892405a997094997864561f52003d0999d8ad5dc572ec0d4",
    "port": 5000,
    "namespace": "serverless",
    "hostname": "cloud-node",
    "host_ip": "192.168.17.162",
    "cool_down_time": 20,
    "curl_time": 10,
    "detection_time": 30
}
```

- `test_case`: type of measurement, in this case is `web`
- `repetition`: in general standard of measurement, it must be measure multiple times, this specific total measure time
- `replicas`: in case you want to measure action of serverless when deploy multiple replicas. You can config it like this [1, 2, 3] means that first time deploy 1 replica, second time deploy 2 replicas ...
- `ksvc_name`: specific name of `ksvc` - `Knative` service
- `arch`: architecture of node you deploy application
- `image`: specify image of application you want to measure
- `port`: port that your application is running on
- `namespace`: namespace you want to specify your application running on
- `hostname`: name of your node
- `host_ip`: ip of your node
- `cool_down_time`: for more accuracy, between test cases it must have cool down time for physical node to return to that state
- `curl_time`: to get response time, we execute multiple curl commands, this specify this
- `detection_time`: to measure hardware usage, we make the pod running in `detection_time` seconds to measure hardware usage

### Streaming Measurement

Sample configuration for web services is shown in [config_streaming.json](config/config_streaming.json)

```js
{
    "test_case": "streaming",
    "repetition": 2,
    "replicas": [
        1
    ],
    "ksvc_name": "measure-streaming",
    "arch": "x86",
    "image": "docker.io/lazyken/measure-streaming:v1@sha256:3edd67c01b0326fbd9a194d60899cc7f88b888466393d68f9bb1b169dae1a1ff",
    "port": 5000,
    "namespace": "serverless",
    "hostname": "edge-node",
    "host_ip": "192.168.17.163",
    "cool_down_time": 30,
    "curl_time": 1,
    "detection_time": 30,
    "resolution": 360
}
```

- `test_case`: type of measurement, in this case is `web`
- `repetition`: in general standard of measurement, it must be measure multiple times, this specific total measure time
- `replicas`: in case you want to measure action of serverless when deploy multiple replicas. You can config it like this [1, 2, 3] means that first time deploy 1 replica, second time deploy 2 replicas ...
- `ksvc_name`: specific name of `ksvc` - `Knative` service
- `arch`: architecture of node you deploy application
- `image`: specify image of application you want to measure
- `port`: port that your application is running on
- `namespace`: namespace you want to specify your application running on
- `hostname`: name of your node
- `host_ip`: ip of your node
- `cool_down_time`: for more accuracy, between test cases it must have cool down time for physical node to return to that state
- `curl_time`: to get response time, we execute multiple curl commands, this specify this
- `detection_time`: to measure hardware usage, we make the pod running in `detection_time` seconds to measure hardware usage
- `resolution`: our provided image have expose an api to specify which resolution we will runnning, this will do this
