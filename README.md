# serverless-measurement
Measurement code for Serverless-based container life-cycle 

## Purpose:
Built specifically to deploy and measure `Knative` serverless function's lifecycle in terms of energy, resource consumption and latency.
However, it can be used for any K8S-related platform to conduct the same purpose. This code is able to collect:
```
Resource consumption -- CPU, GPU, RAM (%)
Power consumption (W) 
Energy consumption (Wh/J)
Duration (s) and response time (ms)
Frame Per Second (FPS) for app that exposes API to get this info

```

## Before running:
You can see the required library in the [`main.py`](./main.py) file. Other than that, there are some prerequisites as follows:

- File [`curl.yaml`](./curl.yaml) must be deployed and stay in the system forever; it serves as the container for the curl command to run over.
- Prometheus must be installed, and its address must be given in the [`variable.py`](./variable.py) file.
- Tinkerforge devices and their relevant libraries must be implemented in order to be used in the [`power.py`](./power.py) file.
- Images for applications you want to measure—premade images (YOLO) are available in [`/application`](./application).


## Basic cmds:
File `auto_measure.py` contains a loop, which repetitively runs the `main.py` file. Why repetition? because it's basic measurement requirement. Run `auto_measure.py` by: `python3 auto_measure.py`  

File `main.py` control which part of lifecycle needs to be measured, it can be run independently for testing purpose. This files has three important functions:  
  
- `collect_life_cycle` for measuring most of the lifecycle.
- `curl_latency` for measuring the response time perceived by end-users through **curl** cmd
- `collect_cold_warm_disk` for temporarily measuring the *Cold to Warm Disk* process. This is previously measured by detecting the **Pulled** status of deployment  

However, a bug occurred in the code leading to wrong detection. Notice that result from this process must be minused with *Null to Cold* and *Warm Disk to Warm CPU* to get the real *Cold to Warm Disk* value.  

Since Knative is having problem with terminating deployment, an `auto_delete` process is written inside `functional_methods` to automatically detect **Terminating** pod's status and send a terminated signal there. This process automatically runs when user runs file `auto_measure.py`.  

In order to manually delete one or multiple pods that are stuck at **Terminating** without running measurement, please run `python3 delete.py`. This function acts just like the described one above, but as a standalone version. 