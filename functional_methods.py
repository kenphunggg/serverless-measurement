from collections import defaultdict
from datetime import datetime
import queue
import requests
import variables
import urllib.request
import json
import time
import csv
import paramiko
import sys
import threading
from multiprocessing import Event, Process
from multiprocessing.pool import ThreadPool
import subprocess
import os
import signal
from power import pw, em
from variables import *
import k8s_API
from time import sleep
import re
import psutil


def get_bytes():
    return psutil.net_io_counters().bytes_sent + psutil.net_io_counters().bytes_recv

# def get_mbits():
#     return (psutil.net_io_counters().bytes_sent + psutil.net_io_counters().bytes_recv)/1024./1024.*8


# def thread_remote(cmd: str):
#     thread = threading.Thread(target=remote_worker_call, args=(cmd, )).start()
#     return thread


def remote_worker_call(command: str, host_username: str, host_ip: str, host_pass: str, event=None):
    print("Trying to connect to remote host {}, IP: {}".format(
        host_username, host_ip))
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(host_ip, username=host_username,
                       password=host_pass)
    except paramiko.AuthenticationException:
        print("Authentication failed when connecting to %s" % host_ip)
        sys.exit(1)
    except:
        print("Could not SSH to %s, waiting for it to start" % host_ip)
    print(command)
    stdin, stdout, stderr = client.exec_command(command, get_pty=True)
    stdin.write(JETSON_PASSWORD + '\n')
    stdin.flush()
    for line in stdout:
        print(line.strip('\n'))
    client.close()
    if event is not None:
        event.set()


def get_data_from_api(query: str):
    url_data = PROMETHEUS_DOMAIN + query
    try:
        contents = urllib.request.urlopen(url_data).read().decode('utf-8')
        values = json.loads(contents)["data"]["result"][0]['value']
    except:
        values = -1
    return values

# def get_power():
#     # print(pw.get_power()/1000.0)
#     return pw.get_power()/1000.0

# def is_pod_terminating(): # check pod is terminating? consider replacing this function with Py-python
#     status = str(subprocess.run(['deployments/get_pod_status.sh', '-l'], stdout=subprocess.PIPE).stdout.decode('utf-8')).strip()
#     return status == TERMINATING_STATUS


def get_curl_values_and_update_job(cmd: str, host: str, image: str, target_pods: int, job: str, quality: str, repetition: int):
    # Run the command and capture its output
    status, results = exec_pod(cmd, target_pods, "curl_time")
    # print("Size of results: {}".format(len(results)))
    for i, output in enumerate(results, start=1):
        # output_t = output.decode("utf-8")
        # print("Type of output: {}".format(type(output)))
        # print(output)
        if b"OK" in output or b"Active" in output:
            print("'OK' request {}".format(i))
        else:
            print("Error in request {}".format(i))
            try:
                writer = csv.writer(open(DATA_CURL_FILE_DIRECTORY.format(
                    str(host), str(image), str(target_pods), str(repetition), generate_file_time), 'a'))
                writer.writerow([job, quality, "error"])
            except Exception as ex:
                print(ex)
            continue
        output = output.replace(b",", b".")

        time_namelookup = float((output.split(
            b"time_namelookup:  ")[1].split(b" ")[0]).split(b"s")[0])
        
        time_connect = float((output.split(b"time_connect:  ")[1].split(b" ")[0]).split(b"s")[0])

        time_appconnect = float((output.split(
            b"time_appconnect:  ")[1].split(b" ")[0]).split(b"s")[0])
        
        time_pretransfer = float((output.split(
            b"time_pretransfer:  ")[1].split(b" ")[0]).split(b"s")[0].split(b"s")[0])
        
        time_redirect = float((output.split(b"time_redirect:  ")[1].split(b" ")[0]).split(b"s")[0])

        time_starttransfer = float((output.split(
            b"time_starttransfer:  ")[1].split(b" ")[0]).split(b"s")[0])
        
        time_total = float((output.split(b"time_total:  ")[1].split(b" ")[0]).split(b"s")[0].split(b"s")[0])

        # Store the values in a dictionary
        # time_dict = {"time_namelookup": time_namelookup, "time_connect": time_connect, "time_appconnect": time_appconnect, "time_pretransfer": time_pretransfer,
        #           "time_redirect": time_redirect, "time_starttransfer": time_starttransfer, "time_total": time_total}

        # Write value to data file
        try:
            writer = csv.writer(open(DATA_CURL_FILE_DIRECTORY.format(
                str(host), str(image), str(target_pods), str(repetition), generate_file_time), 'a'))
            writer.writerow([time_namelookup, time_connect, time_appconnect,
                            time_pretransfer, time_redirect, time_starttransfer, time_total, job, quality])
        except Exception as ex:
            print(ex)


def get_prometheus_values_and_update_job(host: str, image: str, target_pods: int, state: str, repetition: int):
    ip = ""
    gpu_query = ""
    values_power = 0
    values_energy = 0
    if host == 'jetson':
        ip = JETSON_IP
        # gpu_query = VALUES_GPU_QUERY_JETSON
        values_power = pw.get_power()/1000.0
        values_energy = 0 # Jetson power board has now energy value, so pls ignore it
    else:
        ip = MEC_IP
        gpu_query = VALUES_GPU_QUERY_MEC
        voltage, current, energy, real_power, apparent_power, reactive_power, power_factor, frequency = em.get_energy_data()
        # real_power = 100000
        values_power = real_power/100.0
        values_energy = energy*36 #Convert from Wh --> J

    values_per_cpu_in_use = get_data_from_api(VALUES_CPU_QUERY.format(ip)) # query CPU
    # values_per_gpu_in_use = get_data_from_api(gpu_query.format(ip)) # query CPU, turn on if GPU exporter exists
    values_per_gpu_in_use = [0,0] # turn off if GPU exporter exists
    # values_network_receive = get_data_from_api(VALUES_NETWORK_RECEIVE_QUERY) # turn on to measure bandwidth
    values_memory = get_data_from_api(VALUES_MEMORY_QUERY.format(ip, ip, ip)) # query RAM
    values_running_pods = k8s_API.get_number_pod() # query number of running pods

    # write values to csv file
    try:
        writer = csv.writer(open(DATA_PROMETHEUS_FILE_DIRECTORY.format(
            str(host), str(image), str(target_pods), str(repetition), generate_file_time), 'a'))
        writer.writerow([values_memory[0], datetime.utcfromtimestamp(values_memory[0]).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], values_running_pods,
                         values_power, values_energy, values_per_cpu_in_use[1], values_per_gpu_in_use[1], values_memory[1], values_nw, state])
    except Exception as ex:
        print(ex)
    # if TEST_MODE: print("Current pods: %s, target: %d" % (curr_pods, (int(target_pods)+POD_EXSISTED)))



def bash_cmd(cmd: str):
    result = subprocess.run([cmd], stderr=subprocess.PIPE, text=True)
    print("Bash output: {}".format(result.stderr))

# def create_request_thread(target_pods: int, request_type: str):
#     for i in range(target_pods):
#         print("Start thread :", i + 1)
#         if request_type == "start":
#             threading.Thread(target=create_request, args=("http://detection"+str(i+1)+".default.svc.cluster.local/{}/api/stream/{}/{}".format(STREAMING_IP, DETECTION_TIME))).start()
#         else if request_type == "stop":
#             threading.Thread(target=create_request, args=("http://detection"+str(i+1)+".default.svc.cluster.local/api/terminate")).start()
#         else:
#             break


def timestamps_to_file(host: str, image: str, timestamps: dict, target_pods: int, repetition: int):
    # print(timestamps)
    with open(DATA_TIMESTAMP_FILE_DIRECTORY.format(
            str(host), str(image), str(target_pods), str(repetition), generate_file_time), 'w') as f:
        # for key, value in terminate_state.items():
        #     timestamps[key+"_start"]=min(value)
        #     timestamps[key+"_end"]=max(value)
        for key in timestamps.keys():
            if "_start" in key:
                job_key = re.search('(.*)_start', key).group(1)
            if "_end" in key:
                job_key = re.search('(.*)_end', key).group(1)
            f.write("%s,%s,%s\n" % (key, timestamps[key], job_key))

# NOTE: the following function auto terminate pods


def auto_delete(target_pod, event):
    token = True
    while not event.is_set():
        if k8s_API.is_pod_terminated() and not k8s_API.is_all_con_not_ready() and token:
            print("Detect terminating pod, it'll be deleted shortly")
            if exec_pod(CURL_TERM, target_pod, "auto_delete"):
                token = False
            else:
                print("Try to terminate pod, but IP returns None, will try again!")
                token = True
        elif not k8s_API.is_pod_terminated():
            # print("Status is: {}, token is: {}".format(k8s_API.is_pod_terminated(), token))
            token = True
        else:
            # print("Status is: {}, token is: {}".format(k8s_API.is_pod_terminated(), token))
            sleep(1)
        # print("In terminating while loop")
    print("Overwatch for termination finished!")


# def exec_pod(cmd: str, type: str = "normal"):
#     results = []
#     threads = []
#     IPs = []
#     result_queue = queue.Queue()
#     output_lock = threading.Lock()
#     status = True

#     if type == "auto_delete":
#         list_pod = k8s_API.get_list_term_pod(NAMESPACE)
#     else:
#         list_pod = k8s_API.list_namespaced_pod_status(NAMESPACE)
    
#     for i in list_pod:
#         IP = i.pod_ip
#         if (IP is None):
#             return False, results
#         else:
#             IPs.append(IP)
#     for ip in IPs:
#         t = threading.Thread(target=connect_pod_exec, args=(cmd.format(ip), result_queue, output_lock, ))
#         threads.append(t)
#     for t in threads:
#         t.start()
#     for t in threads:
#         t.join()
    
#     while not result_queue.empty():
#         result = result_queue.get()
#         results.append(result)
#     status = True
#     return status, results

def exec_pod(cmd: str, target_pod: int, type: str = "normal"):
    results = []
    threads = []
    IPs = []
    result_queue = queue.Queue()
    output_lock = threading.Lock()
    status = True
    if type == "auto_delete":
        list_pod = []
        while len(list_pod) < target_pod: # when multiple pods are deployed, sometimes the code can't query the number of term pod correctly
            list_pod = k8s_API.get_list_term_pod(NAMESPACE)
            print("Query of list_term_pod is {}, while target_pod is {}".format(len(list_pod), target_pod))
        for i in list_pod:
            t = threading.Thread(target=connect_pod_exec, args=(cmd.format(i.pod_ip), result_queue, output_lock, ))
            threads.append(t)
    elif type == "fps":
        for i in range(1, target_pod + 1, 1):
            t = threading.Thread(target=connect_pod_exec, args=(cmd.format(i, i), result_queue, output_lock, ))
            threads.append(t)
    else:
        for i in range(1, target_pod + 1, 1):
            t = threading.Thread(target=connect_pod_exec, args=(cmd.format(i), result_queue, output_lock, ))
            threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    while not result_queue.empty():
        result = result_queue.get()
        results.append(result)
    status = True
    return status, results

def get_fps_exec(host, target_pod, rep):
    try:
        for i in range(1,target_pod + 1, 1):
            cmd = "kubectl cp ubuntu:file{}.log".format(i) + " " + DATA_FPS_FILE_DIRECTORY.format(host, target_pod, rep, i, generate_file_time)
            print(cmd)
            output = subprocess.check_output(['/bin/bash', '-c', cmd]) # or check_output
    except subprocess.CalledProcessError as e:
        output = str(e.output)
    return output

def connect_pod_exec(target_command: str, result_queue, lock, target_name: str = "ubuntu"):
    print(target_command)
    command = "kubectl exec -it {} -- {} ".format(target_name, target_command)
    trial = 0
    while trial < 20:
        try:
            output = subprocess.check_output(['/bin/bash', '-c', command]) # or check_output
            with lock:
                result_queue.put(output)
        except subprocess.CalledProcessError as e:
            output = str(e.output)
            # with output_lock:
            #     print("Subprocess output is: {}".format(output))
            if "52" in output or "200" in output:
                # with output_lock:
                #     print("Terminated successfully")
                return 
            else:
                # with output_lock:
                #     print("Terminated unsuccessfully, trial: {}".format(trial))
                sleep(1)
                trial = trial + 1
                continue
        # with output_lock:
        #     print("Seem like a good request, but we never know :)")
        return output
    # with output_lock:
    #     print("The system has sent {} times curl cmd, but none returns successfully.".format(trial))

def config_deploy(cmd: str):
    Process(target=k8s_API.config_deploy, args=(cmd, )).start()
    # threading.Thread(target= k8s_API.config_deploy, args=(cmd, )).start()


if __name__ == "__main__":

    output = subprocess.check_output(['/bin/bash', '-c', 'curl -w \"@curl-time.txt\" google.com'])
        # print(output)

        # Extract the values you're interested in from the output
        # print(output.split(b"time_pretransfer:  ")[1].split(b" ")[0])

    time_namelookup = float(output.split(
        b"time_namelookup:  ")[1].split(b" ")[0])
    time_connect = float(output.split(b"time_connect:  ")[1].split(b" ")[0])
    time_appconnect = float(output.split(
        b"time_appconnect:  ")[1].split(b" ")[0])
    time_pretransfer = float(output.split(
        b"time_pretransfer:  ")[1].split(b" ")[0])
    time_redirect = float(output.split(b"time_redirect:  ")[1].split(b" ")[0])
    time_starttransfer = float(output.split(
        b"time_starttransfer:  ")[1].split(b" ")[0])
    time_total = float(output.split(b"time_total:  ")[1].split(b" ")[0])
    # Store the values in a dictionary
    time_dict = {"time_namelookup": time_namelookup, "time_connect": time_connect, "time_appconnect": time_appconnect, "time_pretransfer": time_pretransfer,
                "time_redirect": time_redirect, "time_starttransfer": time_starttransfer, "time_total": time_total}
    print(time_dict)
    # Write value to data file
    # try:
    #     writer = csv.writer(open(DATA_CURL_FILE_DIRECTORY.format(
    #         str(host), str(image), str(target_pods), str(repetition), generate_file_time), 'a'))
    #     writer.writerow([time_namelookup, time_connect, time_appconnect,
    #                     time_pretransfer, time_redirect, time_starttransfer, time_total, job])
    # except Exception as ex:
    #     print(ex)
