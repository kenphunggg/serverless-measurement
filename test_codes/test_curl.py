import os
import subprocess

from functional_methods import exec_pod, get_fps_exec
from variables import CURL_FPS

def curl_time(URL :str):
    # Define the command to run
    # cmd = "./myscript.sh https://www.google.com"

    # Run the command and capture its output
    output = subprocess.check_output(['./curltime.sh', URL])
    output = output.replace(b",", b".")
    # print(output)

    # Extract the values you're interested in from the output
    # print(output.split(b"time_pretransfer:  ")[1].split(b" ")[0])
    
    time_namelookup = float(output.split(b"time_namelookup:  ")[1].split(b" ")[0])
    time_connect = float(output.split(b"time_connect:  ")[1].split(b" ")[0])
    time_appconnect = float(output.split(b"time_appconnect:  ")[1].split(b" ")[0])
    time_pretransfer = float(output.split(b"time_pretransfer:  ")[1].split(b" ")[0])
    time_redirect = float(output.split(b"time_redirect:  ")[1].split(b" ")[0])
    time_starttransfer = float(output.split(b"time_starttransfer:  ")[1].split(b" ")[0])
    time_total = float(output.split(b"time_total:  ")[1].split(b" ")[0])

    # Store the values in a dictionary
    times = {"time_namelookup": time_namelookup, "time_connect": time_connect, "time_appconnect": time_appconnect, "time_pretransfer": time_pretransfer,
              "time_redirect": time_redirect, "time_starttransfer": time_starttransfer, "time_total": time_total}

    # Print the dictionary
    print(times)
 

if __name__=="__main__":
    # multiservice_pods.update_replicas(3, "pi4", "29061999/knative-video-detection-arm@sha256:47705b6d9561b0fe45fadf559802a8d500c32b069fd50ef0fc69e6859c34a9e3")
    # create_request_thread(8)
    # curl_time("google.com")
    # print(CURL_FPS)
    exec_pod(CURL_FPS, 2, "fps")
    get_fps_exec("mec", 2, 1)