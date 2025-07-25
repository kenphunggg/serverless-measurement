from time import sleep
# from main import *
from datetime import datetime
from subprocess import call  
import requests  
import threading
import re
import string
# import multiservice_pods

pwd='1'
cmd='kubectl apply -f /home/controller/knative-caculation/deployments/nginx-pod.yaml'
video_path = "/test_video/highway.mp4"
HELLO = "hello = {{'{}'}}"

timestamps = {}
terminate_state = defaultdict(list)

def create_request(url: str):
    print(url)
    rs_response = requests.get(url)
    print(rs_response.content)
    # subprocess.call(['sh','./deployments/curl.sh'])

def create_request_thread(target_pods: int):
    video_path = "test_video/" + "highway.mp4"
    for i in range(target_pods):
        print("Start thread :", i + 1)
        threading.Thread(target=create_request, args=("http://detection"+str(i+1)+".default.svc.cluster.local/{}".format(video_path),)).start()

def exec(command:str):
    subprocess.call('echo {} | sudo -S {}'.format(pwd, command), shell=True)

def get_pod_status():
    return str(subprocess.run(['deployments/get_pod_status.sh', '-l'], stdout=subprocess.PIPE).stdout.decode('utf-8')).strip()
    
    # print(re.search('\n(.*)\n', str(result)))
    # return re.search('(.*)', str(result))

def get_pods_existed():
    timestamps["start"]=time.time()
    url_server_running_pod = PROMETHEUS_DOMAIN + RUNNING_PODS_QUERY.format(CALCULATING_HOSTNAME)
    timestamps["end"]=time.time()
    # values_running_pods=json.loads(urllib.request.urlopen(url_server_running_pod).read())["data"]["result"][0]['value'][1]
    # return int(values_running_pods)

def is_pod_terminating():
    status = str(subprocess.run(['deployments/get_pod_status.sh', '-l'], stdout=subprocess.PIPE).stdout.decode('utf-8')).strip()
    return status == TERMINATING_STATUS 

def curl_time():
    # Define the command to run
    # cmd = "./myscript.sh https://www.google.com"

    # Run the command and capture its output
    output = subprocess.check_output(['curltime.sh', 'google.com'])

    # Extract the values you're interested in from the output
    time_pretransfer = float(output.split(b"time_pretransfer: ")[1].split(b" ")[0])
    time_redirect = float(output.split(b"time_redirect: ")[1].split(b" ")[0])

    # Store the values in a dictionary
    times = {"time_pretransfer": time_pretransfer, "time_redirect": time_redirect}

    # Print the dictionary
    print(times)


if __name__=="__main__":
    # multiservice_pods.update_replicas(3, "pi4", "29061999/knative-video-detection-arm@sha256:47705b6d9561b0fe45fadf559802a8d500c32b069fd50ef0fc69e6859c34a9e3")
    # create_request_thread(8)
    curl_time()