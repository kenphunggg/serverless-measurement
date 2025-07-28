import k8s_API
from variables import *
# from multiprocessing import Event, Process
# from multiprocessing.pool import ThreadPool
import subprocess
import threading
from time import sleep
import queue


def exec_pod(cmd: str):
    results = []
    threads = []
    result_queue = queue.Queue()
    output_lock = threading.Lock()
    status = True
    list_pod = []
    list_pod = k8s_API.get_list_term_pod(NAMESPACE)
    for i in list_pod:
        t = threading.Thread(target=connect_pod_exec, args=(cmd.format(i.pod_ip), result_queue, output_lock, ))
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

def connect_pod_exec(target_command: str, result_queue, lock, target_name: str = "ubuntu"):
    print(target_command)
    command = "kubectl exec -it {} -- {} ".format(target_name, target_command)
    trial = 0
    # while trial < 20:
    try:
        output = subprocess.check_output(['/bin/bash', '-c', command]) # or check_output
        with lock:
            result_queue.put(output)
    except subprocess.CalledProcessError as e:
        output = str(e.output)
        # with output_lock:
        #     print("Subprocess output is: {}".format(output))
        # if "52" in output:
        #     # with output_lock:
        #     #     print("Terminated successfully")
        #     return 
        # else:
        #     # with output_lock:
        #     #     print("Terminated unsuccessfully, trial: {}".format(trial))
        #     sleep(1)
        #     trial = trial + 1
        #     continue
        # with output_lock:
        #     print("Seem like a good request, but we never know :)")
    return output
    # with output_lock:
    #     print("The system has sent {} times curl cmd, but none returns successfully.".format(trial))



if __name__ == "__main__":
    if k8s_API.is_pod_terminated():
        print("Detect terminating pod, it'll be deleted shortly")
        exec_pod(CURL_TERM)