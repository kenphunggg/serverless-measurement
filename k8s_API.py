from kubernetes import client, config
import subprocess
import random
import datetime
from time import sleep
import yaml
import re
from variables import *

class KubernetesPod:
    def __init__(self, pod_name, pod_status, pod_ip, node_name, sum_pod_container, number_container_ready):
        self.pod_name = pod_name
        self.pod_status = pod_status
        self.pod_ip = pod_ip
        self.node_name = node_name
        self.sum_pod_container = sum_pod_container
        self.number_container_ready = number_container_ready

class PodEvents:
    def __init__(self, pod_name, event_time, event, event_message) -> None:
        self.pod_name = pod_name
        self.event_time = event_time
        self.event = event
        self.event_message = event_message
        
def list_namespaced_pod(target_namespace: str = NAMESPACE):
    api_get_pods_response = ApiV1.list_namespaced_pod(target_namespace)

def list_namespaced_pod_status(target_namespace: str = NAMESPACE):
    list_pod_status = []
    api_get_pods_response = ApiV1.list_namespaced_pod(target_namespace)
    for pod in api_get_pods_response.items:
        current_pod_name = pod.metadata.name
        current_node_name = pod.spec.node_name
        current_pod_ip = pod.status.pod_ip
        current_pod_state = ""
        if pod.metadata.deletion_timestamp != None and (pod.status.phase == 'Running' or pod.status.phase == 'Pending'):
            current_pod_state = 'Terminating'
        elif pod.status.phase == 'Pending':
            try:
                for container in pod.status.container_statuses:
                    if container.state.waiting != None:
                        current_pod_state = container.state.waiting.reason
            except:
                return list_pod_status
        else:
            current_pod_state = str(pod.status.phase)
        sum_pod_container = len(pod.status.container_statuses)
        number_container_ready = 0
        for container in pod.status.container_statuses:
            if container.ready == True:
                number_container_ready += 1
        list_pod_status.append(KubernetesPod(
            current_pod_name, current_pod_state, current_pod_ip, current_node_name, sum_pod_container, number_container_ready))
    return list_pod_status


def get_number_namespaced_pod_through_status(target_status: str, target_namespace: str = NAMESPACE):
    count = 0
    list_pod = list_namespaced_pod_status(target_namespace)
    for pod in list_pod:
        if pod.pod_status == target_status:
            count += 1
    return count

# NOTE: get event of pod over  pod's name
#      return array of PodEvents class
def list_namespaced_event(target_pod_name: str, target_namespace: str = NAMESPACE):
    list_pod_event = []
    events_response = ApiV1.list_namespaced_event(
        target_namespace, field_selector=f'involvedObject.name={target_pod_name}')
    for event in events_response.items:
        current_event_time = event.first_timestamp
        current_event = event.reason
        current_event_message = event.message
        if current_event_time != None:
            current_event_time = current_event_time.timestamp()
        list_pod_event.append(PodEvents(target_pod_name, current_event_time,
                                        current_event, current_event_message))
    return list_pod_event

# NOTE:- check if the image image in the pod pulled since pod started
#      - check if the image image in the pod pulled since a timestamp (optional)
#      - return True or False
def is_image_available(target_pod: str, start_timeline: datetime = None):
    name = ""
    is_pulled = False
    api_get_pods_response = ApiV1.list_namespaced_pod(NAMESPACE)
    if target_pod == "random" and len(api_get_pods_response.items) != 0:
        # print(list_namespaced_pod_status())
        pod = random.choice(api_get_pods_response.items)
        name = pod.metadata.name
    elif len(api_get_pods_response.items) == 0:
        return False
    else:
        name = target_pod
    # print(name)
    events = list_namespaced_event(name)
    for event in events:
        # print(event.event)
        if start_timeline != None and event.event_time != None:
            if event.event_time < start_timeline.timestamp():
                continue
        if event.event == "Pulled":
            is_pulled = True
            break
    return is_pulled

def create_namespaced_service(target_service: str, target_ID: str,
                              target_service_port: int, target_namespace: str = "serverless"):
    service_name = target_service + "-" + target_ID + "-service"
    service_selector = target_service + "-" + target_ID + "-deployment"
    body = client.V1Service(
        api_version="v1",
        kind="Service",
        metadata=client.V1ObjectMeta(name=service_name),
        spec=client.V1ServiceSpec(
            selector={"app": service_selector, "ID": target_ID},
            type="ClusterIP",
            ports=[client.V1ServicePort(
                port=target_service_port,
                target_port="container-port")]))
    try:
        response = ApiV1.create_namespaced_service(
            namespace=target_namespace, body=body)
    except:
        return ("There is unknown error when deploy {}.".format(service_name))
    return ("Deploy {} succesfully.".format(service_name))


def create_namespaced_deployment(target_deployment: str, target_ID: str, target_image: str,
                                 target_container_port: int, target_env, target_namespace: str = "serverless"):
    deployment_name = target_deployment + "-" + target_ID + "-deployment"
    body = (
        client.V1Deployment(
            api_version="apps/v1",
            kind="Deployment",
            metadata=client.V1ObjectMeta(
                name=deployment_name
            ),
            spec=client.V1DeploymentSpec(
                selector=client.V1LabelSelector(
                    match_labels={"app": deployment_name, "ID": target_ID}
                ),
                template=client.V1PodTemplateSpec(
                    metadata=client.V1ObjectMeta(
                        labels={"app": deployment_name, "ID": target_ID}
                    ),
                    spec=client.V1PodSpec(
                        containers=[client.V1Container(
                            name=target_deployment,
                            image=target_image,
                            ports=[client.V1ContainerPort(
                                container_port=target_container_port,
                                name="container-port"
                            )],
                            env=target_env
                        )]
                    )
                )
            )

        )
    )
    try:
        response = AppV1.create_namespaced_deployment(
            body=body, namespace=target_namespace)
    except:
        return ("There is unknown error when deploy {}.".format(deployment_name))
    return ("Deploy {} succesfully.".format(deployment_name))


def delete_namespaced_deployment(target_deployment: str, target_ID: str, target_namespace: str = "serverless"):
    deployment_name = target_deployment + "-" + target_ID + "-deployment"
    try:
        AppV1.delete_namespaced_deployment(deployment_name, target_namespace)
    except:
        return ("There is unknown error when delete {}.".format(deployment_name))
    return ("Delete {} succesfully.".format(deployment_name))


def delete_namespaced_service(target_service: str, target_ID: str, target_namespace: str = "serverless"):
    service_name = target_service + "-" + target_ID + "-service"
    try:
        ApiV1.delete_namespaced_service(service_name, target_namespace)
    except:
        return ("There is unknown error when delete {}.".format(service_name))
    return ("Delete {} succesfully.".format(service_name))

#NOTE: Consider rewriting the following functions by Python-k8s
def config_deploy(cmd : str, path_file_output : str = DEPLOYMENT_PATH):
    try:
        subprocess.call('echo {} | sudo {}/./kubectl.sh {} {}'.format(MASTER_PASSWORD, BASH_PATH, cmd, path_file_output), shell=True)
    except Exception as e:
        print(f"An error occurred: {e}")
    # print("Service deployed")


def connect_pod_exec(target_command: str, target_name: str = "ubuntu"):
    command = "kubectl exec -it {} -- {} ".format(target_name, target_command)
    trial = 0
    while trial < 20:
        try:
            subprocess.check_output(['/bin/bash', '-c', command])
        except subprocess.CalledProcessError as e:
            output = str(e.output)
            print("Subprocess output is: {}".format(output))
            if "52" in output:
                print("Terminated successfully")
                return 
            else:
                print("Terminated unsuccessfully, trial: {}".format(trial))
                sleep(1)
                trial = trial + 1
                continue
        print("Seem like a good request, but we never know :)")
        return 
    print("The system has sent {} times curl cmd, but none returns successfully.".format(trial))


def is_pod_terminated(namespace:str = NAMESPACE):
    # print("abcdef")
    list_pod = list_namespaced_pod_status(namespace)
    # print("size: {}".format(len(list_pod)))
    if len(list_pod) == 0:
        return False
    i:KubernetesPod
    for i in list_pod:
        # print("Pod status is: {}".format(i.pod_status))
        if i.pod_status == "Terminating":
            return True
        else:
            return False
    return False

def get_number_pod(namespace:str = NAMESPACE):
    return len(list_namespaced_pod_status(namespace))

def get_number_running_pod(namespace:str = NAMESPACE):
    count = 0
    list_pod = list_namespaced_pod_status(namespace)
    i:KubernetesPod
    for i in list_pod:
        if i.pod_status == "Running":
            count = count + 1
    return count

def get_list_term_pod(namespace:str = NAMESPACE):
    count = 0
    list_pod = list_namespaced_pod_status(namespace)
    list_term_pod = []
    i:KubernetesPod
    for i in list_pod:
        if i.pod_status == "Terminating":
            list_term_pod.append(i)
    return list_term_pod

def update_deployment(target_pods_scale: int, image: str,  host: str, path_file_deploy: str = TEMPLATE_PATH):
#opens the capture file and updates the replica values
    try:  
        new_deployment = []
        for target_pod in range(0, target_pods_scale, 1):
            docs = list(yaml.load_all(open(path_file_deploy, "r"), Loader=yaml.SafeLoader))
            for doc in docs:
                for key, value in doc.items():
                    if value == "serving.knative.dev/v1":
                        doc["metadata"]["name"] = "detection"+str(target_pod+1)
                        doc["spec"]["template"]["spec"]["nodeSelector"]["kubernetes.io/hostname"] = str(host)
                        doc["spec"]["template"]["spec"]["containers"][0]["image"] = str(image)
                        break
                new_deployment.insert(target_pod,doc)

        with open(DEPLOYMENT_PATH, 'w') as yaml_file:
            yaml.dump_all(new_deployment, yaml_file, default_flow_style=False)
    except yaml.YAMLError as exc:
        print(exc)

def config_live_time(time: int, path_file_deploy: str = DEPLOYMENT_PATH): #, detection_image: str
    #opens the capture file and updates the replica values
    try:  
        docs = list(yaml.load_all(open(path_file_deploy, "r"), Loader=yaml.SafeLoader))
        # print("Size of docs: {}".format(len(docs)))
        for doc in docs:
            # print(doc)
            for key, value in doc.items():
                if value == "serving.knative.dev/v1":
                    doc["spec"]["template"]["metadata"]["annotations"]["autoscaling.knative.dev/window"] = str(time)+"s"
                    break
        with open(DEPLOYMENT_PATH, 'w') as yaml_file:
            yaml.dump_all(docs, yaml_file, default_flow_style=False)
    except yaml.YAMLError as exc:
        print(exc)
    print("Live time has been changed to {} seconds".format(time))
 
def config_image(image: str, path_file_deploy: str = DEPLOYMENT_PATH): #, detection_image: str
    #opens the capture file and updates the replica values
    try:  
        docs = list(yaml.load_all(open(path_file_deploy, "r"), Loader=yaml.SafeLoader))
        for doc in docs:
            for key, value in doc.items():
                if value == "serving.knative.dev/v1":
                    doc["spec"]["template"]["spec"]["containers"][0]["image"] = str(image)
                    break
        with open(DEPLOYMENT_PATH, 'w') as yaml_file:
            yaml.dump_all(docs, yaml_file, default_flow_style=False)
    except yaml.YAMLError as exc:
        print(exc)
    print("Image name has been changed")

def is_all_con_ready(namespace: str = NAMESPACE):
    a = list_namespaced_pod_status(namespace)
    count = 0
    i:KubernetesPod
    for i in a:
        if i.number_container_ready/i.sum_pod_container != 1.0:
            return False
        else:
            print("Pod {} is ready.".format(i.pod_name))
            count = count + 1
            if count == len(a):
                return True
    return False

def is_all_con_not_ready(namespace: str = NAMESPACE):
    a = list_namespaced_pod_status(namespace)
    i:KubernetesPod
    for i in a:
        if i.number_container_ready/i.sum_pod_container == 0:
            # print("No containers are ready.")
            return True
        else:
            return False
    return True
#NOTE: return an array of object endpoint
#      only working with serverless testcase
#      not true with other testcase 
def list_namespaced_endpoints(target_namespce: str = "serverless"):
    list_endpoints = []
    get_endpoint_response = ApiV1.list_namespaced_endpoints(target_namespce)
    for endpoint in get_endpoint_response.items:
        entry = {
            "endpoint_name": endpoint.metadata.name,
            "endpoints": []
        }
        list_subnet = []
        if endpoint.subsets != None:
            for subnet in endpoint.subsets:
                ips = []
                ports = []
                if subnet.addresses != None:
                    for address in subnet.addresses:
                        ips.append(address.ip)
                if subnet.ports != None:
                    for port in subnet.ports:
                        ports.append(port.port)
                address = {
                    "ip": ips,
                    "port": ports
                }
                list_subnet.append(address)
        entry["endpoints"] = list_subnet
        list_endpoints.append(entry)
    return list_endpoints

def is_endpoint_available(target_namespce: str = "serverless"):
    get_endpoint_response = ApiV1.list_namespaced_endpoints(target_namespce)
    if len(get_endpoint_response.items) != 0:
        return True
    else:
        return False

if __name__ == "__main__":
    # a = is_endpoint_available()
    # if a:
    #     print("Exist")
    # else:
    #     print("Nothing")
    # a = list_namespaced_endpoints("serverless")
    #i:KubernetesPod
    # for i in a:
    #     print(i)
        # print(i.pod_status)
        # print(i.pod_ip)
        # print("{}/{}".format(i.number_container_ready,i.sum_pod_container))
    print("Test ....")
    update_deployment(3, "Kienkauko", "jetson")
    config_live_time(20000, "./deploy.yaml")
    config_image("Kien-test", "./deploy.yaml")
    print("Finish")