from kubernetes import client, config
from kubernetes.client.rest import ApiException

import logging
import time
import re


class K8sAPI:
    @staticmethod
    def deploy_ksvc(
        ksvc_name: str,
        namespace: str,
        image: str,
        port: int,
        hostname: str,
        replicas: int,
    ):
        """Deploy ksvc(knative service) using given parameters

        Args:
            ksvc_name (str): name of ksvc
            namespace (str): namespace of your ksvc
            image (str): your image to deploy ksvc
            port (int): port your application run on
            hostname (str): name of the host that application will run on
            replicas (int): total number of replicas that you want for your app
        """
        yaml_description = {
            "apiVersion": "serving.knative.dev/v1",
            "kind": "Service",
            "metadata": {
                "name": ksvc_name,
                "namespace": namespace,
            },
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "autoscaling.knative.dev/window": "100s",
                            "autoscaling.knative.dev/min-scale": f"{replicas}",
                            "autoscaling.knative.dev/max-scale": f"{replicas}",
                        }
                    },
                    "spec": {
                        "containers": [
                            {
                                "image": image,
                                "ports": [{"containerPort": port}],
                            }
                        ],
                        "nodeSelector": {"kubernetes.io/hostname": hostname},
                    },
                }
            },
        }

        # Load Kubernetes config and define API parameters

        config.load_kube_config()
        api = client.CustomObjectsApi()
        group = "serving.knative.dev"
        version = "v1"
        plural = "services"

        # --- Apply the configuration to the cluster ---
        try:
            # Check if the object already exists
            api.get_namespaced_custom_object(
                group, version, namespace, plural, ksvc_name
            )
            # If it exists, patch it
            logging.info(f"Knative Service '{ksvc_name}' already exists. Patching...")
            api.patch_namespaced_custom_object(
                group, version, namespace, plural, ksvc_name, yaml_description
            )
            logging.info(f"Knative Service '{ksvc_name}' patched successfully.")
        except ApiException as e:
            if e.status == 404:
                # If it doesn't exist, create it
                logging.info(f"Knative Service '{ksvc_name}' not found. Creating...")
                api.create_namespaced_custom_object(
                    group, version, namespace, plural, yaml_description
                )
                logging.info(f"Knative Service '{ksvc_name}' created successfully.")
            else:
                logging.error(f"API Error for '{ksvc_name}': {e}")

    @staticmethod
    def get_pod_status_by_ksvc(namespace: str, ksvc_name: str) -> list:
        """Get pod status by namespace and ksvc

        Args:
            namespace (str): namespace of finding pods
            ksvc_name (str): name of ksvc

        Returns:
            list: `list` of `dict` {`pod.metadata.name`: `pod.status.phase`}
        """
        result = []
        try:
            # Load Kubernetes configuration from the default location (e.g., ~/.kube/config)
            config.load_kube_config()

            # Create an API client for the Core V1 API
            api = client.CoreV1Api()

            # Define the label selector to find pods managed by the specified ksvc
            label_selector = f"serving.knative.dev/service={ksvc_name}"

            logging.debug(
                f"Searching for pod from ksvc '{ksvc_name}' in namespace '{namespace}'..."
            )

            # List pods in the namespace using the label selector
            pod_list = api.list_namespaced_pod(
                namespace=namespace, label_selector=label_selector
            )

            if not pod_list.items:
                logging.warning(f"No pods found for ksvc '{ksvc_name}'.")
                return result

            # Print the status for each found pod
            for pod in pod_list.items:
                logging.debug(
                    f"Pods Found: Pod: {pod.metadata.name} | Status: {pod.status.phase}"
                )
                pod_info = {"name": pod.metadata.name, "status": pod.status.phase}
                result.append(pod_info)

        except config.ConfigException:
            logging.error(
                "Could not load Kubernetes configuration. Ensure your kubeconfig file is correctly set up."
            )
        except client.ApiException as e:
            logging.error(f"An API error occurred: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")

        return result

    @staticmethod
    def all_pods_ready(pods):
        """_summary_

        Args:
            pods (dict): a `dict` that describe pod status {`pod.metadata.name`: `pod.status.phase`}

        Returns:
            bool: if all pods ready, return true
        """
        ready = True
        if pods:
            for pod in pods:
                if pod["status"] != "Running":
                    ready = False
        else:
            ready = False

        return ready

    @staticmethod
    def delete_ksvc(ksvc: str, namespace: str):
        """
        Deletes a Knative Service (ksvc) in a specific namespace.
        """
        config.load_kube_config()
        api = client.CustomObjectsApi()

        group = "serving.knative.dev"
        version = "v1"
        plural = "services"

        logging.info(
            f"Attempting to delete Knative Service '{ksvc}' in namespace '{namespace}'..."
        )

        try:
            api.delete_namespaced_custom_object(
                group=group,
                version=version,
                name=ksvc,
                namespace=namespace,
                plural=plural,
                body=client.V1DeleteOptions(),
            )
            logging.info(f"service.serving.knative.dev '{ksvc}' deleted")

        except ApiException as e:
            if e.status == 404:
                logging.warning(
                    f"Knative Service '{ksvc}' not found in namespace '{namespace}'. Nothing to delete."
                )
            else:
                logging.error(f"Error deleting Knative Service '{ksvc}': {e.reason}")

        while True:
            pods = K8sAPI.get_pod_status_by_ksvc(namespace=namespace, ksvc_name=ksvc)
            logging.info(
                f"Waiting for all pods in ksvc {ksvc}, namespace {namespace} to be deleted ..."
            )
            time.sleep(2)
            if not pods:
                logging.info(
                    f"All pods in ksvc {ksvc}, namespace {namespace} successfully deleted from the cluster."
                )
                break

    @staticmethod
    def get_pods(namespace: str, ksvc: str):
        """
        Finds and logs the names of pods in a given namespace
        that contain a specific substring in their name.

        Args:
            namespace (str): The Kubernetes namespace to search in.
            ksvc_name_substring (str): The substring to search for in the pod names.
        """

        try:
            # Load Kubernetes configuration from default location (e.g., ~/.kube/config)
            config.load_kube_config()

            # Create an instance of the CoreV1Api client
            v1 = client.CoreV1Api()

            logging.info(
                f"Searching for pods in namespace '{namespace}' containing '{ksvc}'..."
            )

            # List all pods in the specified namespace
            pod_list = v1.list_namespaced_pod(namespace)

            # Filter the pod list in Python (the 'grep' part)
            matching_pods = [
                pod.metadata.name for pod in pod_list.items if ksvc in pod.metadata.name
            ]

            if matching_pods:
                logging.info(f"Found matching pods: {matching_pods}")
            else:
                logging.info("No matching pods found.")

            return matching_pods

        except client.ApiException as e:
            logging.error(f"Error connecting to Kubernetes API: {e}")
            return []
        except FileNotFoundError:
            logging.error(
                "Could not find Kubernetes configuration file. Ensure 'kubectl' is configured."
            )
            return []

    @staticmethod
    def get_stats_from_logs(pod_name: str, namespace: str = "default"):
        """
        Retrieves FFmpeg FPS and bitrate readings by reading logs directly from the Kubernetes API.

        This is the recommended programmatic approach as it interacts directly with the
        Kubernetes control plane and doesn't rely on the `kubectl` CLI tool.

        Args:
            pod_name (str): The name of the pod to get logs from.
            namespace (str, optional): The Kubernetes namespace where the pod is running.
                                    Defaults to 'default'.

        Returns:
            List[Tuple[float, float]]: A list of tuples, where each tuple contains
                                    (fps, bitrate_in_kbits). Returns an empty
                                    list if no readings are found or an error occurs.
        """
        logging.info(
            f"Attempting to get logs for pod '{pod_name}' in namespace '{namespace}' via Kubernetes API..."
        )

        while True:
            try:
                config.load_kube_config()

                # Create an instance of the CoreV1Api client
                api_instance = client.CoreV1Api()

                # Fetch the logs from the specified pod
                # This call is the direct equivalent of `kubectl logs <pod_name> -n <namespace>`
                log_output = api_instance.read_namespaced_pod_log(
                    name=pod_name, namespace=namespace, container="user-container"
                )

                # This regex finds lines containing both "fps=" and "bitrate=",
                # and captures the numeric value for each. The pattern is designed
                # to be flexible and handle other text between the two values.
                # - Group 1: ([\d.]+) captures the FPS value.
                # - Group 2: ([\d.]+) captures the bitrate value.
                stats_regex = re.compile(r"fps=\s*([\d.]+).*?bitrate=\s*([\d.]+)")

                # .findall() returns a list of all matching groups. Since our pattern
                # has two capture groups, it returns a list of tuples.
                # e.g., [('24.0', '1519.2'), ('24.0', '1520.1'), ...]
                readings = stats_regex.findall(log_output)

                if not readings:
                    logging.info(
                        "Log analysis complete. No FPS/bitrate status lines found."
                    )
                    return []

                # Convert the list of string tuples to a list of float tuples for easier use.
                return [(float(fps), float(bitrate)) for fps, bitrate in readings]

            except client.ApiException as e:
                # Handle specific Kubernetes API errors (e.g., pod not found, permissions error)
                logging.error(
                    f"[ERROR] Kubernetes API error occurred: {e.status} - {e.reason}"
                )
                time.sleep(2)
                if e.status == 404:
                    logging.error(
                        f"Check if pod '{pod_name}' exists in namespace '{namespace}'."
                    )
                elif e.status == 403:
                    logging.error(
                        f"Check if the service account has 'pods/log' permissions."
                    )
                # return []
            except Exception as e:
                # Handle other potential errors (e.g., regex issues, config problems)
                logging.error(f"An unexpected error occurred: {e}")
                # return []
