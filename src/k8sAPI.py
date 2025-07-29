from kubernetes import client, config
from kubernetes.client.rest import ApiException

import logging


class K8sAPI:
    @staticmethod
    def deploy_ksvc(
        ksvc_name: str,
        namespace: str,
        image: str,
        port: int,
        hostname: str,
    ):
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
                            "autoscaling.knative.dev/min-scale": "1",
                            "autoscaling.knative.dev/max-scale": "1",
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
    def get_pod_status_by_ksvc(namespace: str, ksvc_name: str):
        """
        Finds pods for a Knative service (ksvc) and prints their status.

        Args:
            namespace: The Kubernetes namespace to search in.
            ksvc_name: The name of the Knative service (ksvc).
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
        ready = True
        if pods:
            for pod in pods:
                if pod["status"] != "Running":
                    ready = False
        else:
            ready = False

        return ready
