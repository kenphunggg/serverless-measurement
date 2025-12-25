import logging
import re
import time
from typing import List

from kubernetes import client, config
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream

from src.lib import DatabaseInfo, StreamingInfo


class K8sAPI:
    @staticmethod
    def deploy_ksvc_web(
        ksvc_name: str,
        namespace: str,
        image: str,
        port: int,
        hostname: str,
        window_time: int,
        min_scale: int,
        max_scale: int,
        database_info: DatabaseInfo,
        cpu: int = 0,
        memory: int = 0,
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
                            "autoscaling.knative.dev/window": f"{window_time}s",
                            "autoscaling.knative.dev/min-scale": f"{min_scale}",
                            "autoscaling.knative.dev/max-scale": f"{max_scale}",
                        }
                    },
                    "spec": {
                        "containers": [
                            {
                                "image": image,
                                "ports": [{"containerPort": port}],
                                "env": [
                                    {
                                        "name": "DB_HOST",
                                        "value": database_info.host,
                                    },
                                    {
                                        "name": "DB_USER",
                                        "value": database_info.user,
                                    },
                                    {
                                        "name": "DB_PASSWORD",
                                        "value": database_info.password,
                                    },
                                ],
                            }
                        ],
                        "nodeSelector": {"kubernetes.io/hostname": hostname},
                    },
                }
            },
        }

        if cpu != 0 and memory != 0:
            resource_settings = {
                "limits": {
                    "cpu": cpu,
                    "memory": memory,
                },
            }

            yaml_description["spec"]["template"]["spec"]["containers"][0][
                "resources"
            ] = resource_settings

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
    def deploy_ksvc_streaming(
        ksvc_name: str,
        namespace: str,
        image: str,
        port: int,
        hostname: str,
        window_time: int,
        min_scale: int,
        max_scale: int,
        streaming_info: StreamingInfo,
        cpu: int = 0,
        memory: int = 0,
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
                            "autoscaling.knative.dev/window": f"{window_time}s",
                            "autoscaling.knative.dev/min-scale": f"{min_scale}",
                            "autoscaling.knative.dev/max-scale": f"{max_scale}",
                        }
                    },
                    "spec": {
                        "containers": [
                            {
                                "image": image,
                                "ports": [{"containerPort": port}],
                                "env": [
                                    {
                                        "name": "SOURCE_IP",
                                        "value": streaming_info.streaming_source,
                                    },
                                ],
                            }
                        ],
                        "nodeSelector": {"kubernetes.io/hostname": hostname},
                    },
                }
            },
        }

        if cpu != 0 and memory != 0:
            resource_settings = {
                "limits": {
                    "cpu": cpu,
                    "memory": memory,
                },
            }

            yaml_description["spec"]["template"]["spec"]["containers"][0][
                "resources"
            ] = resource_settings

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
    def deploy_ksvc_yolo(
        ksvc_name: str,
        namespace: str,
        image: str,
        port: int,
        hostname: str,
        window_time: int,
        min_scale: int,
        max_scale: int,
        rtmp_stream_url: str,
        cpu: int = 0,
        memory: int = 0,
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
                            "autoscaling.knative.dev/window": f"{window_time}s",
                            "autoscaling.knative.dev/min-scale": f"{min_scale}",
                            "autoscaling.knative.dev/max-scale": f"{max_scale}",
                        }
                    },
                    "spec": {
                        "containers": [
                            {
                                "image": image,
                                "ports": [{"containerPort": port}],
                                "env": [
                                    {
                                        "name": "RTMP_STREAM_URL",
                                        "value": rtmp_stream_url,
                                    },
                                ],
                            }
                        ],
                        "nodeSelector": {"kubernetes.io/hostname": hostname},
                    },
                }
            },
        }

        if cpu != 0 and memory != 0:
            resource_settings = {
                "limits": {
                    "cpu": cpu,
                    "memory": memory,
                },
            }

            yaml_description["spec"]["template"]["spec"]["containers"][0][
                "resources"
            ] = resource_settings

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
    def deploy_ksvc_llm(
        ksvc_name: str,
        namespace: str,
        image: str,
        port: int,
        hostname: str,
        window_time: int,
        min_scale: int,
        max_scale: int,
        timeout_seconds: int = 600,
        cpu: int = 0,
        memory: int = 0,
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
                            "autoscaling.knative.dev/window": f"{window_time}s",
                            "autoscaling.knative.dev/min-scale": f"{min_scale}",
                            "autoscaling.knative.dev/max-scale": f"{max_scale}",
                        }
                    },
                    "spec": {
                        # --- ADDED TIMEOUT FIELD ---
                        "timeoutSeconds": timeout_seconds,
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

        if cpu != 0 and memory != 0:
            resource_settings = {
                "limits": {
                    "cpu": cpu,
                    "memory": memory,
                },
            }

            yaml_description["spec"]["template"]["spec"]["containers"][0][
                "resources"
            ] = resource_settings

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
    def k8s_streaming_yaml(
        svc_name: str,
        namespace: str,
        image: str,
        flask_port: int,
        stream_port: int,
        hostname: str,
        replica: int,
        streaming_info: StreamingInfo,
        cpu: int = 0,
        memory: int = 0,
    ):
        # --- Document 1: Deployment Dictionary ---
        deployment_config = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": svc_name,
                "labels": {"app": svc_name},
                "namespace": namespace,
            },
            "spec": {
                "replicas": replica,
                "selector": {"matchLabels": {"app": svc_name}},
                "template": {
                    "metadata": {"labels": {"app": svc_name}},
                    "spec": {
                        "nodeSelector": {"kubernetes.io/hostname": hostname},
                        "containers": [
                            {
                                "name": f"{svc_name}-container",
                                "image": image,
                                "env": [
                                    {
                                        "name": "SOURCE_IP",
                                        "value": streaming_info.streaming_source,
                                    },
                                    {
                                        "name": "SCALE_VALUE",
                                        # Assuming streaming_info has a .resolution attribute
                                        "value": streaming_info.streaming_resolution,
                                    },
                                ],
                                "ports": [
                                    {"name": "api", "containerPort": flask_port},
                                    {"name": "rtmp", "containerPort": stream_port},
                                ],
                                # --- NEW: Probes added here ---
                                "readinessProbe": {
                                    "httpGet": {
                                        "path": "/stream/status",
                                        "port": "api",
                                    },
                                    "initialDelaySeconds": 15,
                                    "periodSeconds": 10,
                                },
                                "livenessProbe": {
                                    "httpGet": {
                                        "path": "/stream/status",
                                        "port": "api",
                                    },
                                    "initialDelaySeconds": 30,
                                    "periodSeconds": 20,
                                },
                            }
                        ],
                    },
                },
            },
        }

        # --- CPU and Memory Logic (Preserved) ---
        if cpu != 0 and memory != 0:
            resource_settings = {
                "limits": {
                    "cpu": f"{cpu}",  # Usually passed as millicores in int, formatted to string
                    "memory": f"{memory}",  # Usually passed as Mi in int
                },
            }
            # In case you pass raw strings, remove the f-string formatting above
            # strictly following your previous logic:
            if isinstance(cpu, int):
                resource_settings["limits"]["cpu"] = str(cpu)
            if isinstance(memory, int):
                resource_settings["limits"]["memory"] = str(memory)

            deployment_config["spec"]["template"]["spec"]["containers"][0][
                "resources"
            ] = resource_settings

        # --- Document 2: Service Dictionary ---
        service_config = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": svc_name, "namespace": namespace},
            "spec": {
                "type": "NodePort",
                "selector": {"app": svc_name},
                "ports": [
                    {
                        "name": "api",
                        "protocol": "TCP",
                        "port": flask_port,
                        "targetPort": "api",
                    },
                    {
                        "name": "rtmp",  # Updated from 'hls' to 'rtmp' to match Deployment
                        "protocol": "TCP",
                        "port": stream_port,
                        "targetPort": "rtmp",
                    },
                ],
            },
        }

        return [deployment_config, service_config]

    @staticmethod
    def deploy_k8s_streaming(
        svc_name: str,
        namespace: str,
        image: str,
        flask_port: int,
        stream_port: int,
        hostname: str,
        replica: int,
        streaming_info: StreamingInfo,
        cpu: int = 0,
        memory: int = 0,
    ):
        """
        Applies (creates or replaces) Kubernetes Deployment and Service configs.
        Uses logging for output.
        """
        # Load Kubernetes configuration from default location (~/.kube/config)
        try:
            config.load_kube_config()
            logging.debug("Kubernetes config loaded successfully.")
        except config.ConfigException as e:
            logging.critical(
                "Could not load kubeconfig. Is it configured correctly? Error: %s", e
            )
            return

        # Create API clients
        apps_v1_api = client.AppsV1Api()
        core_v1_api = client.CoreV1Api()
        logging.debug("Kubernetes API clients created.")

        # Get the configuration dictionaries from your function
        try:
            configs = K8sAPI.k8s_streaming_yaml(
                svc_name=svc_name,
                namespace=namespace,
                image=image,
                flask_port=flask_port,
                stream_port=stream_port,
                hostname=hostname,
                replica=replica,
                streaming_info=streaming_info,
                cpu=cpu,
                memory=memory,
            )
            deployment_config = configs[0]
            service_config = configs[1]
        except Exception:
            # Replaced print with logger.exception to get the full stack trace
            logging.error("Error generating K8s config dictionaries")
            return

        # --- 1. Apply Deployment ---
        try:
            # Try to read the deployment
            apps_v1_api.read_namespaced_deployment(name=svc_name, namespace=namespace)

            # If it exists, replace it
            logging.info("Deployment '%s' already exists. Replacing...", svc_name)
            api_response = apps_v1_api.replace_namespaced_deployment(
                name=svc_name, namespace=namespace, body=deployment_config
            )
            logging.info("Deployment '%s' replaced.", api_response.metadata.name)

        except ApiException as e:
            if e.status == 404:
                # If it doesn't exist (404), create it
                logging.info("Deployment '%s' not found. Creating...", svc_name)
                try:
                    api_response = apps_v1_api.create_namespaced_deployment(
                        namespace=namespace, body=deployment_config
                    )
                    logging.info("Deployment '%s' created.", api_response.metadata.name)
                except ApiException:
                    # Replaced print with logger.exception
                    logging.error(
                        "Error creating deployment '%s'. Status: %s, Reason: %s",
                        svc_name,
                        e.status,
                        e.body,
                    )
            else:
                # Other API error
                # Replaced print with logger.exception
                logging.error("Error applying deployment '%s'", svc_name)

        # --- 2. Apply Service ---
        try:
            # Try to read the service
            existing_service = core_v1_api.read_namespaced_service(
                name=svc_name, namespace=namespace
            )

            # If it exists, replace it.
            service_config["spec"]["clusterIP"] = existing_service.spec.cluster_ip
            logging.info("Service '%s' already exists. Replacing...", svc_name)
            api_response = core_v1_api.replace_namespaced_service(
                name=svc_name, namespace=namespace, body=service_config
            )
            logging.info("Service '%s' replaced.", api_response.metadata.name)

        except ApiException as e:
            if e.status == 404:
                # If it doesn't exist (404), create it
                logging.info("Service '%s' not found. Creating...", svc_name)
                try:
                    api_response = core_v1_api.create_namespaced_service(
                        namespace=namespace, body=service_config
                    )
                    logging.info("Service '%s' created.", api_response.metadata.name)
                except ApiException:
                    # Replaced print with logger.exception
                    logging.error(
                        "Error creating service '%s'. Status: %s, Reason: %s",
                        svc_name,
                        e.status,
                        e.body,
                    )
            else:
                # Other API error
                # Replaced print with logger.exception
                logging.error("Error applying service '%s'", svc_name)

    @staticmethod
    def exec_cmd_pod(
        command: List[str], namespace: str, podname: str, container_name: str
    ):
        config.load_kube_config()
        core_v1 = client.CoreV1Api()

        try:
            resp = stream(
                core_v1.connect_get_namespaced_pod_exec,
                podname,
                namespace,
                command=command,
                container=container_name,
                stderr=True,  # Include stderr in the response
                stdin=False,  # No input stream
                stdout=True,  # Include stdout in the response
                tty=False,  # Not a TTY session
            )

            # The 'resp' object here is the raw string output from stdout
            logging.debug(f"STDOUT from pod '{podname}':\n{resp}")
            return resp

        except client.ApiException as e:
            # The stream function raises an ApiException on errors.
            # The body of the exception contains stderr.
            logging.error(f"ERROR executing command {command} in pod '{podname}':")
            # In this case, the 'e.body' contains the stderr output
            logging.error(e.body)
            return e.body

    @staticmethod
    def kill_pod_process(
        namespace: str, ksvc: str, keyword: str, container_name: str = "user-container"
    ):
        pod_names = K8sAPI.get_pods(namespace=namespace, ksvc=ksvc)

        if not pod_names:
            logging.error(
                f"No pods found for ksvc '{ksvc}' in namespace '{namespace}'."
            )
            return

        pod_name = pod_names[0]

        grep_pattern = f"[{keyword[0]}]{keyword[1:]}"
        shell_command_ls = f"ps aux | grep '{grep_pattern}' | awk '{{print $2}}'"
        command_to_exec_ls = ["/bin/sh", "-c", shell_command_ls]

        pid = K8sAPI.exec_cmd_pod(
            command=command_to_exec_ls,
            namespace=namespace,
            podname=pod_name,
            container_name=container_name,
        )

        logging.info(f"Preparing to remove pid {pid} of pod {pod_name}")

        shell_command_kill = f"kill {pid}"
        command_to_exec_kill = ["/bin/sh", "-c", shell_command_kill]

        resp = K8sAPI.exec_cmd_pod(
            command=command_to_exec_kill,
            namespace=namespace,
            podname=pod_name,
            container_name=container_name,
        )

        logging.info(f"Successfully kill {keyword} | Response: {resp}")

    @staticmethod
    def get_pod_status_by_deployment(namespace: str, deployment_name: str) -> list:
        """Get pod status by namespace and deployment name
        by reading the deployment's own label selector.

        Args:
            namespace (str): namespace of finding pods
            deployment_name (str): name of the deployment

        Returns:
            list: `list` of `dict` {"name": pod.metadata.name, "status": pod.status.phase}
        """
        result = []
        try:
            # Load Kubernetes configuration from the default location
            config.load_kube_config()

            # Create API clients for Core V1 (Pods) and Apps V1 (Deployments)
            core_api = client.CoreV1Api()
            apps_api = client.AppsV1Api()

            logging.debug(
                f"Getting deployment '{deployment_name}' in namespace '{namespace}' to find its label selector..."
            )

            # 1. Get the deployment to find its matchLabels
            try:
                deployment = apps_api.read_namespaced_deployment(
                    name=deployment_name, namespace=namespace
                )
            except client.ApiException as e:
                if e.status == 404:
                    logging.error(
                        f"Deployment '{deployment_name}' not found in namespace '{namespace}'."
                    )
                    return result
                else:
                    raise  # Re-raise other API errors

            match_labels = deployment.spec.selector.match_labels
            if not match_labels:
                logging.error(
                    f"Deployment '{deployment_name}' has no 'spec.selector.match_labels' defined."
                )
                return result

            # 2. Convert match_labels dict to a label selector string
            # e.g., {'app': 'my-app', 'role': 'frontend'} -> "app=my-app,role=frontend"
            label_selector = ",".join([f"{k}={v}" for k, v in match_labels.items()])

            logging.debug(
                f"Searching for pods with selector '{label_selector}' in namespace '{namespace}'..."
            )

            # 3. List pods in the namespace using the dynamically found label selector
            pod_list = core_api.list_namespaced_pod(
                namespace=namespace, label_selector=label_selector
            )

            if not pod_list.items:
                logging.warning(
                    f"No pods found for deployment '{deployment_name}' with selector '{label_selector}'."
                )
                return result

            # 4. Process the found pods
            for pod in pod_list.items:
                pod_status = ""
                if pod.metadata.deletion_timestamp:
                    pod_status = "Terminating"
                else:
                    pod_status = pod.status.phase

                logging.debug(
                    f"Pods Found: Pod: {pod.metadata.name} | Status: {pod_status}"
                )
                pod_info = {"name": pod.metadata.name, "status": pod_status}
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
                # Determine the status just like kubectl does
                pod_status = ""
                if pod.metadata.deletion_timestamp:
                    pod_status = "Terminating"
                else:
                    pod_status = pod.status.phase

                logging.debug(
                    f"Pods Found: Pod: {pod.metadata.name} | Status: {pod_status}"
                )
                pod_info = {"name": pod.metadata.name, "status": pod_status}
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

        # while True:
        #     pods = K8sAPI.get_pod_status_by_ksvc(namespace=namespace, ksvc_name=ksvc)
        #     logging.info(
        #         f"Waiting for all pods in ksvc {ksvc}, namespace {namespace} to be deleted ..."
        #     )
        #     time.sleep(2)
        #     if not pods:
        #         logging.info(
        #             f"All pods in ksvc {ksvc}, namespace {namespace} successfully deleted from the cluster."
        #         )
        #         break

    @staticmethod
    def delete_deployment_svc(svc_name: str, namespace: str):
        """
        Deletes a Kubernetes Deployment and Service by name in a given namespace.
        Uses logging for output.

        Args:
            svc_name (str): The name of the deployment and service to delete.
            namespace (str): The namespace of the resources.
        """
        # Load Kubernetes configuration from default location (~/.kube/config)
        try:
            config.load_kube_config()
            logging.debug("Kubernetes config loaded successfully.")
        except config.ConfigException as e:
            logging.critical(
                "Could not load kubeconfig. Is it configured correctly? Error: %s", e
            )
            return

        # Create API clients
        try:
            apps_v1_api = client.AppsV1Api()
            core_v1_api = client.CoreV1Api()
            logging.debug("Kubernetes API clients created.")
        except Exception as e:
            logging.error(f"Failed to create Kubernetes API clients: {e}")
            return

        # --- 1. Delete Deployment ---
        try:
            logging.info(
                f"Attempting to delete Deployment '{svc_name}' in namespace '{namespace}'..."
            )
            apps_v1_api.delete_namespaced_deployment(
                name=svc_name,
                namespace=namespace,
                body=client.V1DeleteOptions(
                    propagation_policy="Foreground",  # Ensures dependent pods are deleted first
                    grace_period_seconds=5,
                ),
            )
            logging.info(f"Deployment '{svc_name}' delete command issued successfully.")
        except client.ApiException as e:
            if e.status == 404:
                logging.warning(
                    f"Deployment '{svc_name}' not found in namespace '{namespace}'. Nothing to delete."
                )
            else:
                logging.error(
                    f"Error deleting deployment '{svc_name}': {e.reason}",
                    exc_info=True,
                )
        except Exception as e:
            logging.error(
                f"An unexpected error occurred while deleting deployment '{svc_name}': {e}",
                exc_info=True,
            )

        # --- 2. Delete Service ---
        try:
            logging.info(
                f"Attempting to delete Service '{svc_name}' in namespace '{namespace}'..."
            )
            core_v1_api.delete_namespaced_service(name=svc_name, namespace=namespace)
            logging.info(f"Service '{svc_name}' delete command issued successfully.")
        except client.ApiException as e:
            if e.status == 404:
                logging.warning(
                    f"Service '{svc_name}' not found in namespace '{namespace}'. Nothing to delete."
                )
            else:
                logging.error(
                    f"Error deleting service '{svc_name}': {e.reason}",
                    exc_info=True,
                )
        except Exception as e:
            logging.error(
                f"An unexpected error occurred while deleting service '{svc_name}': {e}",
                exc_info=True,
            )

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
                        "Check if the service account has 'pods/log' permissions."
                    )
                # return []
            except Exception as e:
                # Handle other potential errors (e.g., regex issues, config problems)
                logging.error(f"An unexpected error occurred: {e}")
                # return []

    @staticmethod
    def get_ksvc_pod_ip(
        ksvc_name: str,
        namespace: str,
        timeout_seconds: int = 300,
        retry_interval_seconds: int = 0,
    ) -> str | None:
        """
        Continuously checks for and retrieves the IP address of a running Pod
        associated with a given Knative Service (Ksvc) until a timeout is reached.

        Args:
            ksvc_name: The name of the Knative Service (kservice).
            namespace: The namespace where the Ksvc resides.
            timeout_seconds: Maximum time (in seconds) to wait for the Pod to start.
            retry_interval_seconds: Time (in seconds) to wait between checks.

        Returns:
            The IP address (str) of a running pod, or None if the timeout expires.
        """

        # 1. Load Kubernetes Configuration
        try:
            config.load_kube_config()
        except config.ConfigException:
            logging.warning("Failed to load kubeconfig. Trying in-cluster config.")
            try:
                config.load_incluster_config()
            except config.ConfigException:
                logging.error("Failed to load any Kubernetes configuration.")
                return None

        v1_custom = client.CustomObjectsApi()
        v1_core = client.CoreV1Api()

        # --- Step 1: Get the Ksvc UID and Setup Selector ---
        try:
            # Get the Ksvc to ensure it exists and to get its UID if needed,
            # though the service name label is usually sufficient.
            v1_custom.get_namespaced_custom_object(
                group="serving.knative.dev",
                version="v1",
                plural="services",
                name=ksvc_name,
                namespace=namespace,
            )
        except ApiException as e:
            if e.status == 404:
                logging.error(
                    f"Ksvc '{ksvc_name}' not found in namespace '{namespace}'."
                )
            else:
                logging.error(f"Error retrieving Ksvc: {e}")
            return None

        # The standard Knative label selector for Pods belonging to this service
        label_selector = f"serving.knative.dev/service={ksvc_name}"

        logging.info(f"Starting watch for Pod IP (Timeout: {timeout_seconds}s)...")

        start_time = time.time()

        # --- Step 2: Retry Loop to Check Pod Status ---
        while time.time() - start_time < timeout_seconds:
            try:
                # List Pods matching the selector
                pods = v1_core.list_namespaced_pod(
                    namespace=namespace, label_selector=label_selector
                )

                for pod in pods.items:
                    # Check for the desired state: Running and has an IP address
                    if pod.status.phase == "Running" and pod.status.pod_ip:
                        pod_ip = pod.status.pod_ip
                        logging.info(
                            f"Pod '{pod.metadata.name}' is RUNNING. IP found: {pod_ip}"
                        )
                        return pod_ip

                    # Report if we found a Pod, but it's still initializing
                    if pod.status.phase in ["Pending", "ContainerCreating"]:
                        logging.warning(
                            f"Pod '{pod.metadata.name}' found but is in phase: {pod.status.phase}. Waiting..."
                        )

                # If no Pods were found, or all were non-running/non-ready
                if not pods.items:
                    logging.warning(
                        "No Pods currently found for the service. Waiting for Knative to scale up..."
                    )

            except ApiException as e:
                logging.error(f"Kubernetes API error while listing pods: {e}")
                return None  # Exit on hard API error

            # Wait for the next check
            time.sleep(retry_interval_seconds)

        # --- Step 3: Timeout ---
        logging.error(
            f"Timeout reached ({timeout_seconds}s). Could not retrieve a running Pod IP for Ksvc '{ksvc_name}'."
        )
        return None
