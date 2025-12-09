import csv
import logging
import time

import matplotlib.pyplot as plt

from src import variables as var
from src.k8sAPI import K8sAPI
from src.lib import (
    ClusterInfo,
    CreateResultFile,
    query_url,
    query_url_post_image,
)

from src.prometheus import Prometheus


class YoloMeasuring:
    def __init__(self, config, cluster_info: ClusterInfo):
        logging.info("Loading config of 'YoloMeasuring'")
        self.repetition = config["repetition"]
        self.replicas = config["replicas"]
        self.ksvc_name = config["ksvc_name"]
        self.arch = config["arch"]
        self.image = config["image"]
        self.rtmp_stream_url = config["rtmp_stream_url"]
        self.port = config["port"]
        self.namespace = config["namespace"]
        self.hostname = config["hostname"]
        self.host_ip = config["host_ip"]
        self.cool_down_time = config["cool_down_time"]
        self.curl_time = config["curl_time"]
        self.detection_time = config["detection_time"]
        self.resource_requests = config["resource_requests"]
        self.cluster_info: ClusterInfo = cluster_info

    def get_yolo_detection_warm(self):
        logging.info(
            "Scenario: Get 'Yolo Detection attributes' of 'YoloService' when pod in warm status"
        )
        for replica in self.replicas:
            for rep in range(1, self.repetition + 1, 1):
                for resource in self.resource_requests:
                    # This new loop will keep trying the *current resource* until it succeeds
                    
                    logging.info(
                        f"Replicas: {replica}, Repeat time: {rep}/{self.repetition}, Instance: {self.hostname}, CPU req: {resource['cpu']}, Mem req: {resource['memory']}"
                    )

                    # 1. Create result file
                    result_file = CreateResultFile.yolo_detection_warm(
                        nodename=self.hostname,
                        filename=f"{self.arch}_{var.generate_file_time}_{resource['cpu']}cpu_{resource['memory']}mem_rep{rep}.csv",
                    )

                    # 2. Deploy ksvc for measuring
                    window_time = 20
                    K8sAPI.deploy_ksvc_yolo(
                        ksvc_name=self.ksvc_name,
                        namespace=self.namespace,
                        image=self.image,
                        port=self.port,
                        hostname=self.hostname,
                        window_time=window_time,
                        min_scale=replica,
                        max_scale=replica,
                        rtmp_stream_url=self.rtmp_stream_url,
                        cpu=resource["cpu"],
                        memory=resource["memory"],
                    )

                    # 3. Wait for pods to be ready
                    while True:
                        if K8sAPI.all_pods_ready(
                            pods=K8sAPI.get_pod_status_by_ksvc(
                                namespace=self.namespace, ksvc_name=self.ksvc_name
                            )
                        ):
                            logging.info("All pods ready!")
                            break
                        logging.info("Waiting for pods to be ready ...")
                        time.sleep(2)

                    time.sleep(self.cool_down_time)

                    # 4. Execute ffmpeg command to receive video from source and get time to first frame
                    for i in range(self.curl_time):
                        logging.info(
                            f"Start measure response time of yolo service when pod in warm status [{i}/{self.curl_time}]"
                        )

                        start_time = time.time()
                        response = query_url_post_image(
                            url=f"http://{self.ksvc_name}.{self.namespace}/detect",
                            image_path="config/img/4k.jpg",
                        )

                        response_time = time.time() - start_time
                        response_time_ms = response_time * 1000

                        if response is None:
                            logging.error(
                                "Received no response (None) from the detection service. Check the service/network."
                            )
                            continue
                        else:
                            logging.info(response)
                            if not response["success"]:
                                logging.warning(
                                    "Fail when anaylyzing streaming using yolo service"
                                )
                                continue

                            with open(result_file, mode="a", newline="") as f:
                                result_value = [
                                    response["model_loading_time_ms"],
                                    response["model_inference_ms"],
                                    response["model_nms_ms"],
                                    response["model_preprocess_ms"],
                                    response["model_inference_ms"]
                                    + response["model_nms_ms"]
                                    + response["model_preprocess_ms"],
                                    response["total_server_time_ms"],
                                    response_time_ms,
                                ]
                                writer = csv.writer(f)
                                writer.writerow(result_value)
                                logging.info(
                                    f"Successfully write {result_value} into {result_file}"
                                )

                        time.sleep(self.cool_down_time)

                    PlotResult.response_time_warm(
                        result_file=result_file,
                        output_file=f"result/3_1_yolo_warm/{self.hostname}/{self.arch}_{var.generate_file_time}_{resource['cpu']}cpu_{resource['memory']}mem_rep{rep}.png",
                    )

                    K8sAPI.delete_ksvc(ksvc=self.ksvc_name, namespace=self.namespace)
                    time.sleep(
                        2  # Wait for API server to successfully receive delete signal
                    )

                    while True:
                        pods = K8sAPI.get_pod_status_by_ksvc(
                            namespace=self.namespace, ksvc_name=self.ksvc_name
                        )
                        logging.info(
                            f"Waiting for all pods in ksvc {self.ksvc_name}, namespace {self.namespace} to be deleted ..."
                        )
                        time.sleep(2)
                        if not pods:
                            logging.info(
                                f"All pods in ksvc {self.ksvc_name}, namespace {self.namespace} successfully deleted from the cluster."
                            )
                            break

                    time.sleep(self.cool_down_time)

                    logging.info(
                        f"End measure response time of yolo service when pod in warm status [{i}/{self.curl_time}]"
                    )

        logging.info(
            "End Scenario: Get 'Yolo Detection attributes' of 'YoloService' when pod in warm status"
        )

    def get_yolo_detection_cold(self):
        logging.info(
            "Scenario: Get 'Yolo Detection attributes' of 'YoloService' when pod in cold status"
        )
        for replica in self.replicas:
            for rep in range(1, self.repetition + 1, 1):
                for resource in self.resource_requests:
                    # This new loop will keep trying the *current resource* until it succeeds
                    logging.info(
                        f"Replicas: {replica}, Repeat time: {rep}/{self.repetition}, Instance: {self.hostname}, CPU req: {resource['cpu']}, Mem req: {resource['memory']}"
                    )

                    # 1. Create result file
                    result_file = CreateResultFile.yolo_detection_cold(
                        nodename=self.hostname,
                        filename=f"{self.arch}_{var.generate_file_time}_{resource['cpu']}cpu_{resource['memory']}mem_rep{rep}.csv",
                    )

                    # 2. Deploy ksvc for measuring
                    window_time = 20
                    K8sAPI.deploy_ksvc_yolo(
                        ksvc_name=self.ksvc_name,
                        namespace=self.namespace,
                        image=self.image,
                        port=self.port,
                        hostname=self.hostname,
                        window_time=window_time,
                        min_scale=0,
                        max_scale=replica,
                        rtmp_stream_url=self.rtmp_stream_url,
                        cpu=resource["cpu"],
                        memory=resource["memory"],
                    )

                    # 3. Wait for pods to be ready
                    while True:
                        if K8sAPI.all_pods_ready(
                            pods=K8sAPI.get_pod_status_by_ksvc(
                                namespace=self.namespace, ksvc_name=self.ksvc_name
                            )
                        ):
                            logging.info("All pods ready!")
                            break
                        logging.info("Waiting for pods to be ready ...")
                        time.sleep(2)

                    time.sleep(self.cool_down_time)

                    # 4. Execute ffmpeg command to receive video from source and get time to first frame
                    for i in range(self.curl_time):
                        logging.info(
                            f"Start measure response time of yolo service when pod in cold status [{i}/{self.curl_time}]"
                        )

                        while True:
                            if (
                                K8sAPI.get_pod_status_by_ksvc(
                                    namespace=self.namespace, ksvc_name=self.ksvc_name
                                )
                                == []
                            ):
                                logging.info("Scaled to zero!")
                                break
                            logging.info("Waiting for pods to scale to zero ...")
                            time.sleep(2)
                        time.sleep(self.cool_down_time)

                        start_time = time.time()

                        response = query_url_post_image(
                            url=f"http://{self.ksvc_name}.{self.namespace}/detect",
                            image_path="config/img/4k.jpg",
                        )

                        response_time = time.time() - start_time
                        response_time_ms = response_time * 1000

                        if response is None:
                            logging.error(
                                "Received no response (None) from the detection service. Check the service/network."
                            )
                            continue
                        else:
                            # json_response = json.loads(response)
                            # logging.info(json_response)
                            if not response["success"]:
                                logging.warning(
                                    "Fail when anaylyzing streaming using yolo service"
                                )
                                continue

                            with open(result_file, mode="a", newline="") as f:
                                result_value = [
                                    response["model_loading_time_ms"],
                                    response["model_inference_ms"],
                                    response["model_nms_ms"],
                                    response["model_preprocess_ms"],
                                    response["model_inference_ms"]
                                    + response["model_nms_ms"]
                                    + response["model_preprocess_ms"],
                                    response["total_server_time_ms"],
                                    response_time_ms,
                                ]
                                writer = csv.writer(f)
                                writer.writerow(result_value)
                                logging.info(
                                    f"Successfully write {result_value} into {result_file}"
                                )

                        time.sleep(self.cool_down_time)

                    PlotResult.response_time_cold(
                        result_file=result_file,
                        output_file=f"result/3_2_yolo_cold/{self.hostname}/{self.arch}_{var.generate_file_time}_{resource['cpu']}cpu_{resource['memory']}mem_rep{rep}.png",
                    )

                    K8sAPI.delete_ksvc(ksvc=self.ksvc_name, namespace=self.namespace)
                    time.sleep(
                        2  # Wait for API server to successfully receive delete signal
                    )

                    while True:
                        pods = K8sAPI.get_pod_status_by_ksvc(
                            namespace=self.namespace, ksvc_name=self.ksvc_name
                        )
                        logging.info(
                            f"Waiting for all pods in ksvc {self.ksvc_name}, namespace {self.namespace} to be deleted ..."
                        )
                        time.sleep(2)
                        if not pods:
                            logging.info(
                                f"All pods in ksvc {self.ksvc_name}, namespace {self.namespace} successfully deleted from the cluster."
                            )
                            break

                    time.sleep(self.cool_down_time)

                    logging.info(
                        f"End measure response time of yolo service when pod in cold status [{i}/{self.curl_time}]"
                    )

        logging.info(
            "End Scenario: Get 'Yolo Detection attributes' of 'YoloService' when pod in cold status"
        )

    def get_warm_hardware_usage(self):
        logging.info("Scenario: Collecting CPU/RAM usage when pod in warm status")

        for replica in self.replicas:
            for rep in range(1, self.repetition + 1, 1):
                # for resource in self.resource_requests:
                logging.info(
                    f"Replicas: {replica}, Repeat time: {rep}/{self.repetition}, Instance: {self.hostname}"
                )

                # 1. Create result file
                result_file = CreateResultFile.yolo_resource(
                    nodename=self.hostname,
                    filename=f"{self.arch}_{var.generate_file_time}_rep{rep}.csv",
                )

                # 2. Deploy ksvc for measuring
                K8sAPI.deploy_ksvc_web(
                    ksvc_name=self.ksvc_name,
                    namespace=self.namespace,
                    image=self.image,
                    port=self.port,
                    hostname=self.hostname,
                    window_time=100,
                    min_scale=replica,
                    max_scale=replica,
                    database_info=self.cluster_info.database_info,
                )

                # 3. Every 2 seconds, check if all pods in given *namespace* and *ksvc* is Running
                while True:
                    if K8sAPI.all_pods_ready(
                        pods=K8sAPI.get_pod_status_by_ksvc(
                            namespace=self.namespace, ksvc_name=self.ksvc_name
                        )
                    ):
                        logging.info("All pods ready!")
                        break
                    logging.info("Waiting for pods to be ready ...")
                    time.sleep(2)

                time.sleep(self.cool_down_time)

                # 4. Query url on app to trigger task then query Prometheus to get CPU and Mem usage of that action for *detection_time* seconds
                logging.info(
                    "Start query prometheus to get hardware information when running web service"
                )
                start_time = time.time()
                url = f"http://{self.ksvc_name}.{self.namespace}/list-students?duration={self.detection_time}"
                query_url(url=url)

                while time.time() - start_time < self.detection_time:
                    logging.info("Collecting prometheus metrics ...")

                    cpu = Prometheus.queryPodCPU(
                        namespace=self.namespace,
                        prom_server=self.cluster_info.prometheus_ip,
                    )
                    mem = Prometheus.queryPodMemory(
                        namespace=self.namespace,
                        prom_server=self.cluster_info.prometheus_ip,
                    )
                    networkIn = Prometheus.queryPodNetworkIn(
                        namespace=self.namespace,
                        prom_server=self.cluster_info.prometheus_ip,
                    )
                    networkOut = Prometheus.queryPodNetworkOut(
                        namespace=self.namespace,
                        prom_server=self.cluster_info.prometheus_ip,
                    )

                    with open(result_file, mode="a", newline="") as f:
                        result_value = [
                            cpu[0],
                            cpu[1],
                            mem[1],
                            networkIn[1],
                            networkOut[1],
                        ]
                        writer = csv.writer(f)
                        writer.writerow(result_value)
                    logging.debug(
                        f"Successfully write {result_value} into {result_file}"
                    )
                    logging.info("Collecting prometheus metrics successfully!")
                    time.sleep(0.5)

                # 5. Plot result
                PlotResult.plot_hardware(
                    result_file=result_file,
                    output_file=f"result/1_2_resource_web/{self.hostname}/{self.arch}_{var.generate_file_time}_rep{rep}.png",
                )

                logging.info(
                    "End query prometheus to get hardware information when running streaming serviced"
                )

                # 6. Delete ksvc
                K8sAPI.delete_ksvc(ksvc=self.ksvc_name, namespace=self.namespace)
                time.sleep(self.cool_down_time)

        logging.info("End scenario: Collecting CPU/RAM usage when pod in warm status")


class PlotResult:
    @staticmethod
    def response_time_warm(result_file, output_file):
        logging.info("Start plot response time of yolo service when pod in warm status")
        resp_time = []
        proc_time = []
        try:
            with open(result_file, "r", newline="") as file:
                reader = csv.reader(file)
                next(reader)  # Skip header
                for row in reader:
                    if row:
                        try:
                            # row[5] = total response time
                            # row[4] = inference processing time
                            resp_time.append(float(row[5]))
                            proc_time.append(float(row[4]))
                        except (ValueError, IndexError):
                            logging.warning(
                                f"Warning: Skipping invalid row or value: {row}"
                            )
        except FileNotFoundError:
            logging.error(f"Error: The file '{result_file}' was not found.")
            return
        except StopIteration:
            logging.error(
                f"Error: The CSV file '{result_file}' is empty or contains only a header."
            )
            return
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            return

        if not resp_time or not proc_time:
            logging.error("No valid data of response time was found to plot.")
            return

        # --- Create and customize the box plot (using template's style) ---
        try:
            data_to_plot = [resp_time, proc_time]

            fig = plt.figure(figsize=(10, 7))
            ax = fig.add_axes([0, 0, 1, 1])
            ax.boxplot(data_to_plot)

            plt.title(
                "Distribution of Response Time (Warm Pod)",
                fontsize=16,
            )
            plt.ylabel("Time (ms)", fontsize=12)

            # Set custom labels for the x-axis to identify the two boxes
            ax.set_xticklabels(
                ["Total Response Time", "Total Processing Time"], fontsize=12
            )

            # Use the grid style from the template
            plt.grid(True, axis="y", linestyle="--", alpha=0.7)

            # --- Save the plot to an image file (using template's style) ---
            plt.savefig(output_file, dpi=300, bbox_inches="tight")
            logging.info(f"Successfully plotted and saved box plot to {output_file}")

        except Exception as e:
            logging.error(f"Error saving plot: {e}")
        finally:
            plt.close()  # Ensure the plot is closed to free memory

    @staticmethod
    def response_time_cold(result_file, output_file):
        logging.info("Start plot response time of yolo service when pod in warm status")
        resp_time = []
        # proc_time = []  <-- REMOVED

        try:
            with open(result_file, "r", newline="") as file:
                reader = csv.reader(file)
                next(reader)  # Skip header
                for row in reader:
                    if row:
                        try:
                            # row[0] = total response time (was row[5] in your comment)
                            # row[4] = inference processing time
                            resp_time.append(float(row[0]))
                            # proc_time.append(float(row[4]))  <-- REMOVED
                        except (ValueError, IndexError):
                            logging.warning(
                                f"Warning: Skipping invalid row or value: {row}"
                            )
        except FileNotFoundError:
            logging.error(f"Error: The file '{result_file}' was not found.")
            return
        except StopIteration:
            logging.error(
                f"Error: The CSV file '{result_file}' is empty or contains only a header."
            )
            return
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            return

        # --- FIX 3: Simplified check for only resp_time ---
        if not resp_time:
            logging.error("No valid data for response time was found to plot.")
            return

        # --- Create and customize the box plot (using template's style) ---
        try:
            # --- FIX 4: Plot only resp_time ---
            data_to_plot = [resp_time]

            fig = plt.figure(figsize=(10, 7))
            ax = fig.add_axes([0, 0, 1, 1])

            # This will now create ONE box plot
            ax.boxplot(data_to_plot)

            plt.title(
                "Distribution of Response Time (Warm Pod)",
                fontsize=16,
            )
            plt.ylabel("Time (ms)", fontsize=12)

            # Now the 1 label matches the 1 dataset
            ax.set_xticklabels(["Total Response Time"], fontsize=12)

            # Use the grid style from the template
            plt.grid(True, axis="y", linestyle="--", alpha=0.7)

            # --- Save the plot to an image file (using template's style) ---
            plt.savefig(output_file, dpi=300, bbox_inches="tight")
            logging.info(f"Successfully plotted and saved box plot to {output_file}")

        except Exception as e:
            logging.error(f"Error saving plot: {e}")
        finally:
            plt.close()  # Ensure the plot is closed to free memory
