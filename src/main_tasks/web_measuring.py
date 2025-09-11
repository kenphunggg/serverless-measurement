import logging
import time
import csv
import matplotlib.pyplot as plt
import numpy as np


# import requests

from src.lib import (
    get_curl_metrics,
    CreateResultFile,
    create_curl_result_file,
    create_resource_web_result_file,
    query_url,
    ClusterInfo,
    Node,
)
from src.k8sAPI import K8sAPI
from src import variables as var
from src.prometheus import Prometheus


class WebMeasuring:
    def __init__(self, config, cluster_info: ClusterInfo):
        """Get response time of web service using curl when pod in warm state(physical pod already exist)

        Args:
            config (dict): Config that you loaded that have the save format as in `/config/config.json`
        """
        logging.info("Loading config of 'WebMeasuring'")
        self.repetition = config["repetition"]
        self.replicas = config["replicas"]
        self.ksvc_name = config["ksvc_name"]
        self.arch = config["arch"]
        self.image = config["image"]
        self.port = config["port"]
        self.namespace = config["namespace"]
        self.hostname = config["hostname"]
        self.host_ip = config["host_ip"]
        self.cool_down_time = config["cool_down_time"]
        self.curl_time = config["curl_time"]
        self.detection_time = config["detection_time"]
        self.resource_requests = config["resource_requests"]
        self.cluster_info: ClusterInfo = cluster_info

    def baseline(self):
        logging.info("Sceanario: Collecting CPU/RAM usage of 'WebService' in baseline")
        for rep in range(1, self.repetition + 1, 1):
            logging.info(
                f"Repeat time: {rep}/{self.repetition}, Instance: {self.hostname}"
            )

            # 1. Create result file
            result_file = CreateResultFile.web_baseline(
                nodename=self.hostname,
                filename=f"{self.arch}_{var.generate_file_time}_rep{rep}.csv",
            )

            logging.info("Start query prometheus to get baseline hardware information")

            # 2. Query Prometheus to get CPU and Mem usage of that action for *detection_time* seconds
            start_time = time.time()
            while time.time() - start_time < self.detection_time:
                logging.info("Collecting prometheus metrics ...")
                cpu = Prometheus.queryCPU(instance=self.host_ip)
                mem = Prometheus.queryMem(instance=self.host_ip)
                network = Prometheus.queryNetwork(
                    instance=self.host_ip, cluster_info=self.cluster_info
                )
                with open(result_file, mode="a", newline="") as f:
                    result_value = [
                        cpu[0],
                        cpu[1],
                        mem[1],
                        network[1],
                    ]
                    writer = csv.writer(f)
                    writer.writerow(result_value)
                logging.debug(f"Successfully write {result_value} into {result_file}")
                logging.info("Collecting prometheus metrics successfully!")
                time.sleep(0.5)

            # 3. Plot result
            PlotResult.plot_baseline(
                result_file=result_file,
                output_file=f"result/1_0_baseline/{self.hostname}/{self.arch}_{var.generate_file_time}_rep{rep}.png",
            )

    def get_warm_resptime(self):
        logging.info("Sceanario: Response time of web service when pod in warm status")

        for replica in self.replicas:
            for rep in range(1, self.repetition + 1, 1):
                for resource in self.resource_requests:
                    logging.info(
                        f"Replicas: {replica}, Repeat time: {rep}/{self.repetition}, Instance: {self.hostname}, CPU req: {resource["cpu"]}, Mem req: {resource["memory"]}"
                    )

                    # 1. Create result file
                    result_file = CreateResultFile.web_curl(
                        nodename=self.hostname,
                        filename=f"{self.arch}_{var.generate_file_time}_{resource["cpu"]}cpu_{resource["memory"]}mem_rep{rep}.csv",
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
                        cpu=resource["cpu"],
                        memory=resource["memory"],
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

                    logging.info(
                        "Start collecting response time when pod in warm status"
                    )

                    # 4. Executing curl to get response time for every 2s and save to data
                    for _ in range(self.curl_time):
                        # Query random data and get response time
                        url = f"http://{self.ksvc_name}.{self.namespace}.192.168.17.1.sslip.io/processing_time/15"
                        result = get_curl_metrics(url=url)

                        logging.debug(f"result: {result}")
                        with open(result_file, mode="a", newline="") as f:
                            result_value = [
                                result["time_namelookup"],
                                result["time_connect"],
                                result["time_appconnect"],
                                result["time_pretransfer"],
                                result["time_redirect"],
                                result["time_starttransfer"],
                                result["time_total"],
                            ]
                            writer = csv.writer(f)
                            writer.writerow(result_value)
                        logging.debug(
                            f"Successfully write {result_value} into {result_file}"
                        )
                        time.sleep(2)

                    logging.info("End collecting response time when pod in warm status")

                    # 5. Plot result
                    PlotResult.plot_respt(
                        result_file=result_file,
                        output_file=f"result/1_1_curl/{self.hostname}/{self.arch}_{var.generate_file_time}_{resource["cpu"]}cpu_{resource["memory"]}mem_rep{rep}.png",
                    )

                    # 6. Delete ksvc
                    K8sAPI.delete_ksvc(ksvc=self.ksvc_name, namespace=self.namespace)
                    time.sleep(self.cool_down_time)

        logging.info(
            "End sceanario: Response time of web service when pod in warm status"
        )

    def get_warm_hardware_usage(self):
        logging.info("Sceanario: Collecting CPU/RAM usage when pod in warm status")

        for replica in self.replicas:
            for rep in range(1, self.repetition + 1, 1):
                for resource in self.resource_requests:
                    logging.info(
                        f"Replicas: {replica}, Repeat time: {rep}/{self.repetition}, Instance: {self.hostname}, CPU req: {resource["cpu"]}, Mem req: {resource["memory"]}"
                    )

                    # 1. Create result file
                    result_file = CreateResultFile.web_resource(
                        nodename=self.hostname,
                        filename=f"{self.arch}_{var.generate_file_time}_{resource["cpu"]}cpu_{resource["memory"]}mem_rep{rep}.csv",
                    )

                    # 2. Deploy ksvc for measuring
                    K8sAPI.deploy_ksvc_web(
                        ksvc_name=self.ksvc_name,
                        namespace=self.namespace,
                        image=self.image,
                        port=self.port,
                        hostname=self.hostname,
                        window_time=100,
                        min_scale=0,
                        max_scale=replica,
                        database_info=self.cluster_info.database_info,
                        cpu=resource["cpu"],
                        memory=resource["memory"],
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
                    url = f"http://{self.ksvc_name}.{self.namespace}.192.168.17.1.sslip.io/list-students?duration={self.detection_time}"
                    query_url(url=url)

                    while time.time() - start_time < self.detection_time:
                        logging.info("Collecting prometheus metrics ...")
                        cpu = Prometheus.queryCPU(instance=self.host_ip)
                        mem = Prometheus.queryMem(instance=self.host_ip)
                        network = Prometheus.queryNetwork(
                            instance=self.host_ip, cluster_info=self.cluster_info
                        )
                        with open(result_file, mode="a", newline="") as f:
                            result_value = [
                                cpu[0],
                                cpu[1],
                                mem[1],
                                network[1],
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
                        output_file=f"result/1_2_resource_web/{self.hostname}/{self.arch}_{var.generate_file_time}_{resource["cpu"]}cpu_{resource["memory"]}mem_rep{rep}.png",
                    )

                    logging.info(
                        "End query prometheus to get hardware information when running streaming serviced"
                    )

                    # 6. Delete ksvc
                    K8sAPI.delete_ksvc(ksvc=self.ksvc_name, namespace=self.namespace)
                    time.sleep(self.cool_down_time)

        logging.info("End sceanario: Collecting CPU/RAM usage when pod in warm status")

    def get_cold_resptime(self):
        logging.info("Sceanario: Response time of web service when pod in cold status")

        for replica in self.replicas:
            for rep in range(1, self.repetition + 1, 1):
                for resource in self.resource_requests:
                    logging.info(
                        f"Replicas: {replica}, Repeat time: {rep}/{self.repetition}, Instance: {self.hostname}, CPU req: {resource["cpu"]}, Mem req: {resource["memory"]}"
                    )

                    # 1. Create result file
                    result_file = CreateResultFile.web_curl_cold(
                        nodename=self.hostname,
                        filename=f"{self.arch}_{var.generate_file_time}_{resource["cpu"]}cpu_{resource["memory"]}mem_rep{rep}.csv",
                    )

                    # 2. Deploy ksvc for measuring
                    K8sAPI.deploy_ksvc_web(
                        ksvc_name=self.ksvc_name,
                        namespace=self.namespace,
                        image=self.image,
                        port=self.port,
                        hostname=self.hostname,
                        window_time=self.cool_down_time,
                        min_scale=0,
                        max_scale=replica,
                        database_info=self.cluster_info.database_info,
                        cpu=resource["cpu"],
                        memory=resource["memory"],
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

                    time.sleep(self.cool_down_time + 10)

                    # Manual shut down user-container
                    # When using autoscaling to scale down the pod, it take too long to completely shutdown the pod

                    # Wait for all pods scale to zero
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

                    logging.info(
                        "Start collecting response time when pod in cold status"
                    )

                    time.sleep(self.cool_down_time)

                    # 4. Executing curl to get response time for every 2s and save to data
                    for current in range(self.curl_time):
                        # Query random data and get response time
                        url = f"http://{self.ksvc_name}.{self.namespace}.192.168.17.1.sslip.io/processing_time/15"
                        result = get_curl_metrics(url=url)

                        logging.debug(f"result: {result}")
                        with open(result_file, mode="a", newline="") as f:
                            result_value = [
                                result["time_namelookup"],
                                result["time_connect"],
                                result["time_appconnect"],
                                result["time_pretransfer"],
                                result["time_redirect"],
                                result["time_starttransfer"],
                                result["time_total"],
                            ]
                            writer = csv.writer(f)
                            writer.writerow(result_value)
                        logging.debug(
                            f"Successfully write {result_value} into {result_file} | {current}/{self.curl_time}"
                        )

                        # Wait for all pods scale to zero
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

                    logging.info("End collecting response time when pod in warm status")

                    # 5. Plot result
                    PlotResult.plot_respt(
                        result_file=result_file,
                        output_file=f"result/1_3_curl_cold/{self.hostname}/{self.arch}_{var.generate_file_time}_{resource["cpu"]}cpu_{resource["memory"]}mem_rep{rep}.png",
                    )

                    # 6. Delete ksvc
                    K8sAPI.delete_ksvc(ksvc=self.ksvc_name, namespace=self.namespace)
                    time.sleep(self.cool_down_time)

        logging.info(
            "End sceanario: Response time of web service when pod in cold status"
        )

    def get_cold_hardware_usage(self):
        pass


class PlotResult:
    @staticmethod
    def plot_baseline(result_file, output_file):
        logging.info("Start plot baseline hardware usage")
        cpu_data = []
        mem_data = []
        network_data = []
        try:
            with open(result_file, "r", newline="") as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    if row:
                        try:
                            cpu_data.append(float(row[1]))
                            mem_data.append(float(row[2]))
                            network_data.append(float(row[3]))
                        except (ValueError, IndexError):
                            logging.warning(
                                f"Warning: Skipping invalid row or value: {row}"
                            )
        except FileNotFoundError:
            logging.error(f"Error: The file '{result_file}' was not found.")
            return
        except StopIteration:
            # This happens if the CSV is empty or only has a header
            logging.error(
                f"Error: The CSV file '{result_file}' is empty or contains only a header."
            )
            return
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            return

        data = [cpu_data, mem_data, network_data]
        data = [np.array(cpu_data), np.array(mem_data), np.array(network_data)]
        labels = ["CPU (%)", "Memory (%)", "Network (MBps)"]
        plt.title("Hardware usage for Web service", fontsize=16)

        fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(18, 6))

        # Plot data on each subplot
        axes[0].boxplot(data[0])
        axes[0].set_title("CPU Usage Distribution")
        axes[0].set_ylabel("Usage (%)")
        axes[0].set_xticklabels(["CPU"])
        # Add this line to set tighter Y-axis limits
        # padding_cpu = (data[0].max() - data[0].min()) * 0.5  # 50% padding
        # axes[0].set_ylim(data[0].min() - padding_cpu, data[0].max() + padding_cpu)

        axes[1].boxplot(data[1])
        axes[1].set_title("Memory Usage Distribution")
        axes[1].set_ylabel("Usage (%)")
        axes[1].set_xticklabels(["Memory"])
        # Add this line to set tighter Y-axis limits
        # padding_mem = (data[1].max() - data[1].min()) * 0.5  # 50% padding
        # axes[1].set_ylim(data[1].min() - padding_mem, data[1].max() + padding_mem)

        axes[2].boxplot(data[2])
        axes[2].set_title("Network Traffic Distribution")
        axes[2].set_ylabel("Traffic (MBps)")
        axes[2].set_xticklabels(["Network"])
        # Add this line to set tighter Y-axis limits
        # padding_net = (data[2].max() - data[2].min()) * 0.5  # 50% padding
        # axes[2].set_ylim(data[2].min() - padding_net, data[2].max() + padding_net)

        # Adjust layout to prevent titles and labels from overlapping
        plt.tight_layout()

        # Save the figure to a file
        plt.savefig(output_file)

    @staticmethod
    def plot_respt(result_file, output_file):
        logging.info("Start plot response time of web service")
        resp_time = []
        try:
            with open(result_file, "r", newline="") as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    if row:
                        try:
                            resp_time.append(float(row[6]) * 1000)
                        except (ValueError, IndexError):
                            logging.warning(
                                f"Warning: Skipping invalid row or value: {row}"
                            )
        except FileNotFoundError:
            logging.error(f"Error: The file '{result_file}' was not found.")
            return
        except StopIteration:
            # This happens if the CSV is empty or only has a header
            logging.error(
                f"Error: The CSV file '{result_file}' is empty or contains only a header."
            )
            return
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            return

        if not resp_time:
            logging.error("No valid data was found to plot.")
            return

        # --- Create and customize the box plot ---
        fig = plt.figure(figsize=(10, 7))
        ax = fig.add_axes([0, 0, 1, 1])
        ax.boxplot(resp_time)

        plt.title("Distribution of Response Times for Web service", fontsize=16)
        plt.ylabel("Response Time (ms)", fontsize=12)
        # plt.xticks([])
        # plt.grid(True, axis="y", linestyle="--", alpha=0.7)

        # --- Save the plot to an image file ---
        try:
            plt.savefig(output_file, dpi=300, bbox_inches="tight")
        except Exception as e:
            logging.error(f"Error saving plot: {e}")
        finally:
            plt.close()  # Ensure the plot is closed to free memory

    @staticmethod
    def plot_hardware(result_file, output_file):
        logging.info("Start plot hardware usage of web service")
        cpu_data = []
        mem_data = []
        network_data = []
        try:
            with open(result_file, "r", newline="") as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    if row:
                        try:
                            cpu_data.append(float(row[1]))
                            mem_data.append(float(row[2]))
                            network_data.append(float(row[3]))
                        except (ValueError, IndexError):
                            logging.warning(
                                f"Warning: Skipping invalid row or value: {row}"
                            )
        except FileNotFoundError:
            logging.error(f"Error: The file '{result_file}' was not found.")
            return
        except StopIteration:
            # This happens if the CSV is empty or only has a header
            logging.error(
                f"Error: The CSV file '{result_file}' is empty or contains only a header."
            )
            return
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            return

        data = [cpu_data, mem_data, network_data]
        data = [np.array(cpu_data), np.array(mem_data), np.array(network_data)]
        labels = ["CPU (%)", "Memory (%)", "Network (MBps)"]
        plt.title("Hardware usage for Web service", fontsize=16)

        fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(18, 6))

        # Plot data on each subplot
        axes[0].boxplot(data[0])
        axes[0].set_title("CPU Usage Distribution")
        axes[0].set_ylabel("Usage (%)")
        axes[0].set_xticklabels(["CPU"])
        # Add this line to set tighter Y-axis limits
        # padding_cpu = (data[0].max() - data[0].min()) * 0.5  # 50% padding
        # axes[0].set_ylim(data[0].min() - padding_cpu, data[0].max() + padding_cpu)

        axes[1].boxplot(data[1])
        axes[1].set_title("Memory Usage Distribution")
        axes[1].set_ylabel("Usage (%)")
        axes[1].set_xticklabels(["Memory"])
        # Add this line to set tighter Y-axis limits
        # padding_mem = (data[1].max() - data[1].min()) * 0.5  # 50% padding
        # axes[1].set_ylim(data[1].min() - padding_mem, data[1].max() + padding_mem)

        axes[2].boxplot(data[2])
        axes[2].set_title("Network Traffic Distribution")
        axes[2].set_ylabel("Traffic (MBps)")
        axes[2].set_xticklabels(["Network"])
        # Add this line to set tighter Y-axis limits
        # padding_net = (data[2].max() - data[2].min()) * 0.5  # 50% padding
        # axes[2].set_ylim(data[2].min() - padding_net, data[2].max() + padding_net)

        # Adjust layout to prevent titles and labels from overlapping
        plt.tight_layout()

        # Save the figure to a file
        plt.savefig(output_file)
