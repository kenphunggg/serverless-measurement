import csv
import logging
import time

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

from src import variables as var
from src.k8sAPI import K8sAPI

# import requests
from src.lib import (
    ClusterInfo,
    CreateResultFile,
    get_curl_metrics,
    query_url,
)
from src.prometheus import Prometheus


class WebMeasuring:
    def __init__(self, config, cluster_info: ClusterInfo):
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
        logging.info("Scenario: Collecting CPU/RAM usage of 'WebService' in baseline")
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
                cpu = Prometheus.queryCPU(
                    instance=self.host_ip, prom_server=self.cluster_info.prometheus_ip
                )
                mem = Prometheus.queryMem(
                    instance=self.host_ip, prom_server=self.cluster_info.prometheus_ip
                )
                networkIn = Prometheus.queryNetworkIn(
                    instance=self.host_ip,
                    cluster_info=self.cluster_info,
                    prom_server=self.cluster_info.prometheus_ip,
                )
                networkOut = Prometheus.queryNetworkOut(
                    instance=self.host_ip,
                    cluster_info=self.cluster_info,
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
                logging.debug(f"Successfully write {result_value} into {result_file}")
                logging.info("Collecting prometheus metrics successfully!")
                time.sleep(0.5)

            # 3. Plot result
            PlotResult.plot_baseline(
                result_file=result_file,
                output_file=f"result/1_0_baseline/{self.hostname}/{self.arch}_{var.generate_file_time}_rep{rep}.png",
            )

    def get_warm_resptime(self):
        logging.info("Scenario: Response time of web service when pod in warm status")

        for replica in self.replicas:
            for rep in range(1, self.repetition + 1, 1):
                for resource in self.resource_requests:
                    logging.info(
                        f"Replicas: {replica}, Repeat time: {rep}/{self.repetition}, Instance: {self.hostname}, CPU req: {resource['cpu']}, Mem req: {resource['memory']}"
                    )

                    # 1. Create result file
                    result_file = CreateResultFile.web_curl(
                        nodename=self.hostname,
                        filename=f"{self.arch}_{var.generate_file_time}_{resource['cpu']}cpu_{resource['memory']}mem_rep{rep}.csv",
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
                        url = f"http://{self.ksvc_name}.{self.namespace}/processing_time/15"
                        result = get_curl_metrics(url=url)

                        logging.debug(f"result: {result}")
                        if result:
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
                        output_file=f"result/1_1_curl/{self.hostname}/{self.arch}_{var.generate_file_time}_{resource['cpu']}cpu_{resource['memory']}mem_rep{rep}.png",
                    )

                    # 6. Delete ksvc
                    K8sAPI.delete_ksvc(ksvc=self.ksvc_name, namespace=self.namespace)
                    time.sleep(self.cool_down_time)

        logging.info(
            "End scenario: Response time of web service when pod in warm status"
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
                result_file = CreateResultFile.web_resource(
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
                    # cpu=resource["cpu"],
                    # memory=resource["memory"],
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
                        nodename=self.hostname
                    )
                    mem = Prometheus.queryPodMemory(
                        namespace=self.namespace,
                        prom_server=self.cluster_info.prometheus_ip,
                        nodename=self.hostname
                    )
                    networkIn = Prometheus.queryPodNetworkIn(
                        namespace=self.namespace,
                        prom_server=self.cluster_info.prometheus_ip,
                        nodename=self.hostname
                    )
                    networkOut = Prometheus.queryPodNetworkOut(
                        namespace=self.namespace,
                        prom_server=self.cluster_info.prometheus_ip,
                        nodename=self.hostname
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

    def get_cold_resptime(self):
        logging.info("Scenario: Response time of web service when pod in cold status")

        for replica in self.replicas:
            for rep in range(1, self.repetition + 1, 1):
                for resource in self.resource_requests:
                    logging.info(
                        f"Replicas: {replica}, Repeat time: {rep}/{self.repetition}, Instance: {self.hostname}, CPU req: {resource['cpu']}, Mem req: {resource['memory']}"
                    )

                    # 1. Create result file
                    result_file = CreateResultFile.web_curl_cold(
                        nodename=self.hostname,
                        filename=f"{self.arch}_{var.generate_file_time}_{resource['cpu']}cpu_{resource['memory']}mem_rep{rep}.csv",
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
                        url = f"http://{self.ksvc_name}.{self.namespace}/processing_time/15"
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
                        output_file=f"result/1_3_curl_cold/{self.hostname}/{self.arch}_{var.generate_file_time}_{resource['cpu']}cpu_{resource['memory']}mem_rep{rep}.png",
                    )

                    # 6. Delete ksvc
                    K8sAPI.delete_ksvc(ksvc=self.ksvc_name, namespace=self.namespace)
                    time.sleep(self.cool_down_time)

        logging.info(
            "End scenario: Response time of web service when pod in cold status"
        )


class PlotResult:
    @staticmethod
    def plot_baseline(result_file, output_file):
        """
        Reads hardware usage data from a CSV and generates a 2x2 boxplot grid
        for CPU, Memory, Network In, and Network Out.
        """
        logging.info("Start plot hardware usage - baseline of web service")
        cpu_data = []
        mem_data = []
        network_in_data = []
        network_out_data = []  # <-- Added list for Network Out data

        try:
            with open(result_file, "r", newline="") as file:
                reader = csv.reader(file)
                next(reader)  # Skip header row
                for row in reader:
                    if row:
                        try:
                            cpu_data.append(float(row[1]))
                            mem_data.append(float(row[2]))
                            network_in_data.append(float(row[3]))
                            # Read the 5th column (index 4) for Network Out
                            network_out_data.append(float(row[4]))
                        except (ValueError, IndexError) as e:
                            logging.warning(
                                f"Skipping invalid row or value: {row}. Error: {e}"
                            )
        except FileNotFoundError:
            logging.error(f"The file '{result_file}' was not found.")
            return
        except StopIteration:
            logging.error(
                f"The CSV file '{result_file}' is empty or has only a header."
            )
            return
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            return

        # Check if any data was actually loaded
        if not all([cpu_data, mem_data, network_in_data, network_out_data]):
            logging.error("One or more data series is empty. Cannot generate plot.")
            return

        # --- UPGRADED PLOTTING SECTION ---
        # Change layout to a 2x2 grid for better readability
        fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(14, 10))
        fig.suptitle(
            "Streaming Service Resource Usage",
            fontsize=18,
            fontweight="bold",
        )

        # Plot 1: CPU Usage (Top-Left)
        axes[0, 0].boxplot(cpu_data)
        axes[0, 0].set_title("CPU Usage Distribution", fontsize=14)
        axes[0, 0].set_ylabel("Usage (%)")
        axes[0, 0].set_xticklabels(["CPU"])

        # Plot 2: Memory Usage (Top-Right)
        axes[0, 1].boxplot(mem_data)
        axes[0, 1].set_title("Memory Usage Distribution", fontsize=14)
        axes[0, 1].set_ylabel("Usage (%)")
        axes[0, 1].set_xticklabels(["Memory"])

        # Plot 3: Network In Traffic (Bottom-Left)
        axes[1, 0].boxplot(network_in_data)
        axes[1, 0].set_title("Network In Traffic Distribution", fontsize=14)
        axes[1, 0].set_ylabel("Traffic (MBps)")
        axes[1, 0].set_xticklabels(["Network In"])

        # Plot 4: Network Out Traffic (Bottom-Right) - NEW PLOT
        axes[1, 1].boxplot(network_out_data)
        axes[1, 1].set_title("Network Out Traffic Distribution", fontsize=14)
        axes[1, 1].set_ylabel("Traffic (MBps)")
        axes[1, 1].set_xticklabels(["Network Out"])

        # Adjust layout and save the figure
        plt.tight_layout(
            rect=[0, 0.03, 1, 0.95]
        )  # Adjust rect to make space for suptitle
        plt.savefig(output_file)
        logging.info(f"Plot successfully saved to {output_file}")

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
        network_in_data = []
        network_out_data = []

        try:
            # NOTE: For demonstration, we'll skip reading a file and use dummy data.
            # In a real environment, the file reading block below would execute.
            with open(result_file, "r", newline="") as file:
                reader = csv.reader(file)
                next(reader)  # Skip header row
                for row in reader:
                    if row:
                        try:
                            cpu_data.append(float(row[1]))
                            mem_data.append(float(row[2]))
                            network_in_data.append(float(row[3]))
                            network_out_data.append(float(row[4]))
                        except (ValueError, IndexError) as e:
                            logging.warning(f"Skipping invalid row: {row}. Error: {e}")
        except Exception as e:
            logging.error(f"Error reading file: {e}")
            return

        if not all([cpu_data, mem_data, network_in_data, network_out_data]):
            logging.error("Data series empty.")
            return

        # --- 1. DEFINE THE FORMATTER ---
        def human_readable_format(x, pos):
            """
            Converts raw numbers to K, M, G, T units.
            x: value, pos: tick position (required by matplotlib)
            """
            if x >= 1e12:
                return f"{x * 1e-12:.1f}T"
            elif x >= 1e9:
                return f"{x * 1e-9:.1f}G"
            elif x >= 1e6:
                return f"{x * 1e-6:.1f}M"
            elif x >= 1e3:
                return f"{x * 1e-3:.1f}K"
            return f"{x:.0f}"

        # Create the formatter object for Memory and Network (Bytes/Bps)
        byte_formatter = FuncFormatter(human_readable_format)

        # --- PLOTTING ---
        fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(14, 10))
        fig.suptitle("Streaming Service Resource Usage", fontsize=18, fontweight="bold")

        # --- Plot 1: CPU Usage (Convert Core to mCPU) ---
        # Multiply all CPU data points by 1000
        cpu_mcpu_data = [c * 1000 for c in cpu_data] 
        
        axes[0, 0].boxplot(cpu_mcpu_data)
        axes[0, 0].set_title("CPU Usage Distribution", fontsize=14)
        axes[0, 0].set_ylabel("Usage (mCPU)", fontsize=12) # <-- Updated label
        axes[0, 0].set_xticklabels(["CPU"])
        # Axes will automatically scale to the multiplied values (mCPU)
        
        # --- Plot 2: Memory Usage (Apply Byte Formatter) ---
        axes[0, 1].boxplot(mem_data)
        axes[0, 1].set_title("Memory Usage Distribution", fontsize=14)
        axes[0, 1].set_ylabel("Usage (Bytes)", fontsize=12)
        axes[0, 1].set_xticklabels(["Memory"])
        axes[0, 1].yaxis.set_major_formatter(byte_formatter)  # Applied formatter
        
        # --- Plot 3: Network In (Apply Byte Formatter) ---
        axes[1, 0].boxplot(network_in_data)
        axes[1, 0].set_title("Network In Traffic Distribution", fontsize=14)
        axes[1, 0].set_ylabel("Traffic (Bps)", fontsize=12)
        axes[1, 0].set_xticklabels(["Network In"])
        axes[1, 0].yaxis.set_major_formatter(byte_formatter)  # Applied formatter
        
        # --- Plot 4: Network Out (Apply Byte Formatter) ---
        axes[1, 1].boxplot(network_out_data)
        axes[1, 1].set_title("Network Out Traffic Distribution", fontsize=14)
        axes[1, 1].set_ylabel("Traffic (Bps)", fontsize=12)
        axes[1, 1].set_xticklabels(["Network Out"])
        axes[1, 1].yaxis.set_major_formatter(byte_formatter)  # Applied formatter

        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        plt.savefig(output_file)
        logging.info(f"Plot successfully saved to {output_file}")
