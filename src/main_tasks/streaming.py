import logging

import csv
import time
from threading import Thread
import matplotlib.pyplot as plt
import numpy as np

from src.lib import (
    create_timeToFirstFrame_file,
    get_curl_metrics,
    query_url,
    create_streaming_prom_file,
    create_streaming_stats_file,
    CreateResultFile,
)
from src.prometheus import Prometheus
from src import variables as var
from src.k8sAPI import K8sAPI


class StreamingMeasuring:
    def __init__(self, config):
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
        self.resolution = config["resolution"]

    def baseline(self):
        logging.info(
            "Sceanario: Collecting CPU/RAM usage of 'StreamingService' in baseline"
        )
        for rep in range(1, self.repetition + 1, 1):
            logging.info(
                f"Repeat time: {rep}/{self.repetition}, Instance: {self.hostname}"
            )

            # 1. Create result file
            result_file = CreateResultFile.streaming_baseline(
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
                network = Prometheus.queryNetwork(instance=self.host_ip)
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
                output_file=f"result/2_0_baseline/{self.hostname}/{self.arch}_{var.generate_file_time}_rep{rep}.png",
            )

    def timeToFirstFrame(self):
        logging.info(
            "Sceanario: Get 'time to first frame' of 'StreamingService' when pod in warm status"
        )
        for replica in self.replicas:
            for rep in range(1, self.repetition + 1, 1):
                logging.info(
                    f"Replicas: {replica}, Repeat time: {rep}/{self.repetition}, Instance: {self.hostname}"
                )

                # 2. Create result file
                result_file = CreateResultFile.streaming_timeToFirstFrame(
                    nodename=self.hostname,
                    filename=f"{self.arch}_{self.resolution}_{var.generate_file_time}_rep{rep}.csv",
                )

                # 3. Deploy ksvc for measuring
                K8sAPI.deploy_ksvc(
                    ksvc_name=self.ksvc_name,
                    namespace=self.namespace,
                    image=self.image,
                    port=self.port,
                    hostname=self.hostname,
                    replicas=replica,
                )

                # 4. Every 2 seconds, check if all pods in given *namespace* and *ksvc* is Running
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

                # 5. Executing curl save to data
                for _ in range(self.curl_time):
                    start_time = time.time()
                    # 5.1. Executing curl to get time to first frame
                    # Curl to the url to trigger streaming service
                    # Result of curl is:
                    # [time_namelookup,time_connect,time_appconnect,time_pretransfer,time_redirect,time_starttransfer,time_total]
                    # We only focus on time_starttransfer which is time to first frame
                    logging.info("Start trigger service")
                    result = get_curl_metrics(
                        url=f"http://{self.ksvc_name}.{self.namespace}.192.168.17.1.sslip.io/stream/start/{self.resolution}/10"
                    )

                    logging.debug(
                        f"Curl: http://{self.ksvc_name}.{self.namespace}.192.168.17.1.sslip.io/stream/start/{self.resolution}/10"
                    )
                    logging.debug(f"Trigger return {result}")

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
                    while (time.time() - start_time) < 10:
                        time.sleep(1)

                PlotResult.timeToFirstFrame(
                    result_file=result_file,
                    output_file=f"result/2_1_timeToFirstFrame/{self.hostname}/{self.arch}_{self.resolution}_{var.generate_file_time}_rep{rep}.png",
                    resolution=self.resolution,
                )

                K8sAPI.delete_ksvc(ksvc=self.ksvc_name, namespace=self.namespace)

                time.sleep(self.cool_down_time)

                logging.info(
                    "End collecting time to first frame when pod in warm status"
                )

    def measure(self):
        logging.info("Sceanario: Data of streaming service when pod in warm status")
        # 1. Load config values

        for replica in self.replicas:
            for rep in range(1, self.repetition + 1, 1):
                logging.info(
                    f"Replicas: {replica}, Repeat time: {rep}/{self.repetition}, Instance: {self.hostname}"
                )

                # 2. Create result file
                prom_file = CreateResultFile.streaming_resource(
                    nodename=self.hostname,
                    filename=f"{self.arch}_{self.resolution}_{var.generate_file_time}_rep{rep}.csv",
                )
                stream_stats_file = CreateResultFile.streaming_bitrate_fps(
                    nodename=self.hostname,
                    filename=f"{self.arch}_{self.resolution}_{var.generate_file_time}_rep{rep}.csv",
                )

                # 3. Deploy ksvc for measuring
                K8sAPI.deploy_ksvc(
                    ksvc_name=self.ksvc_name,
                    namespace=self.namespace,
                    image=self.image,
                    port=self.port,
                    hostname=self.hostname,
                    replicas=replica,
                )

                # 4. Every 2 seconds, check if all pods in given *namespace* and *ksvc* is Running
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

                # 5. Executing curl save to data
                start_time = time.time()
                # 5.1. Executing curl to get time to first frame
                # Curl to the url to trigger streaming service
                # Result of curl is:
                # [time_namelookup,time_connect,time_appconnect,time_pretransfer,time_redirect,time_starttransfer,time_total]
                # We only focus on time_starttransfer which is time to first frame
                logging.info("Start trigger service")
                result = get_curl_metrics(
                    url=f"http://{self.ksvc_name}.{self.namespace}.192.168.17.1.sslip.io/stream/start/{self.resolution}/{self.detection_time}"
                )

                logging.debug(
                    f"Curl: http://{self.ksvc_name}.{self.namespace}.192.168.17.1.sslip.io/stream/start/{self.resolution}/{self.detection_time}"
                )
                logging.debug(f"Trigger return {result}")

                # 5.2. Now we will start 2 process
                # The first one is trigger prometheus server to query cpu, memory, and network
                # The second one is trigger service to get current bitrate and fps
                # 5.2.1. Get bit rate and fps
                thread1 = Thread(
                    target=MainThread.get_bitrate_fps,
                    kwargs={
                        "detection_time": self.detection_time,
                        "ksvc_name": self.ksvc_name,
                        "namespace": self.namespace,
                        "result_file": stream_stats_file,
                    },
                )
                # 5.2.2. Trigger prometheus server
                thread2 = Thread(
                    target=MainThread.query_prometheus,
                    kwargs={
                        "host_ip": self.host_ip,
                        "detection_time": self.detection_time,
                        "result_file": prom_file,
                    },
                )
                thread1.start()
                thread2.start()
                thread1.join()
                thread2.join()

                while (time.time() - start_time) < (self.detection_time):
                    time.sleep(1)

                PlotResult.resource(
                    result_file=prom_file,
                    output_file=f"result/2_3_streaming_prom/{self.hostname}/{self.arch}_{self.resolution}_{var.generate_file_time}_rep{rep}.png",
                    resolution=self.resolution,
                )
                PlotResult.bitrate_fps(
                    result_file=stream_stats_file,
                    output_file=f"result/2_2_bitrate_fps/{self.hostname}/{self.arch}_{self.resolution}_{var.generate_file_time}_rep{rep}.png",
                    resolution=self.resolution,
                )

                K8sAPI.delete_ksvc(ksvc=self.ksvc_name, namespace=self.namespace)

                logging.info(
                    "End collecting time to first frame when pod in warm status"
                )


class MainThread:
    @staticmethod
    def query_prometheus(host_ip, detection_time, result_file):
        logging.info(
            "Start query prometheus to get hardware information when running streaming service"
        )

        start_time = time.time()
        while time.time() - start_time < detection_time:
            logging.info("Collecting prometheus metrics ...")
            cpu = Prometheus.queryCPU(instance=host_ip)
            mem = Prometheus.queryMem(instance=host_ip)
            network = Prometheus.queryNetwork(instance=host_ip)
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
            time.sleep(0.5)
        logging.info(
            "End query prometheus to get hardware information when running streaming serviced"
        )

    @staticmethod
    def get_bitrate_fps(detection_time, ksvc_name, namespace, result_file):
        logging.info("Start collecting bitrate and fps of streaming service")
        start_time = time.time()
        while time.time() - start_time < detection_time:
            logging.info("Collecting bitrate and fps ...")
            result = query_url(
                url=f"http://{ksvc_name}.{namespace}.192.168.17.1.sslip.io/stream/status"
            )
            if result["status"] == "running":
                bitrate = result["details"]["bitrate"]
                fps = result["details"]["fps"]
                logging.debug(
                    f"Stream running with a final bitrate of {bitrate} and {fps} FPS."
                )
                with open(result_file, mode="a", newline="") as f:
                    result_value = [bitrate, fps]
                    writer = csv.writer(f)
                    writer.writerow(result_value)
                logging.debug(f"successfully write {result_value} into {result_file}")
            time.sleep(0.5)

        logging.info("End collecting bitrate and fps of streaming service")


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
    def timeToFirstFrame(result_file, output_file, resolution):
        logging.info("Start plot time to first frame of streaming service")
        resp_time = []
        try:
            with open(result_file, "r", newline="") as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    if row:
                        try:
                            resp_time.append(float(row[5]) * 1000)
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

        plt.title(
            f"Distribution of time to first frame of streaming service at resolution = {resolution}",
            fontsize=16,
        )
        plt.ylabel("Time to first frame (ms)", fontsize=12)
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
    def resource(result_file, output_file, resolution):
        logging.info("Start plot hardware usage of streaming service")
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

        fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(18, 6))
        fig.suptitle(
            f"Streaming service at resolution = {resolution}",
            fontsize=16,
        )

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
        axes[2].set_ylabel("Traffic (kbps)")
        axes[2].set_xticklabels(["Network"])
        # Add this line to set tighter Y-axis limits
        # padding_net = (data[2].max() - data[2].min()) * 0.5  # 50% padding
        # axes[2].set_ylim(data[2].min() - padding_net, data[2].max() + padding_net)

        # Adjust layout to prevent titles and labels from overlapping
        plt.tight_layout()

        # Save the figure to a file
        plt.savefig(output_file)

    @staticmethod
    def bitrate_fps(result_file, output_file, resolution):
        logging.info("Start plot hardware usage of streaming service")
        bitrate_data = []
        fps_data = []
        try:
            with open(result_file, "r", newline="") as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    if row:
                        try:
                            bitrate_str = row[0]
                            if "kbits/s" in bitrate_str:
                                # It's kbit/s, so just remove the text and convert to float
                                value = float(bitrate_str.replace("kbits/s", ""))
                                bitrate_data.append(value)
                                fps_data.append(float(row[1]))

                            # elif "mbits/s" in bitrate_str:
                            #     # It's mbit/s, so remove text, convert, and multiply by 1000
                            #     value = float(bitrate_str.replace("Mbits/s", "")) * 1000
                            #     bitrate_data.append(value)
                            #     fps_data.append(float(row[1]))

                            else:
                                # Optional: Handle rows with unexpected units
                                logging.warning(
                                    f"Skipping row with unknown bitrate unit: {row}"
                                )
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

        data = [bitrate_data, fps_data]
        data = [np.array(bitrate_data), np.array(fps_data)]

        fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(18, 6))
        fig.suptitle(
            f"Streaming service at resolution = {resolution}",
            fontsize=16,
        )

        # Plot data on each subplot
        axes[0].boxplot(data[0])
        axes[0].set_title("Birate Distribution")
        axes[0].set_ylabel("Usage (kbps)")
        axes[0].set_xticklabels(["Bitrate"])
        # Add this line to set tighter Y-axis limits
        # padding_cpu = (data[0].max() - data[0].min()) * 0.5  # 50% padding
        # axes[0].set_ylim(data[0].min() - padding_cpu, data[0].max() + padding_cpu)

        axes[1].boxplot(data[1])
        axes[1].set_title("FPS Distribution")
        axes[1].set_ylabel("frame per second")
        axes[1].set_xticklabels(["FPS"])
        # Add this line to set tighter Y-axis limits
        # padding_mem = (data[1].max() - data[1].min()) * 0.5  # 50% padding
        # axes[1].set_ylim(data[1].min() - padding_mem, data[1].max() + padding_mem)

        # Adjust layout to prevent titles and labels from overlapping
        plt.tight_layout()

        # Save the figure to a file
        plt.savefig(output_file)
