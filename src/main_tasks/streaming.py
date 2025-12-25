import csv
import logging
import subprocess
import time
from threading import Thread
import pandas as pd

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import FuncFormatter

from src import variables as var
from src.k8sAPI import K8sAPI
from src.lib import (
    ClusterInfo,
    CreateResultFile,
    get_fps_bitrate,
    get_time_to_first_frame,
    get_time_to_first_frame_warm,
    query_url,
    KnativePinger,
)
from src.prometheus import Prometheus


class StreamingMeasuring:
    def __init__(self, config, cluster_info: ClusterInfo):
        logging.info("Loading config of 'StreamingMeasuring'")
        self.repetition = config["repetition"]
        self.replicas = config["replicas"]
        self.ksvc_name = config["ksvc_name"]
        self.arch = config["arch"]
        self.image = config["image"]
        self.k8s_image = config["k8s_image"]
        self.port = config["port"]
        self.flask_port = config["flask_port"]
        self.namespace = config["namespace"]
        self.hostname = config["hostname"]
        self.host_ip = config["host_ip"]
        self.cool_down_time = config["cool_down_time"]
        self.curl_time = config["curl_time"]
        self.detection_time = config["detection_time"]
        self.resource_requests = config["resource_requests"]
        self.cluster_info: ClusterInfo = cluster_info

    def baseline(self):
        logging.info(
            "Scenario: Collecting CPU/RAM usage of 'StreamingService' in baseline"
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
                    result_value = [cpu[0], cpu[1], mem[1], networkIn[1], networkOut[1]]
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

    def get_warm_timeToFirstFrame(self):
        logging.info(
            "Scenario: Get 'time to first frame' of 'StreamingService' when pod in warm status"
        )
        for replica in self.replicas:
            for rep in range(1, self.repetition + 1, 1):
                for resource in self.resource_requests:
                    logging.info(
                        f"Replicas: {replica}, Repeat time: {rep}/{self.repetition}, Instance: {self.hostname}, CPU req: {resource['cpu']}, Mem req: {resource['memory']}"
                    )

                    # 1. Create result file
                    result_file = CreateResultFile.streaming_timeToFirstFrame(
                        nodename=self.hostname,
                        filename=f"{self.arch}_{var.generate_file_time}_{resource['cpu']}cpu_{resource['memory']}mem_rep{rep}.csv",
                    )

                    # 2. Deploy ksvc for measuring
                    K8sAPI.deploy_k8s_streaming(
                        svc_name=self.ksvc_name,
                        namespace=self.namespace,
                        image=self.k8s_image,
                        flask_port=self.flask_port,
                        stream_port=self.port,
                        hostname=self.hostname,
                        replica=replica,
                        streaming_info=self.cluster_info.streaming_info,
                        cpu=resource["cpu"],
                        memory=resource["memory"],
                    )

                    # 3. Every 2 seconds, check if all pods in given *namespace* and *ksvc* is Running
                    while True:
                        if K8sAPI.all_pods_ready(
                            pods=K8sAPI.get_pod_status_by_deployment(
                                namespace=self.namespace, deployment_name=self.ksvc_name
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
                            f"Start catching streaming service [{i + 1}/{self.curl_time}]"
                        )

                        time_to_first_frame = 0
                        starttime = time.monotonic()

                        start_stream = query_url(
                            url=f"http://{self.ksvc_name}.{self.namespace}:{self.flask_port}/stream/start"
                        )
                        if start_stream is None:
                            logging.warning(
                                "Error when starting stream, start deleting svc and try again"
                            )
                            K8sAPI.delete_deployment_svc(
                                svc_name=self.ksvc_name, namespace=self.namespace
                            )

                            while True:
                                pods = K8sAPI.get_pod_status_by_deployment(
                                    namespace=self.namespace,
                                    deployment_name=self.ksvc_name,
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

                            K8sAPI.deploy_k8s_streaming(
                                svc_name=self.ksvc_name,
                                namespace=self.namespace,
                                image=self.k8s_image,
                                flask_port=self.flask_port,
                                stream_port=self.port,
                                hostname=self.hostname,
                                replica=replica,
                                streaming_info=self.cluster_info.streaming_info,
                                cpu=resource["cpu"],
                                memory=resource["memory"],
                            )

                            # 3. Every 2 seconds, check if all pods in given *namespace* and *ksvc* is Running
                            while True:
                                if K8sAPI.all_pods_ready(
                                    pods=K8sAPI.get_pod_status_by_deployment(
                                        namespace=self.namespace,
                                        deployment_name=self.ksvc_name,
                                    )
                                ):
                                    logging.info("All pods ready!")
                                    break
                                logging.info("Waiting for pods to be ready ...")
                                time.sleep(2)
                            time.sleep(self.cool_down_time)
                            i -= 1
                            continue

                        response = get_time_to_first_frame_warm(
                            url=f"rtmp://{self.ksvc_name}.{self.namespace}:{self.port}/{self.cluster_info.streaming_info.streaming_uri}"
                        )
                        time_to_first_frame = time.monotonic() - starttime

                        if response:
                            with open(result_file, mode="a", newline="") as f:
                                writer = csv.writer(f)
                                writer.writerow([time_to_first_frame])
                                logging.info(
                                    f"Successfully write {time_to_first_frame} into {result_file}"
                                )

                        time.sleep(2)

                        query_url(
                            url=f"http://{self.ksvc_name}.{self.namespace}:5000/stream/stop"
                        )

                        time.sleep(2)

                    PlotResult.timeToFirstFrame(
                        result_file=result_file,
                        output_file=f"result/2_1_timeToFirstFrame/{self.hostname}/{self.arch}_{var.generate_file_time}_{resource['cpu']}cpu_{resource['memory']}mem_rep{rep}.png",
                    )

                    K8sAPI.delete_deployment_svc(
                        svc_name=self.ksvc_name, namespace=self.namespace
                    )

                    # Wait for API server to successfully receive delete signal
                    time.sleep(2)

                    while True:
                        pods = K8sAPI.get_pod_status_by_deployment(
                            namespace=self.namespace, deployment_name=self.ksvc_name
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
                        "End collecting time to first frame when pod in warm status"
                    )

        logging.info(
            "End Scenario: Get 'time to first frame' of 'StreamingService' when pod in warm status"
        )

    def get_fps(self):
        logging.info(
            "Scenario: Measure fps of streaming service when pod in warm status"
        )

        for replica in self.replicas:
            for rep in range(1, self.repetition + 1, 1):
                for resource in self.resource_requests:
                    logging.info(
                        f"Replicas: {replica}, Repeat time: {rep}/{self.repetition}, Instance: {self.hostname}, CPU req: {resource['cpu']}, Mem req: {resource['memory']}"
                    )

                    # 1. Create result file
                    stream_stats_file = CreateResultFile.streaming_bitrate_fps(
                        nodename=self.hostname,
                        filename=f"{self.arch}_{var.generate_file_time}_{resource['cpu']}cpu_{resource['memory']}mem_rep{rep}.csv",
                    )

                    # 2. Deploy ksvc for measuring
                    K8sAPI.deploy_ksvc_streaming(
                        ksvc_name=self.ksvc_name,
                        namespace=self.namespace,
                        image=self.image,
                        port=self.port,
                        hostname=self.hostname,
                        window_time=100,
                        min_scale=replica,
                        max_scale=replica,
                        streaming_info=self.cluster_info.streaming_info,
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

                    pod_ip = K8sAPI.get_ksvc_pod_ip(
                        ksvc_name=self.ksvc_name, namespace=self.namespace
                    )

                    # 4. Execute ffmpeg command to receive video from source and get fps
                    logging.info("Start catching fps/bitrate of streaming service")
                    # 4.1. Unpack the two lists returned by the updated function
                    fps_list, bitrate_list = get_fps_bitrate(
                        stream_url=f"rtmp://{pod_ip}:1935/live/stream",
                        sample_needed=self.curl_time * 10,
                    )

                    # 4.2. Log summary statistics (optional, but good for debugging)
                    if fps_list:
                        avg_fps = sum(fps_list) / len(fps_list)
                        avg_bitrate = (
                            sum(bitrate_list) / len(bitrate_list)
                            if bitrate_list
                            else 0.0
                        )
                        logging.info(f"Captured {len(fps_list)} samples.")
                        logging.info(
                            f"Stats -> Avg FPS: {avg_fps:.2f} | Avg Bitrate: {avg_bitrate:.0f} kbits/s"
                        )
                    else:
                        logging.warning("No samples were captured from the stream.")

                    # 4.3. Write to CSV
                    # This will write two columns: [FPS, Bitrate]
                    if fps_list:
                        with open(stream_stats_file, "a", newline="") as f:
                            writer = csv.writer(f)

                            # 'zip' pairs the two lists together so you can write them in the same row
                            for fps, bitrate in zip(fps_list, bitrate_list):
                                writer.writerow([fps, bitrate])

                        logging.info(f"Successfully saved data to {stream_stats_file}")

                    logging.info(f"Successfully saved data to {stream_stats_file}")

                    PlotResult.bitrate_fps(
                        result_file=stream_stats_file,
                        output_file=f"result/2_2_bitrate_fps/{self.hostname}/{self.arch}_{var.generate_file_time}_{resource['cpu']}cpu_{resource['memory']}mem_rep{rep}.png",
                    )

                    K8sAPI.delete_ksvc(ksvc=self.ksvc_name, namespace=self.namespace)

                    K8sAPI.kill_pod_process(
                        namespace=self.namespace, ksvc=self.ksvc_name, keyword="ffmpeg"
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
                    "End collecting time to first frame when pod in warm status"
                )

    def get_hardware_resource(self):
        logging.info(
            "Scenario: Measure hardware resource of streaming service when pod in warm status"
        )

        for replica in self.replicas:
            for rep in range(1, self.repetition + 1, 1):
                logging.info(
                    f"Replicas: {replica}, Repeat time: {rep}/{self.repetition}, Instance: {self.hostname}"
                )

                # 1. Create result file
                stream_resource_file = CreateResultFile.streaming_resource(
                    nodename=self.hostname,
                    filename=f"{self.arch}_{var.generate_file_time}_rep{rep}.csv",
                )

                # 2. Deploy ksvc for measuring
                K8sAPI.deploy_ksvc_streaming(
                    ksvc_name=self.ksvc_name,
                    namespace=self.namespace,
                    image=self.image,
                    port=self.port,
                    hostname=self.hostname,
                    window_time=100,
                    min_scale=replica,
                    max_scale=replica,
                    streaming_info=self.cluster_info.streaming_info,
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

                thread1 = Thread(
                    target=GetHardWareUsage.run_ffmpeg_for_duration,
                    kwargs={
                        "url": f"http://{self.ksvc_name}.{self.namespace}/{self.cluster_info.streaming_info.streaming_uri}",
                        "duration_sec": self.detection_time,
                    },
                )

                thread2 = Thread(
                    target=GetHardWareUsage.query_prometheus,
                    kwargs={
                        "namespace": self.namespace,
                        "detection_time": self.detection_time,
                        "cluster_info": self.cluster_info,
                        "result_file": stream_resource_file,
                        "nodename": self.hostname,
                    },
                )

                thread1.start()
                thread2.start()
                thread1.join()
                thread2.join()

                logging.info(
                    "End query prometheus to get hardware information when running streaming serviced"
                )

                PlotResult.resource(
                    result_file=stream_resource_file,
                    output_file=f"result/2_3_streaming_prom/{self.hostname}/{self.arch}_{var.generate_file_time}_rep{rep}.png",
                )

                K8sAPI.delete_ksvc(ksvc=self.ksvc_name, namespace=self.namespace)

                K8sAPI.kill_pod_process(
                    namespace=self.namespace, ksvc=self.ksvc_name, keyword="ffmpeg"
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

        logging.info("End collecting resource usage when pod in warm status")

    def get_cold_timeToFirstFrame(self):
        logging.info(
            "Scenario: Get 'time to first frame' of 'StreamingService' when pod in cold status"
        )
        for replica in self.replicas:
            for rep in range(1, self.repetition + 1, 1):
                for resource in self.resource_requests:
                    logging.info(
                        f"Replicas: {replica}, Repeat time: {rep}/{self.repetition}, Instance: {self.hostname}, CPU req: {resource['cpu']}, Mem req: {resource['memory']}"
                    )

                    # 1. Create result file
                    result_file = CreateResultFile.streaming_timeToFirstFrame_cold(
                        nodename=self.hostname,
                        filename=f"{self.arch}_{var.generate_file_time}_{resource['cpu']}cpu_{resource['memory']}mem_rep{rep}.csv",
                    )

                    # 2. Deploy ksvc for measuring
                    window_time = 20  # After window time, if no traffic, scale to zero
                    K8sAPI.deploy_ksvc_streaming(
                        ksvc_name=self.ksvc_name,
                        namespace=self.namespace,
                        image=self.image,
                        port=self.port,
                        hostname=self.hostname,
                        window_time=window_time,
                        min_scale=0,
                        max_scale=replica,
                        streaming_info=self.cluster_info.streaming_info,
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

                    # 4. Execute ffmpeg command to receive video from source and get time to first frame
                    for i in range(self.curl_time):
                        logging.info(
                            f"Start catching streaming service [{i+1}/{self.curl_time}]"
                        )

                        # 5. Waiting for all pods to scale to zero
                        while True:
                            terminating = False
                            pods = K8sAPI.get_pod_status_by_ksvc(
                                namespace=self.namespace, ksvc_name=self.ksvc_name
                            )
                            if pods:
                                for pod in pods:
                                    if pod["status"] == "Terminating":
                                        terminating = True
                            if terminating:
                                logging.info("Changing to *Terminating* state")
                                time.sleep(2)
                                break

                            time.sleep(2)

                        # After window time, scale down to zero. However, Knative's *queue-proxy* cannot kill ffmpeg proccess.
                        # This lead to huge scale down time.
                        # Following will fix that
                        K8sAPI.kill_pod_process(
                            namespace=self.namespace,
                            ksvc=self.ksvc_name,
                            keyword="ffmpeg",
                        )

                        # 4. Every 2 seconds, check if pod is scaled to zero
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

                        time.sleep(2)

                        pinger = KnativePinger(
                            url=f"http://{self.ksvc_name}.{self.namespace}.svc.cluster.local"
                        )
                        pinger.start()
                        starttime = time.time()
                        pod_ip = K8sAPI.get_ksvc_pod_ip(
                            ksvc_name=self.ksvc_name, namespace=self.namespace
                        )

                        time_to_first_frame = get_time_to_first_frame(
                            url=f"rtmp://{pod_ip}:1935/live/stream"
                        )

                        if time_to_first_frame:
                            with open(result_file, mode="a", newline="") as f:
                                writer = csv.writer(f)
                                writer.writerow([time.time() - starttime])
                                logging.debug(
                                    f"Successfully write {time_to_first_frame} into {result_file}"
                                )

                        pinger.stop()

                    PlotResult.timeToFirstFrame(
                        result_file=result_file,
                        output_file=f"result/2_4_timeToFirstFrame_cold/{self.hostname}/{self.arch}_{var.generate_file_time}_{resource['cpu']}cpu_{resource['memory']}mem_rep{rep}.png",
                    )

                    K8sAPI.delete_ksvc(ksvc=self.ksvc_name, namespace=self.namespace)
                    time.sleep(
                        2  # Wait for API server to successfully receive delete signal
                    )
                    K8sAPI.kill_pod_process(
                        namespace=self.namespace, ksvc=self.ksvc_name, keyword="ffmpeg"
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
                        "End collecting time to first frame when pod in cold status"
                    )

        logging.info(
            "End Scenario: Get 'time to first frame' of 'StreamingService' when pod in warm status"
        )


class GetHardWareUsage:
    @staticmethod
    def query_prometheus(
        namespace, detection_time, cluster_info, result_file, nodename
    ):
        logging.info(
            "Start query prometheus to get hardware information when running streaming service"
        )

        start_time = time.time()

        while time.time() - start_time < detection_time:
            logging.info("Collecting prometheus metrics ...")
            cpu = Prometheus.queryPodCPU(
                namespace=namespace,
                prom_server=cluster_info.prometheus_ip,
                nodename=nodename,
            )
            mem = Prometheus.queryPodMemory(
                namespace=namespace,
                prom_server=cluster_info.prometheus_ip,
                nodename=nodename,
            )
            networkIn = Prometheus.queryPodNetworkIn(
                namespace=namespace,
                prom_server=cluster_info.prometheus_ip,
                nodename=nodename,
            )
            networkOut = Prometheus.queryPodNetworkOut(
                namespace=namespace,
                prom_server=cluster_info.prometheus_ip,
                nodename=nodename,
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
            time.sleep(0.5)

    @staticmethod
    def run_ffmpeg_for_duration(url: str, duration_sec: int):
        """
        Runs an ffmpeg command to process a stream for a specific duration.

        This function constructs and executes an ffmpeg command that reads from the
        given URL, discards the output, and runs for a specified number of seconds.
        It captures and logs the command's output (stdout and stderr).

        Args:
            url (str): The input URL for the ffmpeg command (e.g., a video stream).
            duration_sec (int): The duration in seconds for which the command should run.

        Returns:
            bool: True if the command executed successfully, False otherwise.
        """
        if not isinstance(duration_sec, int) or duration_sec <= 0:
            logging.error("Duration must be a positive integer.")
            return False

        # Construct the ffmpeg command.
        # -i {url}: Specifies the input URL.
        # -t {duration_sec}: Sets the duration for the process.
        # -f null -: Instructs ffmpeg to discard all output, useful for testing or analysis.
        command = [
            "ffmpeg",
            "-i",
            url,
            "-t",
            str(duration_sec),
            "-f",
            "null",  # Discard the frame data
            "-",
        ]

        logging.info("Executing command: %s", command)

        try:
            # Use shlex.split to handle shell-like quoting safely.

            # Execute the command.
            # capture_output=True captures stdout and stderr.
            # text=True decodes them as text.
            # check=True raises a CalledProcessError if the command returns a non-zero exit code.
            result = subprocess.run(
                command, check=True, capture_output=True, text=True, encoding="utf-8"
            )

            logging.info("--- FFmpeg Command Successful ---")
            # FFmpeg often prints progress and info to stderr.
            logging.debug(
                "FFmpeg Output (stderr):\n%s",
                result.stderr if result.stderr.strip() else "No standard error output.",
            )
            if result.stdout and result.stdout.strip():
                logging.info("FFmpeg Output (stdout):\n%s", result.stdout)
            return True

        except FileNotFoundError:
            logging.error(
                "ffmpeg command not found. Please ensure FFmpeg is installed and accessible in your system's PATH."
            )
            return False
        except subprocess.CalledProcessError as e:
            logging.error(
                "--- FFmpeg command failed with exit code %s ---", e.returncode
            )
            logging.error(
                "FFmpeg Output (stderr):\n%s",
                e.stderr if e.stderr.strip() else "No standard error output.",
            )
            if e.stdout and e.stdout.strip():
                logging.error("FFmpeg Output (stdout):\n%s", e.stdout)
            return False
        except Exception:
            logging.exception("An unexpected error occurred while running ffmpeg.")
            return False


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
        axes[0, 0].set_ylabel("Usage (mCPU)")
        axes[0, 0].set_xticklabels(["CPU"])

        # Plot 2: Memory Usage (Top-Right)
        axes[0, 1].boxplot(mem_data)
        axes[0, 1].set_title("Memory Usage Distribution", fontsize=14)
        axes[0, 1].set_ylabel("Usage (MB)")
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
    def timeToFirstFrame(result_file, output_file):
        logging.info("Start plot time to first frame of streaming service")
        resp_time = []
        try:
            with open(result_file, "r", newline="") as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    if row:
                        try:
                            resp_time.append(float(row[0]) * 1000)
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
            "Distribution of time to first frame of streaming service",
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
    def resource(result_file, output_file):
        logging.info("Start plot hardware usage of streaming service")
        cpu_data = []
        mem_data = []
        network_in_data = []
        network_out_data = []

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

        # Create the formatter object
        formatter = FuncFormatter(human_readable_format)

        # --- PLOTTING ---
        fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(14, 10))
        fig.suptitle("Streaming Service Resource Usage", fontsize=18, fontweight="bold")

        # Plot 1: CPU Usage (Usually kept as raw numbers for Cores)
        axes[0, 0].boxplot(cpu_data)
        axes[0, 0].set_title("CPU Usage Distribution", fontsize=14)
        axes[0, 0].set_ylabel("Usage (Core)")
        axes[0, 0].set_xticklabels(["CPU"])

        # Plot 2: Memory Usage (Apply Formatter)
        axes[0, 1].boxplot(mem_data)
        axes[0, 1].set_title("Memory Usage Distribution", fontsize=14)
        axes[0, 1].set_ylabel("Usage (Bytes)")
        axes[0, 1].set_xticklabels(["Memory"])
        axes[0, 1].yaxis.set_major_formatter(formatter)  # <--- Applied here

        # Plot 3: Network In (Apply Formatter)
        axes[1, 0].boxplot(network_in_data)
        axes[1, 0].set_title("Network In Traffic Distribution", fontsize=14)
        axes[1, 0].set_ylabel("Traffic (Bps)")
        axes[1, 0].set_xticklabels(["Network In"])
        axes[1, 0].yaxis.set_major_formatter(formatter)  # <--- Applied here

        # Plot 4: Network Out (Apply Formatter)
        axes[1, 1].boxplot(network_out_data)
        axes[1, 1].set_title("Network Out Traffic Distribution", fontsize=14)
        axes[1, 1].set_ylabel("Traffic (Bps)")
        axes[1, 1].set_xticklabels(["Network Out"])
        axes[1, 1].yaxis.set_major_formatter(formatter)  # <--- Applied here

        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        plt.savefig(output_file)
        logging.info(f"Plot successfully saved to {output_file}")

    @staticmethod
    def old_resource(result_file, output_file, resolution):
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
    def bitrate_fps(result_file: str, output_file: str):
        """
        Reads FPS and Bitrate data from a CSV file and generates side-by-side
        Box Plots to visualize the distribution of both metrics.

        Args:
            result_file: The path to the CSV file containing the data.
            output_file: The path to save the generated plot image.
        """
        logging.info("Start plotting FPS and Bitrate distribution (Box Plots)")

        try:
            # Use pandas to read the CSV
            df = pd.read_csv(result_file, header=None, names=["FPS", "Bitrate"])

            if df.empty:
                logging.error(
                    f"Error: The CSV file '{result_file}' is empty or contains no data rows."
                )
                return

            # Ensure data is numerical and clean up
            df["FPS"] = pd.to_numeric(df["FPS"], errors="coerce")
            df["Bitrate"] = pd.to_numeric(df["Bitrate"], errors="coerce")
            df.dropna(inplace=True)

            if df.empty:
                logging.error(
                    "Error: All data rows were invalid or non-numeric after cleaning."
                )
                return

            fps_data = df["FPS"].values
            bitrate_data = df["Bitrate"].values

        except FileNotFoundError:
            logging.error(f"Error: The file '{result_file}' was not found.")
            return
        except Exception as e:
            logging.error(f"An error occurred while reading the CSV: {e}")
            return

        # --- Plotting ---

        # Create figure and two subplots (1 row, 2 columns) for side-by-side comparison
        fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(12, 6))
        fig.suptitle("Distribution of FPS and Bitrate Metrics", fontsize=16)

        # --- Plot 1: FPS Box Plot ---
        axes[0].boxplot(
            [fps_data], patch_artist=True, boxprops=dict(facecolor="tab:blue")
        )
        axes[0].set_title("FPS Distribution")
        axes[0].set_ylabel("Frames Per Second")
        axes[0].set_xticklabels(["FPS"])
        axes[0].grid(axis="y", linestyle="--", alpha=0.7)

        # Add mean FPS line
        mean_fps = np.mean(fps_data)
        axes[0].axhline(
            mean_fps,
            color="black",
            linestyle=":",
            linewidth=1.5,
            label=f"Mean: {mean_fps:.2f}",
        )
        axes[0].legend(loc="lower left")

        # --- Plot 2: Bitrate Box Plot ---
        axes[1].boxplot(
            [bitrate_data], patch_artist=True, boxprops=dict(facecolor="tab:red")
        )
        axes[1].set_title("Bitrate Distribution")
        axes[1].set_ylabel("Bitrate (kbits/s)")
        axes[1].set_xticklabels(["Bitrate"])
        axes[1].grid(axis="y", linestyle="--", alpha=0.7)

        # Add mean Bitrate line
        mean_bitrate = np.mean(bitrate_data)
        axes[1].axhline(
            mean_bitrate,
            color="black",
            linestyle=":",
            linewidth=1.5,
            label=f"Mean: {mean_bitrate:.2f}",
        )
        axes[1].legend(loc="lower left")

        # Adjust layout to prevent titles and labels from overlapping
        plt.tight_layout(rect=[0, 0, 1, 0.96])

        # Save the figure to a file
        plt.savefig(output_file)
        logging.info(f"Plot saved to {output_file}")
