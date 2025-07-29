import logging
import time
import csv

# import requests

from src.lib import get_curl_metrics, create_curl_result_file
from src.k8sAPI import K8sAPI

from src import variables as var


class WebMeasuring:
    @staticmethod
    def warm_resptime(config):
        logging.info("Sceanario: Response time of web service when pod in warm status")
        # Load config values
        repetition = config["repetition"]
        replicas = config["replicas"]
        ksvc_name = config["ksvc_name"]
        arch = config["arch"]
        image = config["image"]
        port = config["port"]
        namespace = config["namespace"]
        hostname = config["hostname"]
        cool_down_time = config["cool_down_time"]
        curl_time = config["curl_time"]

        for replica in replicas:
            for rep in range(1, repetition + 1, 1):
                logging.info(
                    f"Replicas: {replica}, Repeat time: {rep}/{repetition}, Instance: {hostname}"
                )

                result_file = f"{arch}_{var.generate_file_time}_rep{rep}.csv"
                create_curl_result_file(
                    nodename=hostname,
                    filename=result_file,
                )
                result_file = f"result/curl/{hostname}/{result_file}"

                # Deploy ksvc for measuring
                K8sAPI.deploy_ksvc(
                    ksvc_name=ksvc_name,
                    namespace=namespace,
                    image=image,
                    port=port,
                    hostname=hostname,
                )

                # Every 1 seconds, check if all pods in given *namespace* and *ksvc* is Running
                while True:
                    if K8sAPI.all_pods_ready(
                        pods=K8sAPI.get_pod_status_by_ksvc(
                            namespace=namespace, ksvc_name=ksvc_name
                        )
                    ):
                        logging.info("All pods ready!")
                        break
                    logging.debug("Waiting for pods to be ready ...")
                    time.sleep(1)

                time.sleep(cool_down_time)

                logging.info("Start collecting response time when pod in warm status")

                # Executing curl to get response time for every 1s and save to data
                for _ in range(curl_time):
                    result = get_curl_metrics(
                        url="http://measure-web.serverless.192.168.17.1.sslip.io/processing_time/15"
                    )

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

                    time.sleep(1)

                time.sleep(cool_down_time)

    @staticmethod
    def warm_hardware_usage(config):
        logging.info("Start collecting CPU/RAM usage when pod in warm status")
