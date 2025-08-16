import json
from prometheus_api_client import PrometheusConnect

import logging


with open("config/config.json", "r") as f:
    data = json.load(f)

PROMETHEUS = data["prometheus"]
SERVER = PROMETHEUS["server_ip"]
PORT = PROMETHEUS["port"]
INTERFACE = PROMETHEUS["interface"]
PROM_URL = f"http://{SERVER}:{PORT}"


class Prometheus:
    @staticmethod
    def query(query: str):
        prom_client = PrometheusConnect(url=PROM_URL, disable_ssl=True)

        # query = (
        #     f"100 - (avg by (instance, job)("
        #     f"irate(node_cpu_seconds_total{{mode='idle', instance='{instance}:9100'}}[1m])"
        #     f") * 100)"
        # )

        logging.debug(f"Executing PromQL query: {query}")

        try:
            # Use custom_query for complex PromQL expressions
            result = prom_client.custom_query(query=query)

            # The result will be a list. If it's empty, no data was found.
            if not result:
                logging.warning("Query returned no data.")
                return [-1, -1]

            # Extract the value, convert it to a float, and return it.
            # The value is at result[0]['value'][1]
            result = result[0]["value"]
            logging.debug(f"Successfully retrieve data {result}")
            return result

        except Exception as e:
            logging.error(f"Failed to execute query. Reason: {e}")
            return [-1, -1]

    @staticmethod
    def queryCPU(instance: str):
        """
        query cpu usage in (%)
        Args:
            instance (str): instance ip

        Returns:
            list: [timestamp, value(%)]
        """
        query_cpu_url = (
            f"100 - (avg by (instance, job)("
            f"irate(node_cpu_seconds_total{{mode='idle', instance='{instance}:9100'}}[1m])"
            f") * 100)"
        )
        return Prometheus.query(query=query_cpu_url)

    def queryGPU(instance: str):
        """
        query gpu usage
        Args:
            instance (str): instance ip

        Returns:
            list: [timestamp, value]
        """
        query_gpu_url = ()
        return Prometheus.query(query=query_gpu_url)

    def queryMem(instance: str):
        """
        query mem usage in (%)
        Args:
            instance (str): instance ip

        Returns:
            list: [timestamp, value]
        """
        query_mem_url = (
            f"((node_memory_MemTotal_bytes{{job='node_exporters', instance='{instance}:9100'}}"
            f" - node_memory_MemAvailable_bytes{{job='node_exporters', instance='{instance}:9100'}})"
            f" / node_memory_MemTotal_bytes{{job='node_exporters', instance='{instance}:9100'}})"
            f" * 100"
        )
        return Prometheus.query(query=query_mem_url)

    def queryNetwork(instance: str):
        """
        query mem usage in (%)
        Args:
            instance (str): instance ip

        Returns:
            list: [timestamp, value]
        """
        query_network_receive = (
            f"rate(node_network_receive_bytes_total{{device='{INTERFACE}', instance='{instance}:9100', job='node_exporters'}}[1m])"
            f" / (1024 * 1024)"
        )
        return Prometheus.query(query=query_network_receive)
