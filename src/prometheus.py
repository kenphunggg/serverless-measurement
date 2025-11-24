import logging

from prometheus_api_client import PrometheusConnect

from src.lib import ClusterInfo

# with open("config/config.json", "r") as f:
#     data = json.load(f)

# PROMETHEUS = data["prometheus"]
# SERVER = PROMETHEUS["server_ip"]
# PORT = PROMETHEUS["port"]
# PROM_URL = f"http://{SERVER}:{PORT}"


class Prometheus:
    @staticmethod
    def query(query: str, prom_server: str):
        prom_client = PrometheusConnect(url=prom_server, disable_ssl=True)

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
    def queryCPU(instance: str, prom_server: str):
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

        return Prometheus.query(query=query_cpu_url, prom_server=prom_server)

    def queryGPU(instance: str, prom_server: str):
        """
        query gpu usage
        Args:
            instance (str): instance ip

        Returns:
            list: [timestamp, value]
        """
        query_gpu_url = ()
        return Prometheus.query(query=query_gpu_url, prom_server=prom_server)

    def queryMem(instance: str, prom_server: str):
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

        return Prometheus.query(query=query_mem_url, prom_server=prom_server)

    def queryNetworkIn(instance: str, cluster_info: ClusterInfo, prom_server: str):
        """
        query mem usage in (%)
        Args:
            instance (str): instance ip

        Returns:
            list: [timestamp, value]
        """
        worker_nodes = cluster_info.worker_nodes
        interface: str
        for node in worker_nodes:
            if node.ip_address == instance:
                interface = node.interface

        query_network_receive = (
            f"rate(node_network_receive_bytes_total{{device='{interface}', instance='{instance}:9100', job='node_exporters'}}[1m])"
            f" / (1024 * 1024)"
        )

        return Prometheus.query(query=query_network_receive, prom_server=prom_server)

    def queryNetworkOut(instance: str, cluster_info: ClusterInfo, prom_server: str):
        """
        query mem usage in (%)
        Args:
            instance (str): instance ip

        Returns:
            list: [timestamp, value]
        """
        worker_nodes = cluster_info.worker_nodes
        interface: str
        for node in worker_nodes:
            if node.ip_address == instance:
                interface = node.interface

        query_network_receive = (
            f"rate(node_network_transmit_bytes_total{{device='{interface}', instance='{instance}:9100', job='node_exporters'}}[1m])"
            f" / (1024 * 1024)"
        )
        return Prometheus.query(query=query_network_receive, prom_server=prom_server)

    @staticmethod
    def queryPodCPU(namespace: str, prom_server: str):
        """
        query cpu usage in (mCPU)
        Args:
            namespace (str): pod namespace
        """

        # Query for real-time CPU usage for ALL pods in the namespace
        query_cpu_url = f'sum(rate(container_cpu_usage_seconds_total{{namespace="{namespace}", container!=""}}[5m])) by (pod)'

        return Prometheus.query(query=query_cpu_url, prom_server=prom_server)

    @staticmethod
    def queryPodMemory(namespace: str, prom_server: str):
        """
        query memory usage in (MegaByte)
        Args:
            namespace (str): namespace of pod
        """

        # Query for real-time memory usage for ALL pods in the namespace
        query_memory_url = f'sum(container_memory_usage_bytes{{namespace="{namespace}", container!=""}}) by (pod)'

        return Prometheus.query(query=query_memory_url, prom_server=prom_server)

    @staticmethod
    def queryPodNetworkIn(namespace: str, prom_server: str):
        """
        query *network in* in (MBps)
        Args:
            namespace (str): pod namespace
        """

        # Query for real-time CPU usage for ALL pods in the namespace
        query_net_in_url = f'sum(rate(container_network_receive_bytes_total{{namespace="{namespace}"}}[5m])) by (pod)'

        return Prometheus.query(query=query_net_in_url, prom_server=prom_server)

    @staticmethod
    def queryPodNetworkOut(namespace: str, prom_server: str):
        """
        query *network out* in (Bps)
        Args:
            instance (str): instance ip
        """

        # Query for real-time network out for ALL pods in the namespace
        query_net_out_url = f'sum(rate(container_network_transmit_bytes_total{{namespace="{namespace}"}}[5m])) by (pod)'

        return Prometheus.query(query=query_net_out_url, prom_server=prom_server)
