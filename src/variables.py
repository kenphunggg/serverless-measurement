from datetime import datetime
import logging
import json
from typing import List, Tuple, Any

# Assuming these classes are imported from your data models module
from src.lib import Node, DatabaseInfo, StreamingInfo, PrometheusServer, ClusterInfo

generate_file_time = ""


def reload_var():
    global generate_file_time

    localdate = datetime.now()
    generate_file_time = f"d{localdate.day}m{localdate.month}y{localdate.year}_{localdate.hour}h{localdate.minute}"

def load_cluster_configuration(config_path: str) -> Tuple[Any, ClusterInfo]:
    """
    Loads configuration from a JSON file and returns test cases and the ClusterInfo object.

    Args:
        config_path (str): Path to the JSON configuration file.

    Returns:
        Tuple[Any, ClusterInfo]: A tuple containing the test_cases data and the populated ClusterInfo object.
    """
    try:
        with open(config_path, "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        logging.error(f"Configuration file not found at: {config_path}")
        raise
    except json.JSONDecodeError:
        logging.error(f"Failed to decode JSON from: {config_path}")
        raise

    # 1. Load Test Cases
    test_cases = data.get("test_cases", [])

    # 2. Load Cluster Info
    cluster_info_json = data["cluster_info"]
    
    # Master Node
    master_node = Node(
        ip_address=cluster_info_json["master-node"]["host_ip"],
        hostname=cluster_info_json["master-node"]["hostname"],
        interface=cluster_info_json["master-node"]["interface"],
    )

    # Worker Nodes
    worker_nodes: List[Node] = []
    worker_nodes_json = cluster_info_json["worker-nodes"]
    for node_data in worker_nodes_json:
        node = Node(
            ip_address=node_data["host_ip"],
            hostname=node_data["hostname"],
            interface=node_data["interface"],
        )
        worker_nodes.append(node)

    # 3. Load Database Info
    database_info_json = data["database_info"]
    database_info = DatabaseInfo(
        host=database_info_json["db_host"],
        user=database_info_json["db_user"],
        password=database_info_json["db_password"],
    )

    # 4. Load Streaming Info
    streaming_info_json = data["streaming_info"]
    streaming_info = StreamingInfo(
        streaming_source=streaming_info_json["source_ip"],
        streaming_uri=streaming_info_json["stream_uri"],
        streaming_resolution=streaming_info_json["resolution"],
    )

    # 5. Load Prometheus Info
    prom_server_json = data["prometheus"]
    prom_server = PrometheusServer(
        ip=prom_server_json["server_ip"],
        port=prom_server_json["port"],
        interface=prom_server_json["interface"],
    )

    # 6. Assemble ClusterInfo
    my_cluster = ClusterInfo(
        master_node=master_node,
        worker_nodes=worker_nodes,
        database_info=database_info,
        streaming_info=streaming_info,
        prom_server=prom_server,
    )

    logging.info(f"Cluster Configuration Loaded: {my_cluster}")

    return test_cases, my_cluster
