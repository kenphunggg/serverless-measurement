import logging
import json
import time
import datetime
from typing import List

from src.main_tasks.web_measuring import WebMeasuring
from src.main_tasks.streaming import StreamingMeasuring
from src.lib import ClusterInfo, Node, DatabaseInfo, StreamingInfo
from src import variables as var


if __name__ == "__main__":
    logging.basicConfig(
        filename="logs/main.log",
        filemode="a",
        format="%(asctime)s,%(msecs)03d - %(name)s - %(levelname)s %(message)s",
        # format="%(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )

    logging.info("Start serverless measurement!")
    start_time = datetime.datetime.now()

    # Reload variable
    var.reload_var()

    # Load the file's content into a dictionary
    with open("config/config_streaming.json", "r") as f:
        data = json.load(f)
    test_cases = data["test_cases"]

    # Load cluster info
    cluster_info = data["cluster_info"]
    master_node = Node(
        ip_address=cluster_info["master-node"]["host_ip"],
        hostname=cluster_info["master-node"]["hostname"],
        interface=cluster_info["master-node"]["interface"],
    )
    worker_nodes: List[Node] = []
    worker_nodes_json = cluster_info["worker-nodes"]
    for node_data in worker_nodes_json:
        node = Node(
            ip_address=node_data["host_ip"],
            hostname=node_data["hostname"],
            interface=node_data["interface"],
        )
        worker_nodes.append(node)

    # Load database info
    database_info_json = data["database_info"]
    database_info = DatabaseInfo(
        host=database_info_json["db_host"],
        user=database_info_json["db_user"],
        password=database_info_json["db_password"],
    )

    streaming_info_json = data["streaming_info"]
    streaming_info = StreamingInfo(streaming_source=streaming_info_json["source_ip"])

    my_cluster = ClusterInfo(
        master_node=master_node,
        worker_nodes=worker_nodes,
        database_info=database_info,
        streaming_info=streaming_info,
    )

    logging.info(my_cluster)

    # Run all test cases
    for test_case in test_cases:
        if test_case["test_case"] == "web":
            # pass
            web_measuring = WebMeasuring(config=test_case, cluster_info=my_cluster)
            # web_measuring.baseline()
            web_measuring.get_warm_resptime()
            web_measuring.get_warm_hardware_usage()
            web_measuring.get_cold_resptime()
            # web_measuring.get_cold_hardware_usage()
            del web_measuring

        elif test_case["test_case"] == "streaming":
            # pass
            streaming_measuring = StreamingMeasuring(
                config=test_case, cluster_info=my_cluster
            )
            # streaming_measuring.baseline()
            # streaming_measuring.get_warm_timeToFirstFrame()
            # streaming_measuring.measure()
            del streaming_measuring

        elif test_case["test_case"] == "":
            pass

    end_time = datetime.datetime.now()

    measure_time = end_time - start_time

    logging.info(f"Measurement successfully, took {measure_time}")
