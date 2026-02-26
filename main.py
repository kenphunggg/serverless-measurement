import datetime
import logging

from src import variables as var
from src.main_tasks.streaming import StreamingMeasuring
from src.main_tasks.web_measuring import WebMeasuring
from src.main_tasks.yolo import YoloMeasuring
from src.main_tasks.llm import LLMMeasuring

# ---------- Preset Test Config ----------
TEST_WEB_CONFIG_PATH = "config/config_web.json"
TEST_STREAMING_CONFIG_PATH = "config/config_streaming.json"
TEST_YOLO_CONFIG_PATH = "config/config_yolo.json"
TEST_LLM_CONFIG_PATH = "config/config_llm.json"

# ---------- Preset Germany Config ----------
GERM_WEB_CONFIG_CLOUD_PATH = "config/germ/config_web_cloud.json"
GERM_WEB_CONFIG_EDGE_PATH = "config/germ/config_web_edge.json"

GERM_STREAMING_CONFIG_CLOUD_PATH = "config/germ/config_streaming_cloud.json"
GERM_STREAMING_CONFIG_EDGE_PATH = "config/germ/config_streaming_edge.json"

GERM_YOLO_CONFIG_CLOUD_PATH = "config/germ/config_yolo_cloud.json"
GERM_YOLO_CONFIG_EDGE_PATH = "config/germ/config_yolo_edge.json"

GERM_LLM_CONFIG_CLOUD_PATH = "config/germ/config_llm_cloud.json"
GERM_LLM_CONFIG_EDGE_PATH = "config/germ/config_llm_edge.json"


# ---------- CONFIG FILE ----------
CONFIG_FILE = TEST_YOLO_CONFIG_PATH

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
    test_cases, my_cluster = var.load_cluster_configuration(config_path=CONFIG_FILE)

    # Run all test cases
    for test_case in test_cases:
        if test_case["test_case"] == "web":
            web_measuring = WebMeasuring(config=test_case, cluster_info=my_cluster)
            # web_measuring.baseline()
            # web_measuring.get_warm_resptime()
            # web_measuring.get_warm_hardware_usage()
            # web_measuring.get_cold_resptime()
            # web_measuring.get_cold_analysis()
            # web_measuring.get_cold_resptime_cpuBoost()
            del web_measuring

        elif test_case["test_case"] == "streaming":
            streaming_measuring = StreamingMeasuring(
                config=test_case, cluster_info=my_cluster
            )
            # streaming_measuring.baseline()
            # streaming_measuring.get_warm_timeToFirstFrame()
            streaming_measuring.get_fps()
            # streaming_measuring.get_hardware_resource()
            # streaming_measuring.get_cold_timeToFirstFrame()
            del streaming_measuring

        elif test_case["test_case"] == "yolo":
            yolo_measuring = YoloMeasuring(config=test_case, cluster_info=my_cluster)
            # yolo_measuring.get_yolo_detection_warm()
            yolo_measuring.get_yolo_detection_cold()
            # yolo_measuring.get_warm_hardware_usage()
            yolo_measuring.get_yolo_detection_cold_CPUBoost()
            del yolo_measuring

        elif test_case["test_case"] == "llm":
            llm_measuring = LLMMeasuring(config=test_case, cluster_info=my_cluster)
            # llm_measuring.get_text2text_warm()
            # llm_measuring.get_text2image_warm()
            llm_measuring.get_model_loadingtime()
            llm_measuring.get_model_loadingtime_CPUBoost()
            del llm_measuring

    end_time = datetime.datetime.now()

    measure_time = end_time - start_time

    logging.info(f"Measurement successfully, took {measure_time}")
