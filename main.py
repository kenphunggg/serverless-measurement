import logging
import json
import time
import datetime

from src.main_tasks.web_measuring import WebMeasuring
from src.main_tasks.streaming import StreamingMeasuring
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
    with open("config/config.json", "r") as f:
        data = json.load(f)
    test_cases = data["test_cases"]

    # Run all test cases
    for test_case in test_cases:
        if test_case["test_case"] == "web":
            # pass
            web_measuring = WebMeasuring(config=test_case)
            web_measuring.baseline()
            web_measuring.get_warm_resptime()
            web_measuring.get_warm_hardware_usage()

        elif test_case["test_case"] == "streaming":
            # pass
            streaming_measuring = StreamingMeasuring(config=test_case)
            streaming_measuring.baseline()
            streaming_measuring.timeToFirstFrame()
            streaming_measuring.measure()

        elif test_case["test_case"] == "":
            pass

        else:
            pass

    end_time = datetime.datetime.now()

    measure_time = end_time - start_time

    logging.info(f"Measurement successfully, took {measure_time}")
