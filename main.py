import logging
import json
import time

from src.main_tasks.web_measuring import WebMeasuring
from src import variables as var


if __name__ == "__main__":
    logging.basicConfig(
        filename="logs/main.log",
        filemode="a",
        # format="%(asctime)s,%(msecs)03d - %(name)s - %(levelname)s %(message)s",
        format="%(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )

    logging.info("Start serverless measurement!")

    # logging.info(f"Before: {var.generate_file_time}")

    var.reload_var()

    # logging.info(f"After: {var.generate_file_time}")

    with open("config/config.json", "r") as f:
        # Load the file's content into a dictionary
        data = json.load(f)

    test_cases = data["test_cases"]

    for test_case in test_cases:
        if test_case["test_case"] == "web":

            WebMeasuring.warm_resptime(config=test_case)

            logging.info("Starting measuring web service")

            WebMeasuring.warm_hardware_usage(config=test_case)
