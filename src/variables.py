from datetime import datetime
import logging

generate_file_time = ""


def reload_var():
    global generate_file_time

    localdate = datetime.now()
    generate_file_time = f"d{localdate.day}m{localdate.month}y{localdate.year}_{localdate.hour}h{localdate.minute}"
