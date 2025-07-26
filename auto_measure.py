import sys
import functional_methods
from k8s_API import update_deployment
from multiprocessing import Event, Process
import variables
import main
import time

import logging
logger = logging.getLogger(__name__)

# import merge

if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)

    logging.basicConfig(filename="logs/main.log",
                        filemode='a',
                        # format='%(asctime)s,%(msecs)03d - %(name)s - %(levelname)s - %(message)s',
                        format='[%(name)s] - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.DEBUG)

    logging.info("Start measurement!")

    target_pods_scale = [1]
    repeat_time = 1
    current_time = 1
    node = 'edge-node'  # name of machine where pods are deployed
    # node = 'mec'
    image = 'arm'  # name of image type
    # image = 'x86'

    # list_quality = ["360P", "480P", "HD", "2K", "4K"]
    list_quality = []

    for target_pod in target_pods_scale:
        # Update number of deployment according to target_pod
        update_deployment(target_pod, "null", node)

        print("Deployment has been updated to {} pods".format(target_pod))

        for rep in range(current_time, repeat_time + 1, 1):
            print("Target pod: {}, Repeat time: {}/{}, Instance: {}".format(target_pod,
                  rep, repeat_time, node))
            logging.info(
                f"Target pod: {target_pod}, Repeat time: {rep}/{repeat_time}, Instance: {node}")

            logging.info("Reset all variables")
            variables.reload()  # reset all variables

            event = Event()  # the event is unset when created
            # curl to exit() the code from inside container, causing container to be removed
            # p0 = Process(target=functional_methods.auto_delete,
            #              args=(target_pod, event, ))
            # p0.start()
            
            main.curl_latency(node, image, list_quality, int(target_pod), int(
                rep), event)  # measure response time using 'curl' commands
            # time.sleep(20) # sleep is used quite often to stablize system, making the results more precise
            # collect the entire lifecycle: Null -> Cold -> Warm disk -> Warm CPU -> Active
            main.collect_life_cycle(
                node, image, int(target_pod), int(rep), event)
            # main.collect_cold_warm_disk(node, image, int(target_pod), int(rep), event) # collect only: Null -> Cold -> warm disk
            # p0.join()
            time.sleep(20)

            # cmd = '/usr/bin/python3 ' + DEFAULT_DIRECTORY +'/main_rebuild.py {} {} {}'.format(str(target_pod),  str(rep), str(instance))
            # process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE)
    # event.set()
