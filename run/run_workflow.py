import sys
import os
import logging
import time
from configparser import ConfigParser

# https://stackoverflow.com/questions/16771894/python-nameerror-global-name-file-is-not-defined
if "__file__" in vars():
    print("We are running the script non interactively")
    path = os.path.join(os.path.dirname(__file__), os.pardir, "src")
    print(f"Source Folder Path: {path}")
else:
    print("We are running the script interactively")
    path = "../src"
    print(f"Source Folder Path: {path}")
sys.path.append(path)

from workflow.local_serial_workflow import LocalSerialWorkflow
from workflow.local_parallel_workflow import LocalParallelWorkflow
from workflow.ray_workflow import RayWorkflow

# TODO: Enable once Spark implementations is complete
# from workflow.spark_workflow import SparkWorkflow

logging.basicConfig(level=logging.INFO, format="%(asctime)s: %(message)s")

# Temporarily set here. Should actually be read from the config file.
PARALLEL_MECHANISM = "local_serial"
# PARALLEL_MECHANISM = "ray"

if __name__ == "__main__":
    # Switch out workflows based on what is present in the config file
    if PARALLEL_MECHANISM == "local_serial":
        workflow = LocalSerialWorkflow(ConfigParser())
    elif PARALLEL_MECHANISM == "local_parallel":
        workflow = LocalParallelWorkflow(ConfigParser())
    elif PARALLEL_MECHANISM == "ray":
        # Need to pass working directory here otherwise ray will not know where
        # the "workflow" folder resides, per https://stackoverflow.com/a/70717760/8925915
        workflow = RayWorkflow(ConfigParser(), working_dir=path)
    # elif PARALLEL_MECHANISM == "spark":
    #     workflow = SparkWorkflow(ConfigParser())
    else:
        raise ValueError(f"Parallel Mechanism '{PARALLEL_MECHANISM}' is not supported.")

    start = time.time()
    workflow.run_workflow()
    end = time.time()
    print(f"Total time taken to process all groups: {round(end-start)}s")
    print("Done")
    sys.exit(0)
