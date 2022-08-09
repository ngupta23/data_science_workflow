import logging
from configparser import ConfigParser
import pandas as pd
import ray
from .base_workflow import BaseWorkflow
from .local_base_workflow import LocalBaseWorkflow
from .parallel import forecast_single_group


class RayWorkflow(LocalBaseWorkflow):
    """Class that implements parallelization using Ray. Ray can share most of the
    code from local python, hence it inherits from LocalBaseWorkflow.
    """

    def __init__(self, cfg: ConfigParser, working_dir: str):
        self.working_dir = working_dir
        super().__init__(cfg=cfg)

    def initialize_parallel(self) -> "BaseWorkflow":
        """Initialize the parallel environment. For local parallel, this will
        not do anything, but for Spark, this will initialize the Spark object.
        Similarly, for Ray, this will initialize the Ray object.
        """
        logging.info("\n\nValidating the parallelizing framework ...")
        runtime_env = {"working_dir": self.working_dir}
        self.ray = ray.init(runtime_env=runtime_env)

        return self

    def compute_results(self) -> "BaseWorkflow":
        """Compute the results. This needs to be parallelized across the groups.
        This class runs the groups in series though
        """
        logging.info("\n\nComputing the results on each group ...")

        self.results = self.data.groupby("group").apply(
            forecast_single_group, fh=self.fh, folds=self.folds, session_id=self.seed
        )

        grouped_data = self.data.groupby(["group"])

        # Step 1: Decorate the function (task) outside the loop
        # If you decorate it inside the for loop, you will get a warning AND slow
        # down the execution:
        """
        WARNING import_thread.py:135 -- The remote function '<func name>' has
        been exported 100 times. It's possible that this warning is accidental,
        but this may indicate that the same remote function is being defined
        repeatedly from within many tasks and exported to all of the workers.
        This can be a performance issue and can be resolved by defining the remote
        function on the driver instead.
        See https://github.com/ray-project/ray/issues/6240 for more discussion.
        """
        function_remote = ray.remote(forecast_single_group)

        all_results = []
        for group in grouped_data.groups.keys():
            # Step 2: Call the remote task (function) with the right argument
            result_single_group = function_remote.remote(
                data=grouped_data.get_group(group),
                fh=self.fh,
                folds=self.folds,
                session_id=self.seed,
            )
            all_results.append(result_single_group)
        all_results = ray.get(all_results)

        # Combine all results into 1 data frame
        self.results = pd.concat(all_results)

        self.results.reset_index(inplace=True, drop=True)
        self.results.sort_values(["group"], inplace=True)

        return self
