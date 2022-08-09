import logging
from .base_workflow import BaseWorkflow
from .local_base_workflow import LocalBaseWorkflow
from .parallel import forecast_single_group


class LocalSerialWorkflow(LocalBaseWorkflow):
    def initialize_parallel(self) -> "BaseWorkflow":
        """Initialize the parallel environment. For local parallel, this will
        not do anything, but for Spark, this will initialize the Spark object.
        Similarly, for Ray, this will initialize the Ray object.
        """
        logging.info("\n\nValidating the parallelizing framework ...")

        return self

    def compute_results(self) -> "BaseWorkflow":
        """Compute the results. This needs to be parallelized across the groups.
        This class runs the groups in series though
        """
        logging.info("\n\nComputing the results on each group ...")

        self.results = self.data.groupby("group").apply(
            forecast_single_group, fh=self.fh, folds=self.folds, session_id=self.seed
        )

        return self
