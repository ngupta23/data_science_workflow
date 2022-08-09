import logging
from .base_workflow import BaseWorkflow
from .local_base_workflow import LocalBaseWorkflow


class LocalParallelWorkflow(LocalBaseWorkflow):
    def compute_results(self) -> "BaseWorkflow":
        """Compute the results. This needs to be parallelized across the groups.
        This class uses the Multiprocessing module to run the groups in parallel.
        """
        logging.info("\n\nComputing the results on each group ...")

        # TODO: Implementation TBD

        return self
