import logging
from pyspark.sql import SparkSession
from .base_workflow import BaseWorkflow


class SparkWorkflow(BaseWorkflow):
    """Class that implements parallelization using Spark.
    TODO: Implementation is incomplete. Need to implement all abstract methods.
    """

    def initialize_parallel(self) -> "BaseWorkflow":
        """Initialize the parallel environment. For local parallel, this will
        not do anything, but for Spark, this will initialize the Spark object.
        Similarly, for Ray, this will initialize the Ray object.
        """
        logging.info("\n\nValidating the parallelizing framework ...")
        # TODO: Placeholder for now
        self.spark = (
            SparkSession.builder.appName("Python Spark SQL basic example")
            .config("spark.some.config.option", "some-value")
            .getOrCreate()
        )

        return self
