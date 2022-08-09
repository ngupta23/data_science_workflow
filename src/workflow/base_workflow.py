from configparser import ConfigParser
from typing import Optional
from abc import ABC, abstractmethod
import logging


class BaseWorkflow(ABC):
    def __init__(self, cfg: ConfigParser):
        logging.info("\n\nInitializing the Workflow Object ...")

        self.cfg = cfg

    def initialize_from_cfg(self) -> "BaseWorkflow":
        """Initialize the workflow from the configuration"""
        logging.info("\n\nInitializing the workflow from the config file ...")

        # For now, we are just initializing some class attributes, but this
        # should actually be read in through the config file (self.cfg).
        self.fh = 12
        self.seed = 42
        self.folds = 3
        return self

    @abstractmethod
    def initialize_parallel(self) -> "BaseWorkflow":
        """Initialize the parallel environment. For local parallel, this will
        not do anything, but for Spark, this will initialize the Spark object.
        Similarly, for Ray, this will initialize the Ray object.
        """

    @abstractmethod
    def validate_inputs(self) -> "BaseWorkflow":
        """Validate the inputs"""

    @abstractmethod
    def read_all_data(self) -> "BaseWorkflow":
        """Read all data"""

    @abstractmethod
    def validate_data(self) -> "BaseWorkflow":
        """Validate the data"""

    @abstractmethod
    def log_read_data(self) -> "BaseWorkflow":
        """Log the read data"""

    @abstractmethod
    def setup_workflow(self) -> "BaseWorkflow":
        """Setup the workflow"""

    @abstractmethod
    def validate_setup(self) -> "BaseWorkflow":
        """Validate the setup"""

    @abstractmethod
    def compute_results(self) -> "BaseWorkflow":
        """Compute the results. This needs to be parallelized across the groups."""

    @abstractmethod
    def clean_results(self) -> "BaseWorkflow":
        """Clean the results"""

    @abstractmethod
    def validate_results(self) -> "BaseWorkflow":
        """Validate the results"""

    @abstractmethod
    def log_results(self) -> "BaseWorkflow":
        """Log the results"""

    @abstractmethod
    def write_results(
        self, path: Optional[str] = ".", name: Optional[str] = "results.csv"
    ) -> "BaseWorkflow":
        """Write the results"""

    def run_workflow(self):
        """Run the workflow"""
        logging.info("\n\nRunning the Workflow ...")

        (
            self.initialize_from_cfg()
            .initialize_parallel()
            .validate_inputs()
            .read_all_data()
            .validate_data()
            .log_read_data()
            .setup_workflow()
            .validate_setup()
            .compute_results()
            .clean_results()
            .validate_results()
            .log_results()
            .write_results()
        )
