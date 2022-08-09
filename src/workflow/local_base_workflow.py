import os
import logging
from typing import Optional
import numpy as np
import pandas as pd
from pycaret.datasets import get_data
from .base_workflow import BaseWorkflow


class LocalBaseWorkflow(BaseWorkflow):
    def validate_inputs(self) -> "BaseWorkflow":
        """Validate the inputs"""
        logging.info("\n\nValidating the inputs in the Config file ...")

        if not isinstance(self.fh, int):
            raise TypeError("fh must be an integer.")
        if not isinstance(self.seed, int):
            raise TypeError("seed must be an integer.")
        return self

    def read_all_data(self) -> "BaseWorkflow":
        """Read all data"""
        logging.info("\n\nReading all Data ...")

        # Get Airline Data
        data1 = pd.DataFrame({"num_passengers": get_data("airline")})
        data1["group"] = "American"
        data2 = pd.DataFrame({"num_passengers": get_data("airline")})
        data2["group"] = "Delta"
        self.data = pd.concat([data1, data2])

        # Get Weather Data (just an example)
        self.weather = pd.DataFrame({"temp": np.random.randint(0, 120, len(data1))})
        self.weather.index = data1.index
        return self

    def validate_data(self) -> "BaseWorkflow":
        """Validate the data"""
        logging.info("\n\nValidating read Data ...")

        if "group" not in self.data.columns or self.data["group"].nunique() == 0:
            raise ValueError("No group provided.")
        return self

    def log_read_data(self) -> "BaseWorkflow":
        """Log the read data"""
        logging.info("\n\nLogging read Data ...")

        logging.info(f"\n\nAirline Data:\n{self.data.info()}")
        logging.info(f"\n\nWeather Data:\n {self.weather.info()}")
        return self

    def setup_workflow(self) -> "BaseWorkflow":
        """Setup the workflow"""
        logging.info("\n\nSetting up the workflow ...")

        # Merge weather data with airline data (feature development)
        self.data = self.data.join(self.weather)

        # Reset index so that account period is available in column itself.
        self.data.reset_index(inplace=True)
        return self

    def validate_setup(self) -> "BaseWorkflow":
        """Validate the setup"""
        logging.info("\n\nValidating the Workflow Setup ...")

        if "temp" not in self.data.columns:
            raise ValueError("Data merge failed to add weather data.")

        pass_ = True

        ##################################
        #### Check for missing values ####
        ##################################
        cols_to_check = ["group", "temp"]

        problem_cols = []
        for col in cols_to_check:
            missing_values = self.data[col].isna().sum()
            if missing_values != 0:
                pass_ = False
                problem_cols.append(col)

        if not pass_:
            raise ValueError(f"Data has missing values in column(s): {problem_cols}")

        return self

    def clean_results(self) -> "BaseWorkflow":
        """Clean the results"""
        logging.info("\n\nCleaning the Results ...")

        self.results.reset_index(inplace=True, drop=True)

        return self

    def validate_results(self) -> "BaseWorkflow":
        """Validate the results"""
        logging.info("\n\nValidating the Results ...")

        pass_ = True

        ##################################
        #### Check for missing values ####
        ##################################
        cols_to_check = ["Period", "group", "y_pred"]

        problem_cols = []
        for col in cols_to_check:
            missing_values = self.results[col].isna().sum()
            if missing_values != 0:
                pass_ = False
                problem_cols.append(col)

        if not pass_:
            raise ValueError(
                f"Results have missing values in column(s): {problem_cols}"
            )

        return self

    def log_results(self) -> "BaseWorkflow":
        """Log the results"""
        logging.info("\n\nLogging the Results ...")

        logging.info(f"\n\nResults:\n{self.results.info()}")

        return self

    def write_results(
        self, path: Optional[str] = ".", name: Optional[str] = "results.csv"
    ) -> "BaseWorkflow":
        """Write the results"""
        logging.info("\n\nWriting the Results ...")

        location = os.path.join(path, name)
        logging.info(f"Location: {location}")
        self.results.to_csv(location)

        return self
