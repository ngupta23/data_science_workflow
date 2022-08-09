# data_science_workflow
A generic data science workflow with optional to parallelize execution at scale.

NOTE: As currently setup, this is not to showcase the speed of execution of the parallelizing frameworks (uses a dummy dataset with only two groups). It is only intended to show the level of effort needed to convert pandas code to various parallelizing frameworks. In the future, speed benchmarking can also be included by using bigger datasets.

# Setup

In run\run_workflow.py, change the PARALLEL_MECHANISM to one of the allowed types. Currently supported options are:
- "local_serial"
- "ray"

NOTE: In the future the following options will be enabled:
- "local_parallel" (using Multiprocessing)
- "spark" (using native Spark or Fugue)

# To Run

data_science_workflow> python run/run_workflow.py

# Future Improvements

- Read all configurations from a config file instead of hard coding


