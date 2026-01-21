"""Pipeline helpers and auxiliary assets.

We use dagster (https://dagster.io) to orchestrate the ETL processes of the
RKI Metadata Exchange. Dagster allows us to structure our code, visualize dependencies
between different data sources and schedule the execution of pipelines.

software-defined assets
-----------------------

Each pipeline should have a `main` module, e.g. `mex.extractors.artificial.main`, that
contains all steps needed for a successful execution. Each "step" should be marked as a
software-defined asset using the decorator by `dagster.asset` and should be assigned to
a group with a name unique to the pipeline.

    # mex/foo_system/main.py

    @asset(group_name="foo_system")
    def extracted_foo() -> Foo:
        return Foo()

An individual step should be one semantic unit, like get something from there,
transform this to that or link these things to those things.
Steps that can be re-used by multiple pipelines should go into `mex.extractors.pipeline`
and should be assigned to the `"default"` group.

    # mex/pipeline/reusable_thing.py

    @asset(group_name="default")
    def reusable_thing() -> Thing:
        return Thing()

dependency injection
--------------------

Dagster helps with modelling dependencies between the different steps of your pipeline.
When a pipeline is started, dagster will build a graph of all the assets and
automatically reuse the output of assets that are used by multiple other assets.
The way to tell dagster which other assets your current asset is dependent on is by
simply adding the name of the dependency in the function signature or passing the `deps`
argument to the `asset` decorator in case you don't need its return value.

    # mex/foo_system/main.py

    @asset(group_name="foo_system", deps=["asset_that_should_run_first"])
    def extracted_foo(reusable_thing: Thing) -> Foo:
        return Foo(thing=reusable_thing)


running pipelines
-----------------

Pipelines can be run in a couple of different ways:

- run `uv run dagster dev` and click `materialize all` on
  `http://localhost:3000/locations/mex/jobs/foo_system`
- run `uv run foo-system` according to the entrypoint in `pyproject.toml`
- run `uv run dagster job execute -m mex -j foo_system` using the asset group name
"""

from mex.extractors.pipeline.base import load_job_definitions, run_job_in_process

__all__ = (
    "load_job_definitions",
    "run_job_in_process",
)
