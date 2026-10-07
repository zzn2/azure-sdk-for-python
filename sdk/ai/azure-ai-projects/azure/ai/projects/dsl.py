# ------------------------------------
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# ------------------------------------
"""Decorators for composing Foundry PipelineJob command nodes."""

from contextvars import ContextVar
from functools import wraps
from typing import Any, Callable, Dict, Mapping, Optional

from typing_extensions import ParamSpec

from .models import CommandJob, Output, PipelineJob

_P = ParamSpec("_P")
_current_jobs: ContextVar[Optional[Dict[str, CommandJob]]] = ContextVar("foundry_pipeline_jobs", default=None)


class _NodeOutputs:
    def __init__(self, node_name: str, outputs: Mapping[str, Output]) -> None:
        self._node_name = node_name
        self._outputs = outputs

    def __getattr__(self, output_name: str) -> str:
        if output_name not in self._outputs:
            raise AttributeError(f"Pipeline node '{self._node_name}' does not declare output '{output_name}'.")
        return f"${{{{parent.jobs.{self._node_name}.outputs.{output_name}}}}}"


class _CommandNode:
    def __init__(self, name: str, outputs: Optional[Dict[str, Output]]) -> None:
        self.outputs = _NodeOutputs(name, outputs or {})


def command(factory: Callable[_P, CommandJob]) -> Callable[_P, _CommandNode]:
    """Register a CommandJob factory call as a named node inside a pipeline.

    The returned node exposes declared outputs as string bindings through ``.outputs``.

    :param factory: Factory returning an SDK CommandJob.
    :type factory: Callable[..., ~azure.ai.projects.models.CommandJob]
    :return: A factory that registers its command node when called inside a pipeline.
    :rtype: Callable
    """

    @wraps(factory)
    def register(*args: _P.args, **kwargs: _P.kwargs) -> _CommandNode:
        jobs = _current_jobs.get()
        if jobs is None:
            raise RuntimeError("A @dsl.command factory must be called inside a @dsl.pipeline function.")
        name = factory.__name__
        if name in jobs:
            raise ValueError(f"Pipeline node '{name}' is already registered.")
        job = factory(*args, **kwargs)
        if not isinstance(job, CommandJob):
            raise TypeError(f"Pipeline node '{name}' factory must return an azure.ai.projects.models.CommandJob.")
        jobs[name] = job
        return _CommandNode(name, job.outputs)

    return register


def pipeline(
    *,
    display_name: str,
    compute_id: str,
    settings: Dict[str, Any],
) -> Callable[[Callable[[], None]], Callable[[], PipelineJob]]:
    """Compose decorated CommandJob calls into an SDK PipelineJob.

    :keyword display_name: Display name of the pipeline job.
    :paramtype display_name: str
    :keyword compute_id: Full resource ID of the pipeline's compute.
    :paramtype compute_id: str
    :keyword settings: Pipeline settings passed to PipelineJob.
    :paramtype settings: dict[str, Any]
    :return: A decorator for a zero-argument pipeline function.
    :rtype: Callable
    """

    def decorate(workflow: Callable[[], None]) -> Callable[[], PipelineJob]:
        @wraps(workflow)
        def build_job() -> PipelineJob:
            jobs: Dict[str, CommandJob] = {}
            token = _current_jobs.set(jobs)
            try:
                workflow()
            finally:
                _current_jobs.reset(token)
            return PipelineJob(
                display_name=display_name,
                compute_id=compute_id,
                settings=settings,
                jobs=jobs,
                inputs={},
                outputs={},
            )

        return build_job

    return decorate
