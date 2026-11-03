# ------------------------------------
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# ------------------------------------
"""Compose a two-step Pipeline component inside a parent pipeline.

Set FOUNDRY_PROJECT_ENDPOINT, JOB_COMPUTE_ID, JOB_ENVIRONMENT_IMAGE,
JOB_NODE_UAI_RESOURCE_ID, and JOB_INSTANCE_TYPE before running. The service must
support inline Pipeline component registration. Running this sample uploads
three Code folders and submits a job; the nested graph is not flattened.
"""

import os
from pathlib import Path

from azure.ai.projects import AIProjectClient
from azure.ai.projects.dsl import Input, Output, component, pipeline
from azure.identity import AzureCliCredential


@component
def produce(text: str, message: Output(type="uri_file")) -> None:  # type: ignore[valid-type]
    Path(message).write_text(text, encoding="utf-8")


@component
def normalize(message: Input(type="uri_file"), normalized: Output(type="uri_file")) -> None:  # type: ignore[valid-type]
    Path(normalized).write_text(Path(message).read_text(encoding="utf-8").strip().upper(), encoding="utf-8")


@component
def consume(message: Input(type="uri_file"), receipt: Output(type="uri_file")) -> None:  # type: ignore[valid-type]
    Path(receipt).write_text(Path(message).read_text(encoding="utf-8"), encoding="utf-8")


_RUNTIME = {
    "compute_id": os.environ["JOB_COMPUTE_ID"],
    "environment_image_reference": os.environ["JOB_ENVIRONMENT_IMAGE"],
    "user_assigned_identity_id": os.environ["JOB_NODE_UAI_RESOURCE_ID"],
    "instance_type": os.environ["JOB_INSTANCE_TYPE"],
}


@pipeline(**_RUNTIME)
def text_subgraph(text: str):
    first = produce(text=text)
    second = normalize(message=first.outputs.message)
    return {"message": second.outputs.normalized}


@pipeline(**_RUNTIME)
def workflow(text: str):
    stage = text_subgraph(text=text)
    final = consume(message=stage.outputs.message)
    return {"receipt": final.outputs.receipt}


def main() -> None:
    job = workflow(text="hello")
    with (
        AzureCliCredential() as credential,
        AIProjectClient(
            endpoint=os.environ["FOUNDRY_PROJECT_ENDPOINT"],
            credential=credential,
            allow_preview=True,
            retry_total=0,
        ) as client,
    ):
        created = client.pipeline_jobs.create_or_update(job, experiment_name="pipeline_samples")
        print(f"Submitted {created.name}: {created.id} (status: {created.status})")


if __name__ == "__main__":
    main()
