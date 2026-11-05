# ------------------------------------
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# ------------------------------------
"""Offline contract regressions for registering authored Command components."""

import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import parse_qs, quote, urlparse

import pytest
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.core.pipeline.transport import AsyncHttpResponse, HttpRequest, HttpResponse

from azure.ai.projects import AIProjectClient
from azure.ai.projects.aio import AIProjectClient as AsyncAIProjectClient
from azure.ai.projects.dsl import Output as ComponentOutput, component, pipeline
from azure.ai.projects.models import DatasetVersion, PipelineJob

from test_pipeline_jobs import (
    _AsyncCredential,
    _AsyncJsonResponse,
    _AsyncTransport,
    _COMPUTE,
    _Credential,
    _ENDPOINT,
    _JsonResponse,
    _Transport,
    _response,
    _uploaded_code,
)

_SURFACES = ("pipeline_jobs", "beta.jobs")
_JOB_NAME = "hello-world"
_IMAGE = "example.azurecr.io/train:latest"
# Fixture only: the returned physical-workspace ID must remain opaque.
_COMPONENT_ID = (
    "/subscriptions/test/resourceGroups/test/providers/Microsoft.MachineLearningServices/"
    "workspaces/fake-account@fake-project@AML/components/hello_world/versions/1"
)


def _author_job() -> PipelineJob:
    @component
    def hello_world(text: str, message: ComponentOutput(type="uri_file")) -> None:
        from pathlib import Path

        Path(message).write_text(text, encoding="utf-8")

    @pipeline(
        compute_id=_COMPUTE,
        environment_image_reference=_IMAGE,
        user_assigned_identity_id="/subscriptions/test/identities/hello",
        instance_type="Singularity.D4_v3",
    )
    def workflow(text: str):
        hello = hello_world(text=text)
        return {"message": hello.outputs.message}

    return workflow(text="hello")


@pytest.fixture
def authored_job() -> Iterator[PipelineJob]:
    job = _author_job()
    try:
        yield job
    finally:
        for directory in job._component_code_dirs:
            directory.cleanup()


def _job_path(surface: str) -> str:
    collection = "pipeline_jobs" if surface == "pipeline_jobs" else "jobs"
    return f"{urlparse(_ENDPOINT).path}/{collection}/{_JOB_NAME}"


class _RegistrationScenario:
    def __init__(self, job: PipelineJob, surface: str, *, fail_registration: bool = False) -> None:
        self.authored = deepcopy(job.as_dict(exclude_readonly=True))
        self.spec = self.authored["jobs"]["hello_world"]["component"]
        self.component_path = (
            f"{urlparse(_ENDPOINT).path}/components/{quote(self.spec['name'], safe='')}"
            f"/versions/{quote(str(self.spec['version']), safe='')}"
        )
        self.job_path = _job_path(surface)
        self.fail_registration = fail_registration
        self.events: list[str] = []
        self.submissions: list[dict[str, Any]] = []
        self.code_uri: str = ""

    def uploaded_authoring(self) -> dict[str, Any]:
        authored = deepcopy(self.authored)
        authored["jobs"]["hello_world"]["component"]["code"] = self.code_uri
        return authored

    def registered_payload(self) -> dict[str, Any]:
        payload = self.uploaded_authoring()
        node = payload["jobs"]["hello_world"]
        del node["component"]
        node["componentId"] = _COMPONENT_ID
        return payload

    def respond(self, request: HttpRequest) -> tuple[dict[str, Any], int]:
        assert request.method == "PUT", "Submission must not GET each component by default."
        url = urlparse(request.url)
        body = json.loads(request.body)
        if url.path == self.component_path:
            self.events.append("register")
            assert self.code_uri, "Source upload must finish before component registration."
            assert parse_qs(url.query)["api-version"] == ["2026-01-15-preview"]
            spec = deepcopy(self.spec)
            spec["code"] = self.code_uri
            assert body == {"properties": {"componentSpec": spec}}
            if self.fail_registration:
                return {"error": {"code": "InvalidComponent", "message": "Component registration rejected"}}, 400
            return {"id": _COMPONENT_ID, "properties": {"componentSpec": spec}}, 201
        assert url.path == self.job_path, f"Unexpected request: {request.method} {request.url}"
        self.events.append("job")
        assert request.headers["Foundry-Features"] == "Jobs=V1Preview"
        assert parse_qs(url.query)["api-version"] == ["2026-01-15-preview"]
        self.submissions.append(body["properties"])
        return {"name": _JOB_NAME, "id": f"/jobs/{_JOB_NAME}", "properties": body["properties"]}, 200


class _RegistrationTransport(_Transport):
    def __init__(self, scenario: _RegistrationScenario) -> None:
        super().__init__([])
        self.scenario = scenario

    def send(self, request: HttpRequest, **kwargs: Any) -> HttpResponse:
        self.requests.append(request)
        payload, status = self.scenario.respond(request)
        response = _JsonResponse(request, payload)
        response.status_code = status
        return response


class _AsyncRegistrationTransport(_AsyncTransport):
    def __init__(self, scenario: _RegistrationScenario) -> None:
        super().__init__([])
        self.scenario = scenario

    async def send(self, request: HttpRequest, **kwargs: Any) -> AsyncHttpResponse:
        self.requests.append(request)
        payload, status = self.scenario.respond(request)
        response = _AsyncJsonResponse(request, payload)
        response.status_code = status
        return response


def _mock_source_upload(
    operations: Any, scenario: _RegistrationScenario, monkeypatch: pytest.MonkeyPatch, *, is_async: bool = False
) -> None:
    def missing_asset(*, name: str, version: str) -> DatasetVersion:
        raise ResourceNotFoundError(f"Dataset {name}:{version} not found")

    def upload_folder(*, name: str, version: str, folder: str, **kwargs: Any) -> DatasetVersion:
        assert (Path(folder) / "component.py").is_file()
        scenario.events.append("upload")
        uploaded = _uploaded_code(name, version)
        assert uploaded.id is not None
        scenario.code_uri = uploaded.id
        return uploaded

    async def missing_asset_async(*, name: str, version: str) -> DatasetVersion:
        return missing_asset(name=name, version=version)

    async def upload_folder_async(*, name: str, version: str, folder: str, **kwargs: Any) -> DatasetVersion:
        return upload_folder(name=name, version=version, folder=folder, **kwargs)

    monkeypatch.setattr(operations._datasets, "get", missing_asset_async if is_async else missing_asset)
    monkeypatch.setattr(operations._datasets, "upload_folder", upload_folder_async if is_async else upload_folder)


def _assert_registered_submission(scenario: _RegistrationScenario, job: PipelineJob) -> None:
    assert scenario.events[:3] == ["upload", "register", "job"]
    assert scenario.submissions[-1] == scenario.registered_payload()
    # Existing code-URI resolution may update code; registration must retain the authored component.
    assert job.as_dict(exclude_readonly=True) == scenario.uploaded_authoring()


@pytest.mark.parametrize("surface", _SURFACES)
@pytest.mark.parametrize(
    "component_name, component_version",
    [("hello_world", "1"), ("hello/world name", "release/1+candidate")],
)
def test_pipeline_registers_command_after_upload_and_preserves_reusable_authoring(
    authored_job: PipelineJob,
    surface: str,
    component_name: str,
    component_version: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authored_job.jobs["hello_world"]["component"]["name"] = component_name
    authored_job.jobs["hello_world"]["component"]["version"] = component_version
    scenario = _RegistrationScenario(authored_job, surface)
    transport = _RegistrationTransport(scenario)
    with AIProjectClient(endpoint=_ENDPOINT, credential=_Credential(), transport=transport) as client:  # type: ignore[arg-type]
        operations = client.pipeline_jobs if surface == "pipeline_jobs" else client.beta.jobs
        _mock_source_upload(operations, scenario, monkeypatch)
        for _ in range(2):
            assert isinstance(operations.create_or_update(_JOB_NAME, authored_job), PipelineJob)
            _assert_registered_submission(scenario, authored_job)
    assert len(scenario.submissions) == 2
    assert all(request.method == "PUT" for request in transport.requests)


@pytest.mark.asyncio
@pytest.mark.parametrize("surface", _SURFACES)
@pytest.mark.parametrize(
    "component_name, component_version",
    [("hello_world", "1"), ("hello/world name", "release/1+candidate")],
)
async def test_pipeline_registers_command_after_upload_and_preserves_reusable_authoring_async(
    authored_job: PipelineJob,
    surface: str,
    component_name: str,
    component_version: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authored_job.jobs["hello_world"]["component"]["name"] = component_name
    authored_job.jobs["hello_world"]["component"]["version"] = component_version
    scenario = _RegistrationScenario(authored_job, surface)
    transport = _AsyncRegistrationTransport(scenario)
    async with AsyncAIProjectClient(
        endpoint=_ENDPOINT, credential=_AsyncCredential(), transport=transport  # type: ignore[arg-type]
    ) as client:
        operations = client.pipeline_jobs if surface == "pipeline_jobs" else client.beta.jobs
        _mock_source_upload(operations, scenario, monkeypatch, is_async=True)
        for _ in range(2):
            assert isinstance(await operations.create_or_update(_JOB_NAME, authored_job), PipelineJob)
            _assert_registered_submission(scenario, authored_job)
    assert len(scenario.submissions) == 2
    assert all(request.method == "PUT" for request in transport.requests)


@pytest.mark.parametrize("surface", _SURFACES)
def test_component_registration_failure_prevents_job_without_inline_fallback(
    authored_job: PipelineJob, surface: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    scenario = _RegistrationScenario(authored_job, surface, fail_registration=True)
    transport = _RegistrationTransport(scenario)
    with AIProjectClient(endpoint=_ENDPOINT, credential=_Credential(), transport=transport) as client:  # type: ignore[arg-type]
        operations = client.pipeline_jobs if surface == "pipeline_jobs" else client.beta.jobs
        _mock_source_upload(operations, scenario, monkeypatch)
        with pytest.raises(HttpResponseError, match="Component registration rejected"):
            operations.create_or_update(_JOB_NAME, authored_job)
    assert scenario.events == ["upload", "register"]
    assert scenario.submissions == []
    assert authored_job.as_dict(exclude_readonly=True) == scenario.uploaded_authoring()


@pytest.mark.asyncio
@pytest.mark.parametrize("surface", _SURFACES)
async def test_component_registration_failure_prevents_job_without_inline_fallback_async(
    authored_job: PipelineJob, surface: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    scenario = _RegistrationScenario(authored_job, surface, fail_registration=True)
    transport = _AsyncRegistrationTransport(scenario)
    async with AsyncAIProjectClient(
        endpoint=_ENDPOINT, credential=_AsyncCredential(), transport=transport  # type: ignore[arg-type]
    ) as client:
        operations = client.pipeline_jobs if surface == "pipeline_jobs" else client.beta.jobs
        _mock_source_upload(operations, scenario, monkeypatch, is_async=True)
        with pytest.raises(HttpResponseError, match="Component registration rejected"):
            await operations.create_or_update(_JOB_NAME, authored_job)
    assert scenario.events == ["upload", "register"]
    assert scenario.submissions == []
    assert authored_job.as_dict(exclude_readonly=True) == scenario.uploaded_authoring()


@pytest.mark.parametrize("surface", _SURFACES)
@pytest.mark.parametrize("reference_field", ("componentId", "component"))
def test_existing_component_references_are_not_registered_or_rewritten(
    authored_job: PipelineJob, surface: str, reference_field: str
) -> None:
    assert authored_job.jobs is not None
    node = authored_job.jobs["hello_world"]
    del node["component"]
    node[reference_field] = _COMPONENT_ID
    expected = deepcopy(authored_job.as_dict(exclude_readonly=True))
    transport = _Transport([_response("Pipeline")])
    with AIProjectClient(endpoint=_ENDPOINT, credential=_Credential(), transport=transport) as client:  # type: ignore[arg-type]
        operations = client.pipeline_jobs if surface == "pipeline_jobs" else client.beta.jobs
        assert isinstance(operations.create_or_update(_JOB_NAME, authored_job), PipelineJob)
    assert len(transport.requests) == 1
    assert transport.requests[0].method == "PUT"
    assert urlparse(transport.requests[0].url).path == _job_path(surface)
    assert json.loads(transport.requests[0].body) == {"properties": expected}
    assert authored_job.as_dict(exclude_readonly=True) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("surface", _SURFACES)
@pytest.mark.parametrize("reference_field", ("componentId", "component"))
async def test_existing_component_references_are_not_registered_or_rewritten_async(
    authored_job: PipelineJob, surface: str, reference_field: str
) -> None:
    assert authored_job.jobs is not None
    node = authored_job.jobs["hello_world"]
    del node["component"]
    node[reference_field] = _COMPONENT_ID
    expected = deepcopy(authored_job.as_dict(exclude_readonly=True))
    transport = _AsyncTransport([_response("Pipeline")])
    async with AsyncAIProjectClient(
        endpoint=_ENDPOINT, credential=_AsyncCredential(), transport=transport  # type: ignore[arg-type]
    ) as client:
        operations = client.pipeline_jobs if surface == "pipeline_jobs" else client.beta.jobs
        assert isinstance(await operations.create_or_update(_JOB_NAME, authored_job), PipelineJob)
    assert len(transport.requests) == 1
    assert transport.requests[0].method == "PUT"
    assert urlparse(transport.requests[0].url).path == _job_path(surface)
    assert json.loads(transport.requests[0].body) == {"properties": expected}
    assert authored_job.as_dict(exclude_readonly=True) == expected


def test_component_decorators_and_pipeline_serialization_stay_offline() -> None:
    transport = _Transport([])
    with AIProjectClient(endpoint=_ENDPOINT, credential=_Credential(), transport=transport):  # type: ignore[arg-type]
        job = _author_job()
        try:
            payload = job.as_dict(exclude_readonly=True)
            spec = payload["jobs"]["hello_world"]["component"]
            assert spec["name"] == "hello_world"
            assert spec["version"] == "1"
            assert spec["inputs"] == {"text": {"type": "string"}}
            assert spec["outputs"] == {"message": {"type": "uri_file"}}
            assert spec["environment"] == {"image": _IMAGE}
            assert transport.requests == []
        finally:
            for directory in job._component_code_dirs:
                directory.cleanup()


@pytest.mark.asyncio
async def test_component_decorators_and_pipeline_serialization_stay_offline_async() -> None:
    transport = _AsyncTransport([])
    async with AsyncAIProjectClient(
        endpoint=_ENDPOINT, credential=_AsyncCredential(), transport=transport  # type: ignore[arg-type]
    ):
        job = _author_job()
        try:
            payload = job.as_dict(exclude_readonly=True)
            spec = payload["jobs"]["hello_world"]["component"]
            assert spec["name"] == "hello_world"
            assert spec["version"] == "1"
            assert spec["inputs"] == {"text": {"type": "string"}}
            assert spec["outputs"] == {"message": {"type": "uri_file"}}
            assert spec["environment"] == {"image": _IMAGE}
            assert transport.requests == []
        finally:
            for directory in job._component_code_dirs:
                directory.cleanup()
