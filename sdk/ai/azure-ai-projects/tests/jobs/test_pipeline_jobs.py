# ------------------------------------
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# ------------------------------------
"""Offline wire and response tests for Command and Pipeline jobs."""

from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Optional, Union
from unittest.mock import AsyncMock, Mock
from urllib.parse import parse_qs, urlparse

import pytest
from azure.core.credentials import AccessToken
from azure.core.exceptions import ResourceNotFoundError
from azure.core.pipeline.transport import (
    AsyncHttpResponse,
    AsyncHttpTransport,
    HttpRequest,
    HttpResponse,
    HttpTransport,
)

from azure.ai.projects import AIProjectClient
from azure.ai.projects.aio import AIProjectClient as AsyncAIProjectClient
from azure.ai.projects.models import AssetTypes, CommandJob, Input, JobResourceConfiguration, JobType, PipelineJob

_ENDPOINT = "https://fake-account.services.ai.azure.com/api/projects/fake-project"
_COMPUTE = "/subscriptions/test/resourceGroups/test/providers/Microsoft.CognitiveServices/accounts/test/computes/cpu"
_DATASET_PREFIX = "azureai://accounts/fake-account/projects/fake-project/data"
_PIPELINE_PROPERTIES = {
    "jobType": "Pipeline",
    "displayName": "example pipeline",
    "computeId": _COMPUTE,
    "settings": {"continueOnStepFailure": False},
    "jobs": {
        "train": {
            "jobType": "Command",
            "command": "python train.py --data ${{parent.inputs.training_data}}",
            "environmentImageReference": "example.azurecr.io/train:latest",
            "inputs": {"training_data": {"path": "${{parent.inputs.training_data}}", "mode": "ro_mount"}},
        }
    },
    "inputs": {"training_data": {"type": "uri_folder", "path": "azureml://datastores/test/paths/training/"}},
    "outputs": {"model": {"type": "uri_folder", "mode": "rw_mount"}},
}
_COMMAND_PROPERTIES = {
    "jobType": "Command",
    "command": "echo hello",
    "environmentImageReference": "example.azurecr.io/train:latest",
    "computeId": _COMPUTE,
}
_INLINE_PIPELINE_PROPERTIES = {
    "jobType": "Pipeline",
    "computeId": _COMPUTE,
    "settings": {"default_compute": _COMPUTE, "force_rerun": True},
    "inputs": {"name": {"jobInputType": "literal", "value": "world"}},
    "outputs": {},
    "jobs": {
        "hello": {
            "type": "command",
            "identity": {"type": "managed", "msi_resource_id": "/subscriptions/test/identities/hello"},
            "component": {
                "name": "hello",
                "version": "1",
                "type": "command",
                "command": "echo hello ${{inputs.name}}",
                "environment": {"image": "example.azurecr.io/train:latest"},
                "inputs": {"name": {"type": "string"}},
                "outputs": {},
            },
            "resources": {
                "instance_count": 1,
                "instance_type": "Standard_D4_v3",
                "properties": {"AISuperComputer": {"SLATier": "Premium"}},
            },
            "inputs": {"name": {"job_input_type": "literal", "value": "${{parent.inputs.name}}"}},
            "outputs": {},
        }
    },
}


class _Credential:
    def get_token(self, *args: Any, **kwargs: Any) -> AccessToken:
        return AccessToken("fake-token", 9_999_999_999)


class _AsyncCredential:
    async def get_token(self, *args: Any, **kwargs: Any) -> AccessToken:
        return AccessToken("fake-token", 9_999_999_999)


class _JsonResponse(HttpResponse):
    def __init__(self, request: HttpRequest, payload: dict[str, Any]) -> None:
        super().__init__(request, None)
        self.status_code = 200
        self.headers["Content-Type"] = "application/json"
        self._content = json.dumps(payload).encode()

    def body(self) -> bytes:
        return self._content

    def json(self) -> Any:
        return json.loads(self._content)


class _AsyncJsonResponse(AsyncHttpResponse):
    def __init__(self, request: HttpRequest, payload: dict[str, Any]) -> None:
        super().__init__(request, None)
        self.status_code = 200
        self.headers["Content-Type"] = "application/json"
        self._content = json.dumps(payload).encode()

    def body(self) -> bytes:
        return self._content

    async def read(self) -> bytes:
        return self._content

    def json(self) -> Any:
        return json.loads(self._content)


class _Transport(HttpTransport):
    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self._responses = iter(responses)
        self.requests: list[HttpRequest] = []

    def send(self, request: HttpRequest, **kwargs: Any) -> HttpResponse:
        self.requests.append(request)
        return _JsonResponse(request, next(self._responses))

    def open(self) -> None:
        pass

    def close(self) -> None:
        pass

    def __exit__(self, *args: Any) -> None:
        pass


class _AsyncTransport(AsyncHttpTransport):
    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self._responses = iter(responses)
        self.requests: list[HttpRequest] = []

    async def send(self, request: HttpRequest, **kwargs: Any) -> AsyncHttpResponse:
        self.requests.append(request)
        return _AsyncJsonResponse(request, next(self._responses))

    async def open(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def __aexit__(self, *args: Any) -> None:
        pass


def _job(kind: str) -> tuple[Union[CommandJob, PipelineJob], dict[str, Any]]:
    if kind == "Pipeline":
        return (
            PipelineJob(
                display_name="example pipeline",
                compute_id=_COMPUTE,
                settings=_PIPELINE_PROPERTIES["settings"],
                jobs=_PIPELINE_PROPERTIES["jobs"],
                inputs=_PIPELINE_PROPERTIES["inputs"],
                outputs=_PIPELINE_PROPERTIES["outputs"],
            ),
            _PIPELINE_PROPERTIES,
        )
    return (
        CommandJob(
            command="echo hello",
            environment_image_reference="example.azurecr.io/train:latest",
            compute=_COMPUTE,
        ),
        _COMMAND_PROPERTIES,
    )


def _response(kind: str) -> dict[str, Any]:
    return {
        "name": kind.lower(),
        "id": f"/jobs/{kind.lower()}",
        "properties": _PIPELINE_PROPERTIES if kind == "Pipeline" else _COMMAND_PROPERTIES,
    }


def _assert_job(result: Union[CommandJob, PipelineJob], kind: str) -> None:
    if kind == "Pipeline":
        assert isinstance(result, PipelineJob)
        assert result.jobs == _PIPELINE_PROPERTIES["jobs"]
        assert result.inputs == _PIPELINE_PROPERTIES["inputs"]
        assert result.outputs == _PIPELINE_PROPERTIES["outputs"]
        assert result.settings == _PIPELINE_PROPERTIES["settings"]
    else:
        assert isinstance(result, CommandJob)
    assert result.job_type == kind
    assert result.name == kind.lower()
    assert result.id == f"/jobs/{kind.lower()}"


def _assert_requests(requests: list[HttpRequest], kind: str, expected: dict[str, Any]) -> None:
    assert len(requests) == 3
    assert [request.method for request in requests] == ["PUT", "GET", "GET"]
    for request in requests:
        assert request.headers["Foundry-Features"] == "Jobs=V1Preview"
        assert parse_qs(urlparse(request.url).query)["api-version"] == ["2026-01-15-preview"]
    assert urlparse(requests[0].url).path.endswith(f"/jobs/{kind.lower()}")
    assert json.loads(requests[0].body) == {"properties": expected}
    assert JobType.PIPELINE == "Pipeline"


def _inline_pipeline(*, code: Optional[str] = None, base_path: Optional[Path] = None) -> tuple[PipelineJob, CommandJob]:
    command = CommandJob(
        command="echo hello ${{inputs.name}}",
        code=code,
        environment_image_reference="example.azurecr.io/train:latest",
        compute=_COMPUTE,
        inputs={"name": Input(type=AssetTypes.LITERAL, value="${{parent.inputs.name}}")},
        user_assigned_identity_id="/subscriptions/test/identities/hello",
        resources=JobResourceConfiguration(
            {
                "instanceCount": 1,
                "instanceType": "Standard_D4_v3",
                "properties": {"AISuperComputer": {"SLATier": "Premium"}},
            }
        ),
    )
    if base_path is not None:
        command._base_path = base_path
    pipeline = PipelineJob(
        compute_id=_COMPUTE,
        settings={"default_compute": _COMPUTE, "force_rerun": True},
        inputs={"name": Input(type=AssetTypes.LITERAL, value="world")},
        outputs={},
        jobs={"hello": command},
    )
    return pipeline, command


def _code_folder(tmp_path: Path) -> Path:
    folder = tmp_path / "code"
    folder.mkdir()
    (folder / "main.py").write_text("print('hello')\n", encoding="utf-8")
    return folder


def _mock_code_upload(monkeypatch: pytest.MonkeyPatch, operations: Any, *, async_upload: bool = False) -> tuple[Mock, Mock]:
    mock_type = AsyncMock if async_upload else Mock
    get = mock_type(side_effect=ResourceNotFoundError("dataset not found"))

    def uploaded_dataset(*, name: str, version: str, **kwargs: Any) -> Any:
        return SimpleNamespace(
            id=f"{_DATASET_PREFIX}/{name}/versions/{version}",
            data_uri="https://example.blob.core.windows.net/code",
        )

    upload = mock_type(side_effect=uploaded_dataset)
    monkeypatch.setattr(operations._datasets, "get", get)
    monkeypatch.setattr(operations._datasets, "upload_folder", upload)
    return get, upload


def _assert_uploaded_code(request: HttpRequest, name: str, version: str) -> None:
    expected = deepcopy(_INLINE_PIPELINE_PROPERTIES)
    expected["jobs"]["hello"]["component"]["code"] = f"{_DATASET_PREFIX}/{name}/versions/{version}"
    assert json.loads(request.body) == {"properties": expected}
    assert "codeId" not in expected["jobs"]["hello"]["component"]


def _assert_inline_request(request: HttpRequest) -> None:
    assert request.method == "PUT"
    assert request.headers["Foundry-Features"] == "Jobs=V1Preview"
    assert json.loads(request.body) == {"properties": _INLINE_PIPELINE_PROPERTIES}


def test_jobs_sync_create_from_command_node() -> None:
    pipeline, command = _inline_pipeline()
    transport = _Transport([_response("Pipeline")])

    with AIProjectClient(endpoint=_ENDPOINT, credential=_Credential(), transport=transport) as client:  # type: ignore[arg-type]
        _assert_job(client.beta.jobs.create_or_update("pipeline", pipeline), "Pipeline")

    assert len(transport.requests) == 1
    _assert_inline_request(transport.requests[0])
    assert command.inputs is not None
    assert command.inputs["name"].value == "${{parent.inputs.name}}"
    assert command.resources is not None
    assert command.resources.instance_type == "Standard_D4_v3"


@pytest.mark.asyncio
async def test_jobs_async_create_from_command_node() -> None:
    pipeline, _ = _inline_pipeline()
    transport = _AsyncTransport([_response("Pipeline")])

    async with AsyncAIProjectClient(
        endpoint=_ENDPOINT, credential=_AsyncCredential(), transport=transport  # type: ignore[arg-type]
    ) as client:
        _assert_job(await client.beta.jobs.create_or_update("pipeline", pipeline), "Pipeline")

    assert len(transport.requests) == 1
    _assert_inline_request(transport.requests[0])


def test_jobs_sync_uploads_composed_command_code(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    folder = _code_folder(tmp_path)
    pipeline, command = _inline_pipeline(code="code", base_path=tmp_path)
    transport = _Transport([_response("Pipeline"), _response("Pipeline")])

    with AIProjectClient(endpoint=_ENDPOINT, credential=_Credential(), transport=transport) as client:  # type: ignore[arg-type]
        operations = client.beta.jobs
        get, upload = _mock_code_upload(monkeypatch, operations)
        operations.create_or_update("pipeline", pipeline)
        operations.create_or_update("pipeline", pipeline)

    upload.assert_called_once()
    assert upload.call_args.kwargs["name"] == "pipeline-hello-code"
    assert upload.call_args.kwargs["folder"] == str(folder)
    version = upload.call_args.kwargs["version"]
    assert len(version) == 8
    get.assert_called_once_with(name="pipeline-hello-code", version=version)
    assert len(transport.requests) == 2
    for request in transport.requests:
        _assert_uploaded_code(request, "pipeline-hello-code", version)
    assert pipeline.jobs is not None
    assert pipeline.jobs["hello"]["component"]["code"] == f"{_DATASET_PREFIX}/pipeline-hello-code/versions/{version}"
    assert command.code == "code"


@pytest.mark.asyncio
async def test_jobs_async_uploads_composed_command_code(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    folder = _code_folder(tmp_path)
    pipeline, command = _inline_pipeline(code=str(folder))
    transport = _AsyncTransport([_response("Pipeline")])

    async with AsyncAIProjectClient(
        endpoint=_ENDPOINT, credential=_AsyncCredential(), transport=transport  # type: ignore[arg-type]
    ) as client:
        operations = client.beta.jobs
        get, upload = _mock_code_upload(monkeypatch, operations, async_upload=True)
        await operations.create_or_update("pipeline", pipeline)

    upload.assert_awaited_once()
    assert upload.call_args.kwargs["name"] == "pipeline-hello-code"
    assert upload.call_args.kwargs["folder"] == str(folder)
    version = upload.call_args.kwargs["version"]
    get.assert_awaited_once_with(name="pipeline-hello-code", version=version)
    assert len(transport.requests) == 1
    _assert_uploaded_code(transport.requests[0], "pipeline-hello-code", version)
    assert command.code == str(folder)


def test_pipeline_raw_graph_code_passes_through_without_upload(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    folder = _code_folder(tmp_path)
    pipeline = PipelineJob(compute_id=_COMPUTE, jobs={"raw": {"type": "command", "component": {"code": str(folder)}}})
    transport = _Transport([_response("Pipeline")])

    with AIProjectClient(endpoint=_ENDPOINT, credential=_Credential(), transport=transport) as client:  # type: ignore[arg-type]
        operations = client.beta.jobs
        get, upload = _mock_code_upload(monkeypatch, operations)
        operations.create_or_update("pipeline", pipeline)

    get.assert_not_called()
    upload.assert_not_called()
    assert json.loads(transport.requests[0].body)["properties"]["jobs"] == {
        "raw": {"type": "command", "component": {"code": str(folder)}}
    }


def test_jobs_sync_command_code_keeps_code_id(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    folder = _code_folder(tmp_path)
    command = CommandJob(
        command="echo hello", code=str(folder), environment_image_reference="example.azurecr.io/train:latest", compute=_COMPUTE
    )
    transport = _Transport([_response("Command")])

    with AIProjectClient(endpoint=_ENDPOINT, credential=_Credential(), transport=transport) as client:  # type: ignore[arg-type]
        operations = client.beta.jobs
        _, upload = _mock_code_upload(monkeypatch, operations)
        operations.create_or_update("command", command)

    upload.assert_called_once()
    version = upload.call_args.kwargs["version"]
    assert json.loads(transport.requests[0].body) == {
        "properties": {**_COMMAND_PROPERTIES, "codeId": f"{_DATASET_PREFIX}/command-code/versions/{version}"}
    }


@pytest.mark.asyncio
async def test_jobs_async_command_code_keeps_code_id(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    folder = _code_folder(tmp_path)
    command = CommandJob(
        command="echo hello", code=str(folder), environment_image_reference="example.azurecr.io/train:latest", compute=_COMPUTE
    )
    transport = _AsyncTransport([_response("Command")])

    async with AsyncAIProjectClient(
        endpoint=_ENDPOINT, credential=_AsyncCredential(), transport=transport  # type: ignore[arg-type]
    ) as client:
        operations = client.beta.jobs
        _, upload = _mock_code_upload(monkeypatch, operations, async_upload=True)
        await operations.create_or_update("command", command)

    upload.assert_awaited_once()
    version = upload.call_args.kwargs["version"]
    assert json.loads(transport.requests[0].body) == {
        "properties": {**_COMMAND_PROPERTIES, "codeId": f"{_DATASET_PREFIX}/command-code/versions/{version}"}
    }


def test_jobs_sync_create_from_mapping_with_command_node() -> None:
    _, command = _inline_pipeline()
    pipeline = PipelineJob(
        {
            "computeId": _COMPUTE,
            "settings": {"default_compute": _COMPUTE, "force_rerun": True},
            "inputs": {"name": Input(type=AssetTypes.LITERAL, value="world")},
            "outputs": {},
            "jobs": {"hello": command},
        }
    )

    transport = _Transport([_response("Pipeline")])
    with AIProjectClient(endpoint=_ENDPOINT, credential=_Credential(), transport=transport) as client:  # type: ignore[arg-type]
        _assert_job(client.beta.jobs.create_or_update("pipeline", pipeline), "Pipeline")

    assert len(transport.requests) == 1
    _assert_inline_request(transport.requests[0])


def test_pipeline_composes_commands_and_preserves_raw_graph_nodes() -> None:
    first, _ = _inline_pipeline()
    raw_node = {"type": "command", "component": {"name": "raw", "version": "1"}}
    pipeline = PipelineJob(
        compute_id=_COMPUTE,
        jobs={
            "hello": CommandJob(
                command="echo hello", environment_image_reference="example.azurecr.io/train:latest", compute=_COMPUTE
            ),
            "second": CommandJob(
                command="echo again", environment_image_reference="example.azurecr.io/train:latest", compute=_COMPUTE
            ),
            "raw": raw_node,
        },
    )

    assert pipeline.jobs is not None
    assert pipeline.jobs["hello"]["component"]["command"] == "echo hello"
    assert pipeline.jobs["second"]["component"]["command"] == "echo again"
    assert pipeline.jobs["raw"] == raw_node
    assert first.jobs is not None
    assert first.jobs["hello"] == _INLINE_PIPELINE_PROPERTIES["jobs"]["hello"]


@pytest.mark.parametrize(
    ("extra", "message"),
    [
        ({"code": ""}, "non-empty code path or URI"),
        ({"inputs": {"data": Input(type=AssetTypes.URI_FILE, path="azureai:data:1")}}, "input 'data'"),
        ({"compute": "/subscriptions/test/computes/other"}, "default compute"),
        ({"resources": JobResourceConfiguration({"shmSize": "1g"})}, "shmSize"),
    ],
)
def test_pipeline_rejects_unmapped_command_fields(extra: dict[str, Any], message: str) -> None:
    fields: dict[str, Any] = {
        "command": "echo hello",
        "environment_image_reference": "example.azurecr.io/train:latest",
        "compute": _COMPUTE,
    }
    fields.update(extra)
    with pytest.raises(ValueError, match=message):
        PipelineJob(compute_id=_COMPUTE, jobs={"hello": CommandJob(**fields)})


@pytest.mark.parametrize("kind", ["Command", "Pipeline"])
def test_jobs_sync_create_get_and_list(kind: str) -> None:
    job, expected = _job(kind)
    transport = _Transport([_response(kind), _response(kind), {"value": [_response("Command"), _response("Pipeline")]}])

    with AIProjectClient(endpoint=_ENDPOINT, credential=_Credential(), transport=transport) as client:  # type: ignore[arg-type]
        _assert_job(client.beta.jobs.create_or_update(kind.lower(), job), kind)
        _assert_job(client.beta.jobs.get(kind.lower()), kind)
        listed = list(client.beta.jobs.list())
        assert len(listed) == 2
        _assert_job(listed[0], "Command")
        _assert_job(listed[1], "Pipeline")

    _assert_requests(transport.requests, kind, expected)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["Command", "Pipeline"])
async def test_jobs_async_create_get_and_list(kind: str) -> None:
    job, expected = _job(kind)
    transport = _AsyncTransport(
        [_response(kind), _response(kind), {"value": [_response("Command"), _response("Pipeline")]}]
    )

    async with AsyncAIProjectClient(
        endpoint=_ENDPOINT, credential=_AsyncCredential(), transport=transport  # type: ignore[arg-type]
    ) as client:
        _assert_job(await client.beta.jobs.create_or_update(kind.lower(), job), kind)
        _assert_job(await client.beta.jobs.get(kind.lower()), kind)
        listed = [item async for item in client.beta.jobs.list()]
        assert len(listed) == 2
        _assert_job(listed[0], "Command")
        _assert_job(listed[1], "Pipeline")

    _assert_requests(transport.requests, kind, expected)
