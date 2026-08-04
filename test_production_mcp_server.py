import asyncio
import importlib.util
import json
import os
import sys
import tempfile
import time
import types
import unittest
from dataclasses import dataclass
from pathlib import Path


@dataclass
class TextContent:
    type: str
    text: str


class Tool:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class Server:
    def __init__(self, *_args, **_kwargs):
        pass

    def list_tools(self):
        return lambda fn: fn

    def call_tool(self):
        return lambda fn: fn


def install_stubs():
    modules = {
        "mcp": types.ModuleType("mcp"),
        "mcp.server": types.ModuleType("mcp.server"),
        "mcp.server.streamable_http_manager": types.ModuleType("mcp.server.streamable_http_manager"),
        "mcp.types": types.ModuleType("mcp.types"),
    }
    modules["mcp.server"].Server = Server
    modules["mcp.server.streamable_http_manager"].StreamableHTTPSessionManager = object
    modules["mcp.types"].TextContent = TextContent
    modules["mcp.types"].Tool = Tool
    sys.modules.update(modules)
    for name in [
        "starlette.applications",
        "starlette.middleware",
        "starlette.middleware.base",
        "starlette.middleware.cors",
        "starlette.requests",
        "starlette.responses",
        "starlette.routing",
        "starlette.types",
        "uvicorn",
    ]:
        sys.modules[name] = types.ModuleType(name)
    sys.modules["starlette.applications"].Starlette = object
    sys.modules["starlette.middleware"].Middleware = lambda *args, **kwargs: (args, kwargs)
    sys.modules["starlette.middleware.base"].BaseHTTPMiddleware = object
    sys.modules["starlette.middleware.cors"].CORSMiddleware = object
    sys.modules["starlette.requests"].Request = object
    sys.modules["starlette.responses"].HTMLResponse = object
    sys.modules["starlette.responses"].PlainTextResponse = object
    sys.modules["starlette.routing"].Mount = object
    sys.modules["starlette.types"].Receive = object
    sys.modules["starlette.types"].Scope = dict
    sys.modules["starlette.types"].Send = object
    sys.modules["uvicorn"].run = lambda *args, **kwargs: None


def load_module(workspace, env=None):
    install_stubs()
    updates = {
        "WIKI_WORKSPACE_PATH": str(workspace),
        "WORKSPACE_NAME": "test-workspace",
        "PRODUCTION_REQUIRE_CONFIRMATION": "true",
        **(env or {}),
    }
    previous = {key: os.environ.get(key) for key in updates}
    os.environ.update(updates)
    try:
        path = Path(__file__).with_name("production_mcp_server.py")
        spec = importlib.util.spec_from_file_location(f"production_mcp_server_test_subject_{time.time_ns()}", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


class ProductionMcpServerTest(unittest.TestCase):
    def test_ingest_progress_advances_between_source_start_and_completion(self):
        progress = self.server._parse_trace_progress
        trace = self.workspace / "trace.log"
        trace.write_text(
            "\n".join(
                [
                    "2026-07-21T10:00:00Z +0ms INFO ingest:run-start inputCount=1",
                    "2026-07-21T10:00:01Z +1ms INFO ingest:source-selection resolvedCount=1",
                    "2026-07-21T10:00:02Z +2ms INFO ingest:source-start sourcePath=raw/a.md",
                ]
            ),
            encoding="utf-8",
        )
        self.assertEqual(progress("trace.log")["percent"], 15)
        with trace.open("a", encoding="utf-8") as stream:
            stream.write("\n2026-07-21T10:00:03Z +3ms INFO ingest:prompt source=raw/a.md")
        self.assertEqual(progress("trace.log")["percent"], 35)
        with trace.open("a", encoding="utf-8") as stream:
            stream.write("\n2026-07-21T10:00:04Z +4ms INFO ingest:plan source=raw/a.md")
        self.assertEqual(progress("trace.log")["percent"], 85)

    def test_trace_summary_exposes_llm_tokens_to_agent_result_metrics(self):
        trace = self.workspace / "trace.log"
        trace.write_text(
            "2026-07-21T10:00:04Z +4ms INFO trace:summary "
            "llmInputTokens=1203 llmOutputTokens=456\n",
            encoding="utf-8",
        )
        progress = self.server._parse_trace_progress("trace.log")
        self.assertEqual(progress["inputTokens"], 1203)
        self.assertEqual(progress["outputTokens"], 456)
        result = self.server._agent_task_result(
            {
                "status": "done",
                "startedAt": "2026-07-21T10:00:00Z",
                "finishedAt": "2026-07-21T10:01:00Z",
            },
            progress,
        )
        self.assertEqual(result["metrics"]["inputTokens"], 1203)
        self.assertEqual(result["metrics"]["outputTokens"], 456)
        self.assertEqual(result["metrics"]["totalTokens"], 1659)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.workspace = Path(self.tmp.name)
        (self.workspace / "templates").mkdir()
        (self.workspace / "deliverables").mkdir()
        self.server = load_module(self.workspace)

    def tearDown(self):
        self.tmp.cleanup()

    def payload(self, result):
        return json.loads(result[0].text)

    def test_shipped_allowed_step_defaults_include_restore(self):
        root = Path(__file__).parent
        for relative in ["Dockerfile", "docker-compose.yml", ".env.example", "README.md"]:
            content = (root / relative).read_text(encoding="utf-8")
            declarations = [
                line for line in content.splitlines()
                if "PRODUCTION_ALLOWED_STEPS" in line and not line.lstrip().startswith("#")
            ]
            self.assertTrue(declarations, f"{relative} must declare PRODUCTION_ALLOWED_STEPS")
            self.assertTrue(
                all("restore" in line.split("PRODUCTION_ALLOWED_STEPS", 1)[1] for line in declarations),
                f"{relative} must allow restore in every shipped default",
            )

    def assert_task_graph_fragment(self, fragment):
        self.assertEqual(fragment["contractVersion"], "1")
        self.assertEqual(fragment["agentInstanceId"], "production-main")
        self.assertIn("capability", fragment)
        self.assertIn("summary", fragment)
        self.assertIn("groups", fragment)
        self.assertIn("tasks", fragment)
        self.assertEqual(fragment["summary"]["estimatedTasks"], len(fragment["tasks"]))
        for task in fragment["tasks"]:
            for key in [
                "id",
                "label",
                "requiredCapability",
                "operation",
                "dependsOn",
                "parallelizable",
                "inputRefs",
                "locks",
                "requiresApproval",
                "idempotencyKey",
                "progressWeight",
            ]:
                self.assertIn(key, task)
            self.assertNotIn("executor", task)
            self.assertIsInstance(task["dependsOn"], list)
            self.assertIsInstance(task["inputRefs"], list)
            self.assertIsInstance(task["locks"], list)
            for ref in task["inputRefs"]:
                self.assertNotIn("*", ref["ref"])
                self.assertNotIn("?", ref["ref"])

    def test_agent_describe_returns_valid_contract(self):
        description = self.payload(self.server._tool_agent_describe())

        self.assertEqual(description["contractVersion"], "1")
        self.assertEqual(description["agentType"], "production")
        self.assertEqual(description["agentInstanceId"], "production-main")
        self.assertEqual(description["displayName"], "Production")
        self.assertEqual(description["health"]["status"], "available")
        self.assertEqual(description["orchestration"]["canPlan"], True)
        self.assertEqual(description["orchestration"]["canExecute"], True)
        self.assertEqual(description["orchestration"]["canCancel"], True)
        self.assertEqual(description["orchestration"]["supportsIdempotency"], True)
        self.assertEqual(description["orchestration"]["supportsParallelWorkers"], True)
        self.assertIn("recommendedConcurrency", description["limits"])
        self.assertIn("maxConcurrency", description["limits"])

        capabilities = {item["id"]: item for item in description["capabilities"]}
        self.assertEqual(capabilities["knowledge.update"]["supportedOperations"], ["ingest", "ingest_plan", "ingest_apply"])
        self.assertEqual(capabilities["document.build"]["supportedOperations"], ["build"])
        self.assertEqual(capabilities["document.publish"]["supportedOperations"], ["export", "polish"])
        self.assertEqual(capabilities["workspace.diagnose"]["supportedOperations"], ["doctor"])
        self.assertEqual(capabilities["knowledge.pipeline"]["supportedOperations"], ["pipeline"])
        self.assertTrue(capabilities["knowledge.update"]["defaultRequiresApproval"])
        self.assertEqual(capabilities["knowledge.update"]["mutationClass"], "workspace")
        self.assertNotIn("defaultRequiresApproval", capabilities["workspace.diagnose"])

    def test_agent_describe_uses_env_instance_and_limits(self):
        server = load_module(
            self.workspace,
            {
                "PRODUCTION_INSTANCE_ID": "production-test",
                "PRODUCTION_RECOMMENDED_CONCURRENCY": "3",
                "PRODUCTION_MAX_CONCURRENCY": "8",
                "PRODUCTION_MAX_TASKS_PER_PLAN": "42",
                "PRODUCTION_MAX_TASK_DURATION_MS": "900000",
            },
        )
        description = self.payload(server._tool_agent_describe())

        self.assertEqual(description["agentInstanceId"], "production-test")
        self.assertEqual(description["limits"]["recommendedConcurrency"], 3)
        self.assertEqual(description["limits"]["maxConcurrency"], 8)
        self.assertEqual(description["limits"]["maxTasksPerPlan"], 42)
        self.assertEqual(description["limits"]["maxTaskDurationMs"], 900000)

    def test_agent_describe_capabilities_follow_allowed_steps(self):
        server = load_module(
            self.workspace,
            {"PRODUCTION_ALLOWED_STEPS": "doctor,ingest,polish,pipeline"},
        )
        description = self.payload(server._tool_agent_describe())
        capabilities = {item["id"]: item for item in description["capabilities"]}

        self.assertEqual(capabilities["knowledge.update"]["supportedOperations"], ["ingest"])
        self.assertNotIn("document.build", capabilities)
        self.assertEqual(capabilities["document.publish"]["supportedOperations"], ["polish"])
        self.assertEqual(capabilities["workspace.diagnose"]["supportedOperations"], ["doctor"])
        self.assertEqual(capabilities["knowledge.pipeline"]["supportedOperations"], ["pipeline"])

    def test_agent_plan_accepts_every_advertised_operation(self):
        # The plan schema used to hard-code five operations while
        # agent_describe advertised doctor/copy/ingest_plan/ingest_apply too,
        # so Donna planning `doctor` got
        # "'doctor' is not one of ['ingest','build','export','polish','pipeline']".
        description = self.payload(self.server._tool_agent_describe())
        advertised = {
            operation
            for capability in description["capabilities"]
            for operation in capability["supportedOperations"]
        }
        plan_enum = set(self.server._agent_plan_input_schema()["properties"]["operation"]["enum"])
        execute_enum = set(self.server._agent_execute_input_schema()["properties"]["operation"]["enum"])

        self.assertIn("doctor", plan_enum)
        self.assertTrue(advertised <= plan_enum, advertised - plan_enum)
        self.assertEqual(plan_enum, execute_enum)

    def test_agent_plan_operations_follow_allowed_steps(self):
        server = load_module(
            self.workspace,
            {"PRODUCTION_ALLOWED_STEPS": "doctor,ingest,polish,pipeline"},
        )
        plan_enum = server._agent_plan_input_schema()["properties"]["operation"]["enum"]

        self.assertEqual(plan_enum, ["doctor", "ingest", "pipeline", "polish"])

    def test_restore_contract_preserves_run_identity_and_capability(self):
        job_args = self.server._agent_start_job_args(
            {
                "taskId": "restore-task",
                "runId": "donna-run-42",
                "capability": "workspace.restore",
                "idempotencyKey": "restore-idem-42",
                "operation": "restore",
                "arguments": {"run": "abc123"},
            },
            {"name": "test-workspace"},
        )

        self.assertEqual(job_args["runId"], "donna-run-42")
        self.assertEqual(job_args["capability"], "workspace.restore")
        self.assertEqual(job_args["idempotencyKey"], "restore-idem-42")
        self.assertEqual(job_args["restoreRun"], "abc123")

        fragment = self.payload(self.server._tool_agent_plan({
            "capability": "workspace.restore",
            "operation": "restore",
            "workspace": {"revision": "rev-restore"},
            "arguments": {"run": "abc123", "dryRun": True},
        }))
        self.assert_task_graph_fragment(fragment)
        self.assertEqual(fragment["tasks"][0]["arguments"], {"run": "abc123", "dryRun": True})
        self.assertEqual(fragment["tasks"][0]["locks"], ["workspace-write"])

        dry_run_job_args = self.server._agent_start_job_args(
            {
                "operation": "restore",
                "capability": "workspace.restore",
                "arguments": {"run": "abc123", "dryRun": True},
            },
            {"name": "test-workspace"},
        )
        self.assertNotIn("dryRun", dry_run_job_args)
        self.assertTrue(dry_run_job_args["executeDryRun"])

    def test_restore_is_never_inferred_from_the_objective_wording(self):
        # workspace.restore overwrites files from a Git revision. Unlike the
        # read-mostly capabilities, it must never be selected from free-text
        # keywords: plenty of unrelated objectives contain "restore".
        for objective in (
            "restore the ingest pipeline after the outage",
            "rollback plan for the documentation",
        ):
            capability, _ = self.server._plan_capability_operation({"objective": objective})
            self.assertNotEqual(capability, "workspace.restore", objective)

        # An explicit operation, or an explicit capability, still selects it.
        self.assertEqual(
            self.server._plan_capability_operation({"operation": "restore"})[0],
            "workspace.restore",
        )
        self.assertEqual(
            self.server._plan_capability_operation(
                {"capability": "workspace.restore", "operation": "restore"}
            )[0],
            "workspace.restore",
        )

    def test_restore_dry_run_preview_contains_exact_command(self):
        payload = self.payload(asyncio.run(self.server._tool_start_job({
            "type": "restore",
            "restoreRun": "abc123",
            "dryRun": True,
        })))

        self.assertEqual(payload["commands"], [
            f"node {self.server._WIKI_BIN} restore --run abc123 --dry-run"
        ])

    def test_agent_restore_dry_run_creates_a_tracked_job(self):
        async def scenario():
            async def hold_job(_job_id):
                await asyncio.Future()

            self.server._run_job = hold_job
            result = self.payload(await self.server._tool_agent_execute({
                "taskId": "restore-dry-run",
                "operation": "restore",
                "capability": "workspace.restore",
                "workspace": {"name": "test-workspace"},
                "arguments": {"run": "abc123", "dryRun": True},
            }))

            self.assertTrue(result["accepted"])
            self.assertIn("jobId", result)
            job = self.server._load_job(result["jobId"])
            self.assertTrue(job["dryRun"])
            await self.server._tool_agent_cancel({"jobId": result["jobId"]})

        asyncio.run(scenario())

    def test_restore_cli_uses_in_memory_job_metadata_and_exact_environment(self):
        captured = {}

        class EmptyStdout:
            async def readline(self):
                return b""

        class SuccessfulProcess:
            stdout = EmptyStdout()

            async def wait(self):
                return 0

        async def create_subprocess_exec(*command, **kwargs):
            captured["command"] = list(command)
            captured["env"] = kwargs["env"]
            return SuccessfulProcess()

        self.server.asyncio.create_subprocess_exec = create_subprocess_exec
        self.server._append_log = lambda *_args, **_kwargs: None
        self.server._load_job = lambda _job_id: self.fail("_run_cli_step must not reload the job")

        exit_code = asyncio.run(self.server._run_cli_step(
            "prod-job-1",
            "restore",
            [],
            [],
            [],
            restore_file="wiki/page.md",
            restore_revision="deadbeef",
            job_metadata={
                "runId": "donna-run-42",
                "callerLabel": "restore-task",
                "capability": "workspace.restore",
                "idempotencyKey": "restore-idem-42",
            },
        ))

        self.assertEqual(exit_code, 0)
        self.assertEqual(captured["command"], [
            "node", self.server._WIKI_BIN, "restore", "--file", "wiki/page.md", "--to", "deadbeef"
        ])
        self.assertEqual(captured["env"]["WIKI_RUN_ID"], "donna-run-42")
        self.assertEqual(captured["env"]["WIKI_TASK_ID"], "restore-task")
        self.assertEqual(captured["env"]["WIKI_CAPABILITY"], "workspace.restore")
        self.assertEqual(captured["env"]["WIKI_IDEMPOTENCY_KEY"], "restore-idem-42")

    def test_ingest_plan_falls_back_to_one_executable_task_when_parallel_helpers_are_disabled(self):
        server = load_module(
            self.workspace,
            {"PRODUCTION_ALLOWED_STEPS": "doctor,ingest,polish,pipeline"},
        )
        source = self.workspace / "raw" / "untracked" / "brief.md"
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text("# Brief\n", encoding="utf-8")

        fragment = server._plan_knowledge_update(
            "ingest",
            {},
            server._planning_constraints({"maxConcurrency": 4}),
            "revision-1",
        )

        self.assertEqual(len(fragment["tasks"]), 1)
        self.assertEqual(fragment["tasks"][0]["operation"], "ingest")
        self.assertFalse(fragment["tasks"][0]["parallelizable"])

    def test_agent_describe_is_listed_and_callable(self):
        tools = asyncio.run(self.server.list_tools())
        self.assertIn("agent_describe", [tool.name for tool in tools])

        payload = self.payload(asyncio.run(self.server.call_tool("agent_describe", {})))
        self.assertEqual(payload["agentType"], "production")

    def test_agent_status_discovers_pending_capability_inputs_recursively(self):
        source = self.workspace / "raw" / "untracked"
        (source / "nested").mkdir(parents=True)
        (source / "b.md").write_text("# B\n", encoding="utf-8")
        (source / "nested" / "a.md").write_text("# A\n", encoding="utf-8")
        (source / "ignored.txt").write_text("ignored\n", encoding="utf-8")

        status = self.payload(self.server._tool_agent_status({
            "capability": "knowledge.update",
            "operation": "ingest",
        }))

        self.assertEqual(status["contractVersion"], "1")
        self.assertEqual(status["agentInstanceId"], "production-main")
        self.assertEqual(status["capability"], "knowledge.update")
        self.assertEqual(status["operation"], "ingest")
        self.assertTrue(status["available"])
        self.assertEqual(
            [item["ref"] for item in status["pendingInputs"]],
            ["raw/untracked/b.md", "raw/untracked/nested/a.md"],
        )
        self.assertEqual(status["pendingInputs"][0]["mediaType"], "text/markdown")

    def test_agent_status_keeps_job_status_schema_and_rejects_unknown_discovery(self):
        schema = self.server._agent_status_input_schema()
        self.assertEqual(len(schema["oneOf"]), 2)
        with self.assertRaisesRegex(ValueError, "not supported"):
            self.server._tool_agent_status({"capability": "document.build", "operation": "build"})

    def test_agent_plan_ingest_scans_untracked_and_checkpoints_each_source(self):
        (self.workspace / "raw" / "untracked").mkdir(parents=True)
        (self.workspace / "raw" / "untracked" / "b.md").write_text("# B\n", encoding="utf-8")
        (self.workspace / "raw" / "untracked" / "a.md").write_text("# A\n", encoding="utf-8")

        fragment = self.payload(
            self.server._tool_agent_plan(
                {
                    "capability": "knowledge.update",
                    "operation": "ingest",
                    "workspace": {"revision": "rev-1"},
                    "constraints": {"maxConcurrency": 2, "requireApprovalForMutations": True},
                }
            )
        )

        self.assert_task_graph_fragment(fragment)
        self.assertEqual(fragment["capability"], "knowledge.update")
        self.assertEqual(
            [task["operation"] for task in fragment["tasks"]],
            ["ingest_plan", "ingest_plan", "ingest_apply", "ingest_apply"],
        )
        self.assertNotIn("export", [task["operation"] for task in fragment["tasks"]])
        self.assertEqual(fragment["groups"][0]["id"], "ingest")
        self.assertEqual(fragment["groups"][0]["recommendedConcurrency"], 2)
        apply_tasks = fragment["tasks"][2:]
        self.assertTrue(all(task["barrier"] for task in apply_tasks))
        # The write lock conflicts with both applies and still-running planning
        # read locks: wait for the whole ingest group, and let the lock — not a
        # dependency chain — serialize the writes. Chaining applies to each
        # other made one unreadable source file skip every apply behind it.
        self.assertTrue(all(task["dependsOnGroup"] == "ingest" for task in apply_tasks))
        self.assertEqual(apply_tasks[0]["dependsOn"], [fragment["tasks"][0]["id"]])
        self.assertEqual(apply_tasks[1]["dependsOn"], [fragment["tasks"][1]["id"]])
        self.assertTrue(all("workspace-write" in task["locks"] for task in apply_tasks))
        self.assertEqual(apply_tasks[0]["arguments"]["inputs"], [fragment["tasks"][0]["expectedOutputRefs"][0]["ref"]])
        self.assertEqual(apply_tasks[1]["arguments"]["inputs"], [fragment["tasks"][1]["expectedOutputRefs"][0]["ref"]])
        # Labels name the effect, not the CLI flag: the two tasks a user sees
        # per file must read as "analyzed" then "written".
        self.assertEqual(
            [task["label"] for task in fragment["tasks"][:2]],
            ["Analyze a.md", "Analyze b.md"],
        )
        self.assertEqual(
            [task["label"] for task in apply_tasks],
            ["Write a.md to the wiki", "Write b.md to the wiki"],
        )
        self.assertEqual(fragment["groups"][0]["label"], "Analyze sources")
        self.assertTrue(all(task["locks"] == ["workspace-write"] for task in apply_tasks))
        self.assertEqual([task["priority"] for task in apply_tasks], [1, 2])
        self.assertTrue(all(task["requiresApproval"] for task in fragment["tasks"]))
        self.assertTrue(all(len(task["idempotencyKey"]) == 64 for task in fragment["tasks"]))
        self.assertEqual(fragment["tasks"][0]["inputRefs"][0]["ref"], "raw/untracked/a.md")
        self.assertEqual(fragment["tasks"][1]["inputRefs"][0]["ref"], "raw/untracked/b.md")
        self.assertEqual(fragment["tasks"][0]["locks"], ["ingest-plan:raw/untracked/a.md"])
        self.assertEqual(fragment["tasks"][1]["locks"], ["ingest-plan:raw/untracked/b.md"])
        self.assertTrue(all(task["retryPolicy"]["maxAttempts"] == 3 for task in fragment["tasks"]))
        self.assertTrue(all("execution_failed" in task["retryPolicy"]["retryableErrors"] for task in fragment["tasks"]))

    def test_agent_status_preserves_detailed_build_progress(self):
        job = {
            "jobId": "job-build",
            "type": "build",
            "status": "running",
            "workspace": "test-workspace",
        }
        progress = {
            "percent": 47,
            "phase": "build",
            "detail": "Batch 2/4 · LLM throttled",
            "currentStep": "build",
            "batchIndex": 1,
            "batchCount": 4,
            "lastEvent": "provider:throttle",
            "waitMs": 1200,
            "retryAt": "2026-07-21T10:00:01Z",
            "currentBatchStartedAt": "2026-07-21T10:00:00Z",
            "traceFile": ".wiki/logs/build.trace",
        }

        payload = self.server._agent_task_status(job, progress)

        status_progress = payload["progress"]
        for key, value in progress.items():
            self.assertEqual(status_progress[key], value)
        self.assertEqual(status_progress["stepIndex"], None)
        self.assertEqual(status_progress["stepTotal"], 0)
        self.assertEqual(status_progress["steps"], [])
        self.assertEqual(status_progress["batch"], {"index": 2, "total": 4, "status": "running", "startedAt": "2026-07-21T10:00:00Z"})
        self.assertEqual(status_progress["throttling"]["active"], True)

    def test_agent_plan_empty_ingest_returns_empty_fragment(self):
        fragment = self.payload(
            self.server._tool_agent_plan(
                {
                    "capability": "knowledge.update",
                    "operation": "ingest",
                    "workspace": {"revision": "rev-empty"},
                }
            )
        )

        self.assert_task_graph_fragment(fragment)
        self.assertEqual(fragment["tasks"], [])
        self.assertIn("No Markdown files", fragment["summary"]["initialSynthesis"][0])

    def test_agent_plan_build_creates_one_task_per_template(self):
        (self.workspace / "templates" / "a.md").write_text("---\noutput: alpha.md\n---\n# A\n", encoding="utf-8")
        (self.workspace / "templates" / "nested").mkdir()
        (self.workspace / "templates" / "nested" / "b.md").write_text("# B\n", encoding="utf-8")

        fragment = self.payload(
            self.server._tool_agent_plan(
                {
                    "capability": "document.build",
                    "operation": "build",
                    "workspace": {"revision": "rev-build"},
                    "constraints": {"requireApprovalForMutations": True},
                }
            )
        )

        self.assert_task_graph_fragment(fragment)
        self.assertEqual([task["operation"] for task in fragment["tasks"]], ["build", "build"])
        self.assertEqual([task["inputRefs"][0]["ref"] for task in fragment["tasks"]], ["templates/a.md", "templates/nested/b.md"])
        self.assertEqual(
            [task["expectedOutputRefs"][0]["ref"] for task in fragment["tasks"]],
            ["deliverables/alpha.md", "deliverables/nested/b.md"],
        )
        self.assertTrue(all(task["requiresApproval"] for task in fragment["tasks"]))
        self.assertNotIn("export", [task["operation"] for task in fragment["tasks"]])

    def test_agent_plan_publish_creates_one_task_per_deliverable(self):
        (self.workspace / "deliverables" / "a.md").write_text("# A\n", encoding="utf-8")
        (self.workspace / "deliverables" / "b.md").write_text("# B\n", encoding="utf-8")

        fragment = self.payload(
            self.server._tool_agent_plan(
                {
                    "capability": "document.publish",
                    "operation": "export",
                    "workspace": {"revision": "rev-publish"},
                    "constraints": {"maxConcurrency": 3},
                }
            )
        )

        self.assert_task_graph_fragment(fragment)
        self.assertEqual([task["operation"] for task in fragment["tasks"]], ["export", "export"])
        self.assertEqual([task["inputRefs"][0]["ref"] for task in fragment["tasks"]], ["deliverables/a.md", "deliverables/b.md"])
        self.assertEqual(fragment["tasks"][0]["locks"], ["deliverable:deliverables/a.md"])
        self.assertEqual(fragment["groups"][0]["recommendedConcurrency"], 3)

    def test_agent_plan_pipeline_full_scenario(self):
        (self.workspace / "raw" / "untracked").mkdir(parents=True)
        (self.workspace / "raw" / "untracked" / "source.md").write_text("# Source\n", encoding="utf-8")
        (self.workspace / "templates" / "report.md").write_text("---\noutput: report.md\n---\n# Report\n", encoding="utf-8")

        fragment = self.payload(
            self.server._tool_agent_plan(
                {
                    "capability": "knowledge.pipeline",
                    "operation": "pipeline",
                    "workspace": {"revision": "rev-pipeline"},
                    "constraints": {"requireApprovalForMutations": True},
                }
            )
        )

        self.assert_task_graph_fragment(fragment)
        operations = [task["operation"] for task in fragment["tasks"]]
        self.assertEqual(operations, ["ingest_plan", "ingest_apply", "build", "export", "polish"])
        build = next(task for task in fragment["tasks"] if task["operation"] == "build")
        export = next(task for task in fragment["tasks"] if task["operation"] == "export")
        polish = next(task for task in fragment["tasks"] if task["operation"] == "polish")
        self.assertIn("ingest-apply", build["dependsOn"])
        self.assertIn(build["id"], export["dependsOn"])
        self.assertIn(export["id"], polish["dependsOn"])
        self.assertEqual(export["inputRefs"][0]["ref"], "deliverables/report.md")

    def test_agent_plan_pipeline_respects_requested_steps(self):
        (self.workspace / "raw" / "untracked").mkdir(parents=True)
        (self.workspace / "raw" / "untracked" / "source.md").write_text("# Source\n", encoding="utf-8")
        (self.workspace / "templates" / "report.md").write_text("# Report\n", encoding="utf-8")

        fragment = self.payload(
            self.server._tool_agent_plan(
                {
                    "capability": "knowledge.pipeline",
                    "operation": "pipeline",
                    "workspace": {"revision": "rev-pipeline-steps"},
                    "arguments": {"steps": ["ingest"]},
                }
            )
        )

        self.assert_task_graph_fragment(fragment)
        operations = [task["operation"] for task in fragment["tasks"]]
        self.assertEqual(operations, ["ingest_plan", "ingest_apply"])
        self.assertNotIn("build", operations)
        self.assertNotIn("export", operations)

    def test_pipeline_keeps_each_ingest_apply_independent(self):
        pending = self.workspace / "raw" / "untracked"
        pending.mkdir(parents=True)
        (pending / "a.md").write_text("# A\n", encoding="utf-8")
        (pending / "b.md").write_text("# B\n", encoding="utf-8")

        fragment = self.payload(
            self.server._tool_agent_plan(
                {
                    "capability": "knowledge.pipeline",
                    "operation": "pipeline",
                    "workspace": {"revision": "rev-pipeline-serial-apply"},
                    "arguments": {"steps": ["ingest"]},
                    "constraints": {"maxConcurrency": 2},
                }
            )
        )

        plan_tasks = [task for task in fragment["tasks"] if task["operation"] == "ingest_plan"]
        apply_tasks = [task for task in fragment["tasks"] if task["operation"] == "ingest_apply"]
        self.assertEqual(len(plan_tasks), 2)
        self.assertEqual(len(apply_tasks), 2)
        # La barrière de groupe attend la fin de TOUTE l'analyse parallèle.
        self.assertTrue(all(task["dependsOnGroup"] == "ingest" for task in apply_tasks))
        # Chaque apply ne dépend que de son propre plan. Les enchaîner les uns
        # aux autres était une ceinture par-dessus des bretelles : un apply
        # ignoré parce que son plan a échoué faisait hériter la même
        # impossibilité à tous les suivants, et un seul fichier illisible
        # laissait neuf fichiers valides non écrits.
        self.assertEqual(apply_tasks[0]["dependsOn"], [plan_tasks[0]["id"]])
        self.assertEqual(apply_tasks[1]["dependsOn"], [plan_tasks[1]["id"]])
        # La sérialisation des écritures reste garantie, mais par le verrou :
        # deux apply ne peuvent pas détenir workspace-write en même temps.
        for task in apply_tasks:
            self.assertIn("workspace-write", task["locks"])

    def test_a_failed_analysis_does_not_strand_the_other_applies(self):
        """A skipped apply must not take its siblings down with it.

        Observed 2026-08-04: ten sources, nine analyzed, one producing
        malformed JSON. The applies were chained, so the apply of the failed
        source blocked every apply behind it and nine valid files were never
        written. Each apply now depends on its own analysis only.
        """
        pending = self.workspace / "raw" / "untracked"
        pending.mkdir(parents=True)
        for name in ("a.md", "b.md", "c.md"):
            (pending / name).write_text(f"# {name}\n", encoding="utf-8")

        fragment = self.payload(
            self.server._tool_agent_plan(
                {
                    "capability": "knowledge.update",
                    "operation": "ingest",
                    "workspace": {"revision": "rev-partial-ingest"},
                    "arguments": {},
                    "constraints": {"maxConcurrency": 3},
                }
            )
        )
        plan_tasks = [task for task in fragment["tasks"] if task["operation"] == "ingest_plan"]
        apply_tasks = [task for task in fragment["tasks"] if task["operation"] == "ingest_apply"]
        self.assertEqual(len(plan_tasks), 3)
        self.assertEqual(len(apply_tasks), 3)

        apply_ids = {task["id"] for task in apply_tasks}
        for apply_task, plan_task in zip(apply_tasks, plan_tasks, strict=True):
            # No apply references another apply: whichever analysis fails, only
            # its own apply becomes impossible.
            self.assertEqual(apply_task["dependsOn"], [plan_task["id"]])
            self.assertFalse(apply_ids.intersection(apply_task["dependsOn"]))

        # Writes stay strictly serialized, by the lock rather than by the DAG.
        self.assertTrue(all("workspace-write" in task["locks"] for task in apply_tasks))
        # And every apply still waits for the whole analysis group, so no write
        # starts while a read lock is still held.
        self.assertTrue(all(task["dependsOnGroup"] == "ingest" for task in apply_tasks))

    def test_agent_plan_aggregates_when_max_tasks_is_exceeded(self):
        (self.workspace / "templates" / "a.md").write_text("# A\n", encoding="utf-8")
        (self.workspace / "templates" / "b.md").write_text("# B\n", encoding="utf-8")

        fragment = self.payload(
            self.server._tool_agent_plan(
                {
                    "capability": "document.build",
                    "operation": "build",
                    "workspace": {"revision": "rev-aggregate"},
                    "constraints": {"maxTasks": 1},
                }
            )
        )

        self.assert_task_graph_fragment(fragment)
        self.assertEqual(len(fragment["tasks"]), 1)
        self.assertEqual(fragment["tasks"][0]["id"], "build-batch")
        self.assertEqual(fragment["tasks"][0]["arguments"]["templates"], ["templates/a.md", "templates/b.md"])
        self.assertEqual(fragment["tasks"][0]["progressWeight"], 2)

    def test_agent_plan_is_listed_and_callable(self):
        tools = asyncio.run(self.server.list_tools())
        self.assertIn("agent_plan", [tool.name for tool in tools])

        payload = self.payload(
            asyncio.run(
                self.server.call_tool(
                    "agent_plan",
                    {
                        "capability": "knowledge.update",
                        "operation": "ingest",
                        "workspace": {"revision": "rev-call"},
                    },
                )
            )
        )
        self.assertEqual(payload["capability"], "knowledge.update")

    def test_agent_execute_status_done_on_doctor(self):
        async def scenario():
            async def successful_cli_step(
                _job_id,
                _step,
                _inputs,
                _templates,
                _deliverables,
                stabilize=False,
                config_path=None,
                job_metadata=None,
            ):
                return 0

            self.server._run_cli_step = successful_cli_step
            accepted = self.payload(
                await self.server._tool_agent_execute(
                    {
                        "operation": "doctor",
                        "workspace": {"name": "requested-workspace"},
                    }
                )
            )

            self.assertTrue(accepted["accepted"])
            await self.server._ACTIVE_TASKS[accepted["jobId"]]
            status = self.payload(self.server._tool_agent_status({"jobId": accepted["jobId"]}))

            self.assertEqual(status["status"], "done")
            self.assertEqual(status["progress"]["percent"], 100)
            self.assertEqual(status["result"]["status"], "succeeded")
            self.assertIn("durationMs", status["result"]["metrics"])

        asyncio.run(scenario())

    def test_agent_cancel_cancels_one_job(self):
        async def scenario():
            async def hold_job(_job_id):
                await asyncio.Future()

            self.server._run_job = hold_job
            accepted = self.payload(
                await self.server._tool_agent_execute(
                    {
                        "operation": "doctor",
                        "workspace": {"name": "requested-workspace"},
                    }
                )
            )

            status = self.payload(await self.server._tool_agent_cancel({"jobId": accepted["jobId"]}))

            self.assertEqual(status["jobId"], accepted["jobId"])
            self.assertEqual(status["status"], "cancelled")
            self.assertEqual(status["result"]["status"], "cancelled")

        asyncio.run(scenario())

    def test_agent_execute_uses_request_workspace_name(self):
        async def scenario():
            async def hold_job(_job_id):
                await asyncio.Future()

            self.server._run_job = hold_job
            accepted = self.payload(
                await self.server._tool_agent_execute(
                    {
                        "operation": "doctor",
                        "workspace": {"name": "request-wins"},
                    }
                )
            )
            job = self.server._load_job(accepted["jobId"])
            status = self.payload(self.server._tool_agent_status({"jobId": accepted["jobId"]}))

            self.assertEqual(job["workspace"], "request-wins")
            self.assertEqual(status["workspace"], {"name": "request-wins"})
            await self.server._tool_agent_cancel({"jobId": accepted["jobId"]})

        asyncio.run(scenario())

    def test_agent_execute_same_idempotency_key_reuses_active_job(self):
        async def scenario():
            async def hold_job(_job_id):
                await asyncio.Future()

            self.server._run_job = hold_job
            request = {
                "taskId": "task-idempotent-active",
                "idempotencyKey": "idem-active",
                "operation": "doctor",
                "workspace": {"name": "requested-workspace"},
            }
            first = self.payload(await self.server._tool_agent_execute(request))
            second = self.payload(await self.server._tool_agent_execute(request))

            self.assertTrue(first["accepted"])
            self.assertTrue(second["accepted"])
            self.assertEqual(first["jobId"], second["jobId"])
            self.assertEqual(second["idempotent"], True)
            self.assertEqual(len(list((self.server._JOBS_DIR / "jobs").glob("*.json"))), 1)
            await self.server._tool_agent_cancel({"jobId": first["jobId"]})

        asyncio.run(scenario())

    def test_agent_execute_lost_response_reuses_finished_idempotent_job(self):
        async def scenario():
            async def successful_cli_step(
                _job_id,
                _step,
                _inputs,
                _templates,
                _deliverables,
                stabilize=False,
                config_path=None,
                job_metadata=None,
            ):
                return 0

            self.server._run_cli_step = successful_cli_step
            request = {
                "taskId": "task-idempotent-done",
                "idempotencyKey": "idem-done",
                "operation": "doctor",
                "workspace": {"name": "requested-workspace"},
            }
            first = self.payload(await self.server._tool_agent_execute(request))
            await self.server._ACTIVE_TASKS[first["jobId"]]
            second = self.payload(await self.server._tool_agent_execute(request))

            self.assertEqual(first["jobId"], second["jobId"])
            self.assertEqual(second["idempotent"], True)
            self.assertEqual(second["terminal"], True)
            self.assertEqual(second["result"]["status"], "succeeded")
            self.assertEqual(len(list((self.server._JOBS_DIR / "jobs").glob("*.json"))), 1)

        asyncio.run(scenario())

    def test_agent_status_failed_job_includes_task_result_error(self):
        async def scenario():
            async def failing_cli_step(
                _job_id,
                _step,
                _inputs,
                _templates,
                _deliverables,
                stabilize=False,
                config_path=None,
                job_metadata=None,
            ):
                return 2

            self.server._run_cli_step = failing_cli_step
            accepted = self.payload(
                await self.server._tool_agent_execute(
                    {
                        "operation": "doctor",
                        "workspace": {"name": "requested-workspace"},
                    }
                )
            )

            await self.server._ACTIVE_TASKS[accepted["jobId"]]
            status = self.payload(self.server._tool_agent_status({"jobId": accepted["jobId"]}))

            self.assertEqual(status["status"], "failed")
            self.assertEqual(status["result"]["status"], "failed")
            self.assertEqual(status["result"]["error"]["code"], "execution_failed")
            self.assertIs(status["result"]["error"]["retryable"], False)

        asyncio.run(scenario())

    def test_agent_execute_status_cancel_are_listed_and_callable(self):
        async def scenario():
            async def hold_job(_job_id):
                await asyncio.Future()

            self.server._run_job = hold_job
            tools = await self.server.list_tools()
            names = [tool.name for tool in tools]
            self.assertIn("agent_execute", names)
            self.assertIn("agent_status", names)
            self.assertIn("agent_cancel", names)

            accepted = self.payload(
                await self.server.call_tool(
                    "agent_execute",
                    {
                        "operation": "doctor",
                        "workspace": {"name": "requested-workspace"},
                    },
                )
            )
            status = self.payload(await self.server.call_tool("agent_status", {"jobId": accepted["jobId"]}))
            cancelled = self.payload(await self.server.call_tool("agent_cancel", {"jobId": accepted["jobId"]}))

            self.assertTrue(accepted["accepted"])
            self.assertEqual(status["status"], "queued")
            self.assertEqual(cancelled["status"], "cancelled")

        asyncio.run(scenario())

    def test_start_job_dry_run_returns_plan_without_confirmation(self):
        result = asyncio.run(self.server._tool_start_job({"type": "doctor", "dryRun": True}))
        payload = self.payload(result)
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["dryRun"])
        self.assertEqual(payload["plan"]["steps"], ["doctor"])

    def test_ingest_dry_run_accepts_target_inputs(self):
        result = asyncio.run(
            self.server._tool_start_job(
                {
                    "type": "ingest",
                    "inputs": ["raw/untracked/doc-a.md", "doc-b.md"],
                    "dryRun": True,
                }
            )
        )
        payload = self.payload(result)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["plan"]["inputs"], ["raw/untracked/doc-a.md", "doc-b.md"])
        self.assertIn("node /app/bin/wiki.js ingest raw/untracked/doc-a.md doc-b.md", payload["commands"])

    def test_ingest_dry_run_expands_input_globs(self):
        (self.workspace / "raw" / "untracked").mkdir(parents=True)
        (self.workspace / "raw" / "untracked" / "b.md").write_text("# B\n", encoding="utf-8")
        (self.workspace / "raw" / "untracked" / "a.md").write_text("# A\n", encoding="utf-8")
        result = asyncio.run(
            self.server._tool_start_job(
                {
                    "type": "ingest",
                    "inputs": ["raw/untracked/*.md"],
                    "dryRun": True,
                }
            )
        )
        payload = self.payload(result)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["plan"]["inputs"], ["raw/untracked/a.md", "raw/untracked/b.md"])

    def test_ingest_dry_run_rejects_unmatched_input_globs(self):
        with self.assertRaisesRegex(ValueError, r"No files match raw/untracked/\*.md"):
            asyncio.run(
                self.server._tool_start_job(
                    {
                        "type": "ingest",
                        "inputs": ["raw/untracked/*.md"],
                        "dryRun": True,
                    }
                )
            )

    def test_ingest_plan_dry_run_accepts_target_inputs_without_workspace_write_lock(self):
        result = asyncio.run(
            self.server._tool_start_job(
                {
                    "type": "ingest_plan",
                    "inputs": ["raw/untracked/doc-a.md", "doc-b.md"],
                    "dryRun": True,
                }
            )
        )
        payload = self.payload(result)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["plan"]["steps"], ["ingest_plan"])
        self.assertEqual(payload["plan"]["lockScopes"], ["read"])
        self.assertIn("node /app/bin/wiki.js ingest --plan-only raw/untracked/doc-a.md doc-b.md", payload["commands"])

    def test_ingest_apply_dry_run_accepts_plan_files_with_workspace_write_lock(self):
        result = asyncio.run(
            self.server._tool_start_job(
                {
                    "type": "ingest_apply",
                    "inputs": [".wiki/ingest-plans/plan-a.json"],
                    "dryRun": True,
                }
            )
        )
        payload = self.payload(result)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["plan"]["steps"], ["ingest_apply"])
        self.assertEqual(payload["plan"]["lockScopes"], ["workspace-write"])
        self.assertIn("node /app/bin/wiki.js ingest --apply .wiki/ingest-plans/plan-a.json", payload["commands"])

    def test_mutating_job_requires_confirmation(self):
        with self.assertRaisesRegex(ValueError, "confirm=true"):
            asyncio.run(self.server._tool_start_job({"type": "build"}))

    def test_targeted_build_jobs_can_run_in_parallel_but_conflicting_export_waits(self):
        async def scenario():
            async def hold_job(_job_id):
                await asyncio.Future()

            self.server._run_job = hold_job
            (self.workspace / "templates" / "a.md").write_text("---\noutput: a.md\n---\n# A\n", encoding="utf-8")
            (self.workspace / "templates" / "b.md").write_text("---\noutput: b.md\n---\n# B\n", encoding="utf-8")

            first = self.payload(await self.server._tool_start_job({"type": "build", "templates": ["a.md"], "confirm": True}))
            second = self.payload(await self.server._tool_start_job({"type": "build", "templates": ["b.md"], "confirm": True}))
            conflict = self.payload(await self.server._tool_start_job({"type": "export", "deliverables": ["a.md"], "confirm": True}))

            self.assertTrue(first["ok"])
            self.assertTrue(second["ok"])
            self.assertFalse(conflict["ok"])
            self.assertEqual(conflict["error"], "target_busy")
            self.assertEqual(first["plan"]["lockScopes"], ["deliverable:deliverables/a.md"])
            self.assertEqual(second["plan"]["lockScopes"], ["deliverable:deliverables/b.md"])

            await self.server._tool_cancel_job({"jobId": first["jobId"]})
            await self.server._tool_cancel_job({"jobId": second["jobId"]})

        asyncio.run(scenario())

    def test_targeted_build_jobs_are_faster_in_parallel(self):
        async def scenario():
            async def slow_cli_step(
                _job_id,
                _step,
                _inputs,
                _templates,
                _deliverables,
                stabilize=False,
                config_path=None,
                job_metadata=None,
            ):
                await asyncio.sleep(0.5)
                return 0

            async def run_build(template):
                payload = self.payload(
                    await self.server._tool_start_job(
                        {"type": "build", "templates": [template], "confirm": True}
                    )
                )
                self.assertTrue(payload["ok"])
                task = self.server._ACTIVE_TASKS[payload["jobId"]]
                await task
                return payload["jobId"]

            self.server._run_cli_step = slow_cli_step
            (self.workspace / "templates" / "a.md").write_text("---\noutput: a.md\n---\n# A\n", encoding="utf-8")
            (self.workspace / "templates" / "b.md").write_text("---\noutput: b.md\n---\n# B\n", encoding="utf-8")

            sequential_start = time.perf_counter()
            await run_build("a.md")
            await run_build("b.md")
            sequential_seconds = time.perf_counter() - sequential_start

            parallel_start = time.perf_counter()
            await asyncio.gather(run_build("a.md"), run_build("b.md"))
            parallel_seconds = time.perf_counter() - parallel_start

            self.assertLess(parallel_seconds, sequential_seconds * 0.65)

        asyncio.run(scenario())

    def test_log_redaction_masks_secret_values(self):
        masked = self.server._mask_secret_text("Authorization: Bearer abc123 token=runtime password:secret")
        self.assertNotIn("abc123", masked)
        self.assertNotIn("runtime", masked)
        self.assertNotIn("secret", masked)

    def test_read_scope_cannot_start_or_cancel_job(self):
        token = self.server._CURRENT_SCOPES.set({"read"})
        try:
            denied = self.server._require_tool_scope("production_start_job")
            allowed = self.server._require_tool_scope("production_status")
        finally:
            self.server._CURRENT_SCOPES.reset(token)

        self.assertFalse(self.payload(denied)["ok"])
        self.assertIn("write scope", self.payload(denied)["error"])
        self.assertIsNone(allowed)


if __name__ == "__main__":
    unittest.main()
