import base64
import json
import os
import pathlib
import random
import string
import subprocess
import sys
from datetime import datetime
from typing import Any, Dict, Union

import dotenv
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobSettings
from databricks.sdk.service.workspace import ImportFormat

# Get the project root directory (3 levels up from this file)
PROJECT_ROOT = str(pathlib.Path(__file__).parent.parent.parent.parent.absolute())


def merge_configs(
    config: Dict[str, Any], override_config: Dict[str, Any]
) -> Dict[str, Any]:
    for key, value in override_config.items():
        if isinstance(value, dict):
            config[key] = merge_configs(config.get(key, {}), value)
        else:
            config[key] = value
    return config


def resolve_base(config: Dict[str, Any]) -> Dict[str, Any]:
    if "__base__" in config:
        base_config_path = os.path.join(PROJECT_ROOT, "configs", config["__base__"])
        with open(base_config_path, "r") as f:
            base_config = yaml.safe_load(f) or {}
        del config["__base__"]
    else:
        base_config = {}

    return merge_configs(base_config, config)


def load_config(
    env: str,
    client: str,
    workflow: str,
    client_config: Union[Dict[str, Any], None] = None,
) -> Dict[str, Any]:
    # Convert env to lowercase for directory matching
    env_lower = env.lower()

    # Base paths
    config_root = os.path.join(PROJECT_ROOT, "configs")
    base_path = os.path.join(config_root, "base")

    # Load default config
    default_config = {}
    default_file = os.path.join(base_path, "defaults.yaml")
    if os.path.exists(default_file):
        with open(default_file, "r") as f:
            default_config = yaml.safe_load(f) or {}
    default_config = resolve_base(default_config)

    # Load environment config
    env_config = {}
    env_file = os.path.join(base_path, "envs", f"{env_lower}.yaml")
    if os.path.exists(env_file):
        with open(env_file, "r") as f:
            env_config = yaml.safe_load(f) or {}
    env_config = resolve_base(env_config)

    # Load workflow config
    workflow_config = {}
    workflow_file = os.path.join(base_path, "workflows", f"{workflow}.yaml")
    if os.path.exists(workflow_file):
        with open(workflow_file, "r") as f:
            workflow_config = yaml.safe_load(f) or {}
    workflow_config = resolve_base(workflow_config)

    run_config = {
        "environment": env,
        "client": client,
        "workflow": workflow,
    }

    # Merge configs in order of precedence
    # 1. defaults
    # 2. environment
    # 3. client
    # 4. workflow
    # 6. run-time config
    configs = {}
    for config in [
        default_config,
        env_config,
        client_config,
        workflow_config,
        run_config,
    ]:
        configs = merge_configs(configs, config)

    dotenv.load_dotenv(override=True)

    return configs


def resolve_dynamic_configs(config: Any):
    if isinstance(config, dict):
        for key, value in config.items():
            config[key] = resolve_dynamic_configs(value)
    elif isinstance(config, list):
        config = [resolve_dynamic_configs(item) for item in config]
    elif isinstance(config, str) and config.startswith("$"):
        from_env = os.getenv(config[1:])
        config = from_env or config
    return config


def push_secrets(config: Any, scope_name: str, workspace_client: WorkspaceClient):
    if isinstance(config, dict):
        for _, value in config.items():
            push_secrets(value, scope_name, workspace_client)
    elif isinstance(config, list):
        [push_secrets(item, scope_name, workspace_client) for item in config]
    elif isinstance(config, str) and config.startswith("$"):
        from_env = os.getenv(config[1:])
        workspace_client.secrets.put_secret(
            scope_name, config[1:], string_value=(from_env or config)
        )


def extract_all_secret_keys(config: Any):
    all_secret_keys = []
    if isinstance(config, dict):
        for _, value in config.items():
            all_secret_keys.extend(extract_all_secret_keys(value))
    elif isinstance(config, list):
        for item in config:
            all_secret_keys.extend(extract_all_secret_keys(item))
    elif isinstance(config, str) and config.startswith("$"):
        all_secret_keys.append(config[1:])
    return list(set(all_secret_keys))


def build_wheel():
    # Change the working directory to the project root before building the wheel
    current_dir = os.getcwd()
    os.chdir(PROJECT_ROOT)
    try:
        result = subprocess.run(
            [sys.executable, "setup.py", "bdist_wheel"], capture_output=True
        )
        if result.returncode != 0:
            print("Error: Failed to build the wheel.")
            print(result.stderr)
            raise Exception("Failed to build the wheel.")
        wheel_file = os.path.join("dist", sorted(os.listdir("dist"))[-1])
        return os.path.join(PROJECT_ROOT, wheel_file)
    finally:
        os.chdir(current_dir)


def get_deploy_id(env: str):
    temp_suffix = "".join(random.choices(string.ascii_lowercase, k=4))
    return f"{env}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{temp_suffix}"


def upload_wheel_to_databricks(
    workspace_client: WorkspaceClient, wheel_path: str, deploy_id: str
):
    workspace_client.workspace.mkdirs(f"/Workspace/Shared/wheels/{deploy_id}")
    with open(wheel_path, "rb") as f:
        wheel_content = base64.b64encode(f.read()).decode("utf-8")

    # Import the wheel file to Databricks workspace
    wheel_path = f"/Workspace/Shared/wheels/{deploy_id}/{os.path.basename(wheel_path)}"
    workspace_client.workspace.import_(
        path=wheel_path,
        content=wheel_content,
        format=ImportFormat.RAW,
    )
    return wheel_path


def compile(config: Dict[str, Any], workspace_client: WorkspaceClient, wheel_path: str):
    # Read the workflow template
    workflow_template_keys = [
        "tasks",
        "compute",
    ]  # can add more fields to this list
    workflow_template = {key: config.get(key) for key in workflow_template_keys}
    for key in workflow_template_keys:
        if key in config:
            del config[key]
    workflow = {"resources": {"jobs": {}}}
    workflow_name = f"{config['environment']}.{config['client']}.{config['workflow']}"
    config["workflow-name"] = workflow_name
    workflow["resources"]["jobs"][workflow_name] = {
        "name": workflow_name,
        "tasks": [],
        "queue": {"enabled": True},
        "max_concurrent_runs": 1,
        "parameters": [
            {"name": "run-id", "default": "{{job.trigger.time.timestamp_ms}}"},
            {"name": "workflow-config", "default": json.dumps(config)},
            {"name": "runtime-config", "default": json.dumps({})},
        ],
        "job_clusters": [],
    }
    if config.get("notification-emails"):
        workflow["resources"]["jobs"][workflow_name]["email_notifications"] = {
            "on_failure": config["notification-emails"],
        }

    all_secret_keys = extract_all_secret_keys(config)

    default_compute_template = None
    if workflow_template.get("compute") is not None:
        default_compute_template = resolve_compute(
            workflow_template["compute"],
            workflow_name,
            workspace_client,
            all_secret_keys,
            config=config,
        )
        workflow["resources"]["jobs"][workflow_name]["job_clusters"].append(
            default_compute_template
        )

    for task_key, task in workflow_template["tasks"].items():
        conflict_keys = set(config.keys() & task.get("task-config", {}).keys())
        if len(conflict_keys) > 0:
            raise Exception(
                f"Task {task_key} has conflicting keys with config: {conflict_keys}"
            )
            # Note: we don't support common keys in task-config and config

        task_config = task.get("task-config", {})
        if "task-key" in task_config:
            raise Exception(
                f"Task {task_key} has task-key in task-config, which is not allowed"
            )
        task_config["task-key"] = task_key
        task_yaml = {
            "task_key": task_key,
            "python_wheel_task": {
                "package_name": "propel-data-platform",
                "entry_point": "execute_task",
                "named_parameters": {
                    "task-class": task["task-class"],
                    "task-config": json.dumps(task_config),
                },
            },
            "libraries": [{"whl": wheel_path}],
            "depends_on": [
                {
                    "task_key": dep,
                }
                for dep in task.get("depends-on", [])
            ],
        }

        retry_policy = config.get("retry-policy", {}) | task.get("retry-policy", {})
        if retry_policy.get("max-retries") is not None:
            task_yaml["max_retries"] = retry_policy["max-retries"]
        if retry_policy.get("min-retry-interval-millis") is not None:
            task_yaml["min_retry_interval_millis"] = retry_policy[
                "min-retry-interval-millis"
            ]

        if task.get("compute") is not None:
            compute_template = resolve_compute(
                task["compute"],
                workflow_name,
                workspace_client,
                all_secret_keys,
                task=task_key,
                config=config,
            )
            workflow["resources"]["jobs"][workflow_name]["job_clusters"].append(
                compute_template
            )
            task_yaml["job_cluster_key"] = compute_template["job_cluster_key"]
        elif default_compute_template is not None:
            task_yaml["job_cluster_key"] = default_compute_template["job_cluster_key"]
        else:
            raise Exception(f"No compute defined for task {task_key}")

        workflow["resources"]["jobs"][workflow_name]["tasks"].append(task_yaml)

    return workflow, workflow_name


def resolve_compute(
    compute: str,
    workflow_name: str,
    workspace_client: WorkspaceClient,
    all_secret_keys: list,
    task: str = "default",
    config: Dict[str, Any] = {},
):
    compute_template_path = os.path.join(
        PROJECT_ROOT, "configs", "compute", f"{compute}.yaml"
    )
    with open(compute_template_path, "r") as f:
        compute_template = yaml.safe_load(f) or {}
        compute_template["single_user_name"] = (
            workspace_client.current_user.me().emails[0].value
        )

        compute_template["custom_tags"] = compute_template.get("custom_tags", {})
        compute_template["custom_tags"]["PropelClient"] = config.get(
            "client", "Unknown"
        )
        compute_template["custom_tags"]["PropelWorkflow"] = config.get(
            "workflow", "Unknown"
        )
        compute_template["custom_tags"]["PropelEnvironment"] = config.get(
            "environment", "Unknown"
        )

        compute_template["spark_env_vars"] = compute_template.get("spark_env_vars", {})
        for key in all_secret_keys:
            compute_template["spark_env_vars"][f"PRPL_{key}"] = (
                "{{secrets/" + workflow_name + "/" + key + "}}"
            )
        compute_template = {
            "job_cluster_key": workflow_name + "." + task,
            "new_cluster": compute_template,
        }
        compute_template["job_cluster_key"] = "".join(
            [c if c.isalnum() else "_" for c in compute_template["job_cluster_key"]]
        )
    return compute_template


def deploy_one_workflow(
    env: str,
    client: str,
    workflow: str,
    deploy_id: str = None,
    wheel_path: str = None,
    run_now: bool = False,
    client_config_override: Union[Dict[str, Any], None] = None,
    runtime_config: str = "{}",
):
    print(f"Deploying workflow '{workflow}' for client '{client}' to {env} environment")
    config = load_config(env, client, workflow, client_config_override)

    workspace_client = WorkspaceClient(
        host=resolve_dynamic_configs(config.get("databricks-host")),
        token=resolve_dynamic_configs(config.get("databricks-token")),
    )

    if deploy_id is None:
        wheel_file = build_wheel()
        deploy_id = get_deploy_id(env)
        wheel_path = upload_wheel_to_databricks(
            workspace_client,
            wheel_file,
            deploy_id,
        )
        print(f"Wheel uploaded to {wheel_path}")

    workflow_yaml, workflow_name = compile(config, workspace_client, wheel_path)

    job_kwargs = JobSettings.from_dict(
        workflow_yaml["resources"]["jobs"][workflow_name]
    )
    exists, job_id = False, None
    for existing_job in workspace_client.jobs.list():
        if existing_job.settings.name == workflow_name:
            exists, job_id = True, existing_job.job_id

    if exists:
        existing_job_object = workspace_client.jobs.get(job_id)
        fields_to_remove = set(
            existing_job_object.settings.as_shallow_dict().keys()
        ) - set(job_kwargs.as_shallow_dict().keys())
        workspace_client.jobs.update(
            existing_job_object.job_id, fields_to_remove=fields_to_remove
        )
        workspace_client.jobs.reset(existing_job_object.job_id, new_settings=job_kwargs)
    else:
        job_response = workspace_client.jobs.create(**job_kwargs.as_shallow_dict())
        job_id = job_response.job_id

    # upload secrets to databricks
    all_scopes = workspace_client.secrets.list_scopes()
    if workflow_name not in [scope.name for scope in all_scopes]:
        workspace_client.secrets.create_scope(scope=workflow_name)
    push_secrets(config, workflow_name, workspace_client)

    run_id = None
    if run_now:
        run_id = workspace_client.jobs.run_now(
            job_id, job_parameters={"runtime-config": runtime_config}
        ).run_id
        print(
            f"Workflow {workflow_name} run triggered successfully with run ID: {run_id}"
        )

    return deploy_id, wheel_path, run_id
