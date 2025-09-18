import argparse
import importlib
import json
import os
from typing import Any

from propel_data_platform.workflows import merge_configs


def execute_task():

    # parse args
    parser = argparse.ArgumentParser(description="Run a task")
    parser.add_argument("--task-class", type=str, required=True, help="Task class")
    parser.add_argument("--task-config", type=str, required=True, help="Task config")
    parser.add_argument("--run-id", type=str, required=True, help="Run ID")
    parser.add_argument(
        "--workflow-config", type=str, required=True, help="Workflow config"
    )
    parser.add_argument(
        "--runtime-config", type=str, required=True, help="Runtime config"
    )
    args = parser.parse_args()

    # load configs
    runtime_config = json.loads(args.runtime_config)
    task_config = json.loads(args.task_config)
    workflow_config = json.loads(args.workflow_config)

    execute_task_fn(
        runtime_config, task_config, workflow_config, args.task_class, args.run_id
    )


def execute_task_fn(
    runtime_config, task_config, workflow_config, task_class, run_id, local=False
):
    # merge configs
    task_key = task_config["task-key"]
    if task_key in runtime_config.get("tasks", {}):
        task_config = merge_configs(
            task_config, runtime_config["tasks"][task_key].get("task-config", {})
        )
        del runtime_config["tasks"]
    workflow_config = merge_configs(workflow_config, runtime_config)
    config = workflow_config | task_config
    config = resolve_dynamic_configs(config, config["workflow-name"], local)

    # resolve task class
    if task_class == "RunConnectorTask":
        task_class = "connector"
    elif task_class == "RunIngestorTask":
        task_class = "ingestor"
    elif task_class == "RunTransformationTask":
        task_class = "transformation"
    elif task_class == "RunCleanerTask":
        task_class = "cleaner"
    else:
        raise ValueError(f"Invalid task class: {task_class}")

    module_path, class_name = (
        f'propel_data_platform.{task_class}s.{config[f"{task_class}-class"]}'.rsplit(
            ".", 1
        )
    )
    module = importlib.import_module(module_path)
    task_class = getattr(module, class_name)

    task = task_class(run_id=run_id, config=config, local=local)
    task.run()


def resolve_dynamic_configs(config: Any, workflow_name: str, local):
    if isinstance(config, dict):
        for key, value in config.items():
            config[key] = resolve_dynamic_configs(value, workflow_name, local)
    elif isinstance(config, list):
        config = [
            resolve_dynamic_configs(item, workflow_name, local) for item in config
        ]
    elif isinstance(config, str) and config.startswith("$"):
        if not local:
            from_env = os.getenv("PRPL_" + config[1:])
        else:
            from_env = os.getenv(config[1:])
        config = from_env or config
    return config
