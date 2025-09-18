from datetime import datetime

from propel_data_platform.tasks import execute_task_fn
from propel_data_platform.workflows import load_config

RUN_ID = f"local{datetime.now().strftime('%Y%m%d%H%M%S')}"
ENV = "dev"
CLIENT = "dorsia"
WORKFLOW = "braze-campaigns"
TASK_KEY = "list-connector"
CLIENT_CONFIG = {
    "braze-config": {
        "api-key": "$DORSIA_BRAZE_API_KEY",
        "api-url": "$DORSIA_BRAZE_API_URL",
    }
}
RUNTIME_CONFIG = {
    "start-date": "2025-07-01",
    "end-date": "2025-07-02",
}


config = load_config(ENV, CLIENT, WORKFLOW, CLIENT_CONFIG)
workflow_template = {"tasks": config["tasks"]}
del config["tasks"]
del config["compute"]
workflow_name = f"{config['environment']}.{config['client']}.{config['workflow']}"
config["workflow-name"] = workflow_name
task = workflow_template["tasks"][TASK_KEY]

conflict_keys = set(config.keys() & task.get("task-config", {}).keys())
if len(conflict_keys) > 0:
    raise Exception(
        f"Task {TASK_KEY} has conflicting keys with config: {conflict_keys}"
    )

task_config = task.get("task-config", {})
if "task-key" in task_config:
    raise Exception(
        f"Task {TASK_KEY} has task-key in task-config, which is not allowed"
    )
task_config["task-key"] = TASK_KEY

params = {
    "runtime_config": RUNTIME_CONFIG,
    "task_config": task_config,
    "workflow_config": config,
    "task_class": task["task-class"],
    "run_id": RUN_ID,
    "local": True,
}

execute_task_fn(**params)
