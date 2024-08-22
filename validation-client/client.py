"""Test client for validation"""

import json
import logging
import os
import pathlib as pl
import time
import uuid

from osparc_filecomms import handshakers

logging.basicConfig(
    level=logging.INFO,
    format="Validation client: [%(filename)s:%(lineno)d] %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    logger.info("Started validation client")

    client_uuid = str(uuid.uuid4())
    task_uuid = str(uuid.uuid4())
    client_input_path = pl.Path(os.environ["VALIDATION_CLIENT_INPUT_PATH"])
    client_output_path = pl.Path(os.environ["VALIDATION_CLIENT_OUTPUT_PATH"])
    input_tasks_path = client_output_path / "input_tasks.json"
    output_tasks_path = client_input_path / "output_tasks.json"

    this_dir = pl.Path(__file__).parent

    input_tasks_template_path = this_dir / "input_tasks_template.json"
    input_tasks = json.loads(input_tasks_template_path.read_text())
    input_tasks["uuid"] = task_uuid
    input_tasks["caller_uuid"] = client_uuid

    handshaker = handshakers.FileHandshaker(
        client_uuid,
        client_input_path,
        client_output_path,
        is_initiator=False,
        verbose_level=logging.DEBUG,
        polling_interval=0.1,
        print_polling_interval=100,
    )
    map_uuid = handshaker.shake()
    input_tasks["map_uuid"] = map_uuid

    input_tasks_path.write_text(json.dumps(input_tasks))

    while not os.path.exists(output_tasks_path):
        time.sleep(0.1)

    output_tasks = json.loads(output_tasks_path.read_text())

    logger.info(output_tasks)

    assert output_tasks["uuid"] == task_uuid
    for task_id in range(len(input_tasks["tasks"])):
        for input_name in input_tasks["tasks"][task_id]["input"].keys():
            assert (
                output_tasks["tasks"][task_id]["input"][input_name]["value"]
                == input_tasks["tasks"][task_id]["input"][input_name]["value"]
            )

    stop_command = {
        "caller_uuid": client_uuid,
        "map_uuid": map_uuid,
        "command": "stop",
    }
    input_tasks_path.write_text(json.dumps(stop_command))


if __name__ == "__main__":
    main()
