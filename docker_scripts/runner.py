import json
import os
import subprocess
import time
import pathlib as pl
import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(filename)s:%(lineno)d] %(message)s')
logger = logging.getLogger(__name__)


def main():
    """Main"""

    input_path = pl.Path(
        os.environ["DY_SIDECAR_PATH_INPUTS"])

    pyrunner = PythonRunner(input_path)

    try:
        pyrunner.setup()
        pyrunner.start()
        pyrunner.teardown()
    except Exception as err:  # pylint: disable=broad-except
        logger.error(f"{err} . Stopping %s", exc_info=True)


class PythonRunner:

    def __init__(self, input_path, polling_time=1):
        """Constructor"""

        self.input_path = input_path  # path where osparc write all our input
        self.pythoncode_path = self.input_path / 'input_1'
        self.keyvalues_path = self.input_path / 'key_values.json'
        self.main_sh_path = pl.Path(__file__).parent.absolute() / 'main.sh'
        self.venv_dir = pl.Path.home() / ".venv"
        self.polling_time = polling_time
        self.found_user_code_entrypoint = False
        self.user_code_entrypoint = None
        self.keyvalues = {}

    def setup(self):
        """Setup the Python Runner"""

        # Try to find user entrypoint, will poll until found
        self.user_code_entrypoint = self.try_find_user_entrypoint()

        # We have found the user entrypoint
        self.found_user_code_entrypoint = True

        # Set up the user entrypoint
        self.setup_entrypoint()

    def start(self):
        """Start the Python Runner"""

        logger.info(f"Starting script {self.main_sh_path.resolve()} ...")
        if self.found_user_code_entrypoint:
            env = os.environ.copy()
            env |= self.main_env  # Merge envs
            logging.debug(f'Using env: {env}')

            proc = subprocess.Popen(
                [f'{self.main_sh_path.resolve()}'], env=env)
            proc.wait()
        else:
            logger.info("No main script found, exiting")
        logger.info("Finished running python scripts")

    def teardown(self):
        logger.info("Completed")

    def _ensure_pip_requirements(self, code_dir: pl.Path) -> pl.Path:
        logger.info("Searching for requirements file ...")
        requirements = list(code_dir.rglob("requirements.txt"))
        if len(requirements) > 1:
            raise ValueError("Multiple requirements found, "
                             "don't know which one to pick"
                             f": {requirements}")
        elif len(requirements) == 0:
            return None
        else:
            requirements = requirements[0]
            logger.info("Found: %s", requirements)
        return requirements

    def try_find_user_entrypoint(self):
        """Trying to find entrypoin"""

        poll_counter = 0
        while not self.keyvalues_path.exists():
            if poll_counter % 20 == 0:
                logging.info("Waiting for keyvalues file at "
                             f"{self.keyvalues_path}")
            time.sleep(self.polling_time)
            poll_counter += 1

        poll_counter = 0
        user_code_entrypoint = None
        while not user_code_entrypoint:
            if poll_counter % 20 == 0:
                logging.info("Waiting for correct keyvalues in "
                             f"{self.keyvalues_path}")
            time.sleep(self.polling_time)
            user_code_entrypoint = self._find_keyvalues_user_code_entrypoint()
            poll_counter += 1

        logging.info(f"Found user entrypoint: {user_code_entrypoint}")
        return user_code_entrypoint

    def _find_keyvalues_user_code_entrypoint(self):

        user_code_entrypoint = None

        self.keyvalues = self.read_keyvalues()
        logging.info(f"Found key-values: {self.keyvalues}")

        if 'input_0' in self.keyvalues and \
                'input_0' in self.keyvalues['input_0']:
            user_code_entrypoint = self.pythoncode_path / \
                pl.Path(self.keyvalues['input_0']['input_0'])
            poll_counter = 0
            while not user_code_entrypoint.exists():
                if poll_counter % 20 == 0:
                    logging.info("Waiting for user provided endpoint at "
                                 f"{user_code_entrypoint.resolve()} to become "
                                 "available")
                time.sleep(self.polling_time)
                poll_counter += 1

        return user_code_entrypoint

    def setup_entrypoint(self):
        """Setup entrypoint after it is found"""

        self.requirements_path = self._ensure_pip_requirements(
            self.pythoncode_path)
        self.main_env = {}
        self.main_env['OSPARC_VENV_DIR'] = str(self.venv_dir)
        self.main_env['OSPARC_USER_ENTRYPOINT_PATH'] = \
            str(self.user_code_entrypoint)
        self.main_env['OSPARC_REQUIREMENTS_TXT'] = str(
            self.requirements_path) if self.requirements_path else ''
        self.main_env['OSPARC_USER_ENTRYPOINT_DIR'] = \
            str(self.user_code_entrypoint.parents[0])

    def read_keyvalues(self):
        """Read keyvalues file"""

        keyvalues_unprocessed = json.loads(
            self.keyvalues_path.read_text())

        keyvalues = {}
        for key, value in keyvalues_unprocessed.items():
            keyvalues[key] = {}
            keyvalues[key][value['key']] = value['value']

        return keyvalues


if __name__ == "__main__":
    main()
