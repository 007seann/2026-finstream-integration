"""
Pipeline skeleton demonstrating the patterns required by the integration guidelines:
- config loaded from a separate YAML file (not hardcoded)
- logging for diagnosability
- checkpoint-based resume after interruption

Replace the collect/process/save steps with your actual pipeline logic.
"""
import json
import logging
import os
from pathlib import Path

import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def load_checkpoint(checkpoint_file: str) -> dict:
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            logger.info("Resuming from existing checkpoint: %s", checkpoint_file)
            return json.load(f)
    return {"last_processed_id": None}


def save_checkpoint(checkpoint_file: str, state: dict) -> None:
    Path(os.path.dirname(checkpoint_file)).mkdir(parents=True, exist_ok=True)
    with open(checkpoint_file, "w") as f:
        json.dump(state, f)


def collect(config: dict, checkpoint: dict):
    logger.info("Collecting data from %s", config["api"]["base_url"])
    # TODO: implement actual API calls, respecting rate limits from metadata.yaml
    raise NotImplementedError


def process(raw_data):
    logger.info("Processing raw data")
    # TODO: implement cleaning / transformation, save intermediate output
    raise NotImplementedError


def save(processed_data, output_dir: str):
    logger.info("Saving final output to %s", output_dir)
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    # TODO: write final dataset


def main():
    config = load_config()
    checkpoint = load_checkpoint(config["run"]["checkpoint_file"])

    try:
        raw_data = collect(config, checkpoint)
        processed_data = process(raw_data)
        save(processed_data, config["paths"]["output_dir"])
    except Exception:
        logger.exception("Pipeline failed — checkpoint preserved for resume")
        raise
    else:
        save_checkpoint(config["run"]["checkpoint_file"], checkpoint)
        logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    main()
