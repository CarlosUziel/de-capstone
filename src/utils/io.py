import re
from configparser import ConfigParser
from pathlib import Path


def extract_sas_map(sas_file: Path):
    """
    Extract a map variable from a SAS file.

    Args:
        sas_file: A .SAS file.
    """
    # 0. Read SAS file contents
    sas_file_content = sas_file.read_text().replace("\t", "")

    # 1. Get file sections containing variables
    var_sections = re.findall(r"value [^;]*", sas_file_content)

    # 2. Extract variable contents into dictionary
    var_contents = dict()
    for var_section in var_sections:
        var_section_lines = var_section.split("\n")

        # 2.1. Get variable name
        var_name = var_section_lines[0].split(" ")[1]

        # 2.2. Get variable contents
        var_contents[var_name] = dict(
            (split_line[0].strip(), split_line[1].strip())
            for split_line in [
                line.replace("'", "").split("=") for line in var_section_lines[1:]
            ]
            if len(split_line) == 2
        )

    return var_contents


def process_config(config_path: Path) -> ConfigParser:
    """Process a single configuration file."""
    assert config_path.exists(), (
        f"User configuration file {config_path} does not exist, "
        "please create it following the README.md file of this project."
    )

    with config_path.open("r") as fp:
        config = ConfigParser()
        config.read_file(fp)

    return config
