import argparse
import contextlib
import functools
import json
import subprocess
import sys
from collections.abc import Callable, Generator
from dataclasses import dataclass
from io import TextIOWrapper
from pathlib import Path
from typing import TypeAlias

import yaml

DeploymentMapper: TypeAlias = Callable[[dict[str, dict], "DeploymentFile"], None]


# these were previous consntant, but are configdential
@dataclass
class AppConfig:
    cfg_path: Path
    team_names: set[str]
    github_var_names: set[str]

    @classmethod
    def from_yaml(cls, yaml_path: Path) -> "AppConfig":
        if not yaml_path.is_file():
            raise FileNotFoundError(f"Invalid YAML File: {yaml_path}")

        with yaml_path.open("r", encoding="utf-8") as file:
            data = yaml.safe_load(file)

        return cls(
            cfg_path=Path(data.get("cfg_path", ".")),
            team_names=set(data.get("team_names", [])),
            github_var_names=set(data.get("github_var_names", [])),
        )


@functools.lru_cache(maxsize=1)
def app_config() -> AppConfig:
    return AppConfig.from_yaml(Path("private.yaml"))


def get_team_names() -> Generator[str, None, None]:
    for team in app_config().team_names:
        yield team


def github_cfg_variables() -> set[str]:
    """
    the variables that are included in every cfg file, but can be resolved
    seperately from the rest of the cfg variables,

    Returns
    -------
    set[str]
        _the variable names_
    """
    return app_config().github_var_names


def unknown_team_name(team: str) -> Exception:
    return ValueError(
        f"Unknown team name: {team}. Valid teams are: {', '.join(get_team_names())}"
    )


@dataclass
class DeploymentFile:
    path: Path
    variables: dict[str, str]
    team_name: str
    cluster_name: str
    deployment_type: str

    @classmethod
    def create(cls, cfg_path: Path) -> "DeploymentFile":
        if not cfg_path.is_file() or not cfg_path.name.endswith(".cfg"):
            raise FileNotFoundError(f"Invalid CFG File: {cfg_path}")

        variables = ConfigFileUtils.dict_parse_cfg(cfg_path)
        parts = cfg_path.name.split("_")
        if len(parts) < 3:
            raise ValueError(f"Invalid file name format: {cfg_path.name}")

        team = parts[0]
        cluster_name = parts[1]
        deployment_type = cfg_path.name.replace(f"{team}_{cluster_name}_", "").rstrip(
            ".cfg"
        )
        return cls(
            path=cfg_path,
            variables=variables,
            team_name=team,
            cluster_name=cluster_name,
            deployment_type=deployment_type,
        )

    def exclude_github_vars(self) -> dict[str, str]:
        github_var_names = github_cfg_variables()
        return {
            key: value
            for key, value in self.variables.items()
            if key not in github_var_names
        }


class ConfigFileUtils:
    @staticmethod
    def dict_parse_cfg(file: Path) -> dict[str, str]:
        """
        Reads a .cfg file and returns it's lines a dictionary of key-value pairs.

        Parameters
        ----------
        file : Path
            _path to the file_

        Returns
        -------
        dict[str, str]
            _the key value pair of the variables_
        """
        cfg = {}
        cfg_lines = file.read_text(encoding="utf-8").splitlines()

        for line in cfg_lines:
            if not line.strip() or line.startswith("#"):
                continue
            _, assignment = line.split(" ")
            key, value = assignment.split("=")
            cfg[key.strip()] = value.strip().replace("'", "")
        return cfg

    @staticmethod
    def iter_team_cfgs(team_name: str) -> Generator[Path, None, None]:
        """
        Yields all .cfg files for a given team.

        Parameters
        ----------
        team_name : str
            _the name of the team_

        Yields
        ------
        Path
            _the path to the .cfg file_
        """
        if team_name not in get_team_names():
            raise unknown_team_name(team_name)

        target = Path(app_config().cfg_path)

        for file in target.rglob(f"{team_name}_*.cfg"):
            if not file.is_file():
                continue
            yield file

    @staticmethod
    def iter_deployments(team_name: str) -> Generator[DeploymentFile, None, None]:
        for cfg_file in ConfigFileUtils.iter_team_cfgs(team_name):
            yield DeploymentFile.create(cfg_file)


class CFGExporter:
    def __init__(
        self,
        team_name: str,
        *,
        dest_dir: str,
    ) -> None:
        self.data: dict[str, dict] = {}
        self.team_name: str = team_name
        self.dest_dir: str = dest_dir

    @contextlib.contextmanager
    def export(
        self, *, mkdirs: bool = False
    ) -> Generator[tuple[TextIOWrapper, dict], None, None]:
        dest = Path(self.dest_dir, f"{self.team_name}.{self.dest_dir}")
        if mkdirs:
            dest.parent.mkdir(parents=True, exist_ok=True)
        with dest.open("w", encoding="utf-8") as f:
            yield f, self.data

    def build_contents(
        self,
        *,
        mapper_fn: DeploymentMapper | None = None,
    ) -> None:
        """
        Builds the contents of the data dict by iterating over deployments

        Parameters
        ----------
        mapper_fn : DeploymentMapper | None, optional
            a function that is passed the data dict and a DeploymentFile object,
            you should set the keys of the data dict in this function don't return anything,
        """
        map_fn = mapper_fn or self.deployment_mapper
        for deployment in ConfigFileUtils.iter_team_cfgs(self.team_name):
            deployment_obj = DeploymentFile.create(deployment)
            map_fn(self.data, deployment_obj)

    def deployment_mapper(
        self, data_dict: dict[str, dict], deployment: DeploymentFile
    ) -> None:
        """sets the contents of the data dict"""
        data_dict.setdefault(deployment.cluster_name, {})
        data_dict[deployment.cluster_name].update(
            {
                deployment.deployment_type: deployment.exclude_github_vars(),
            }
        )

    @classmethod
    def export_all_generator(
        cls, *, dest_dir: str, mkdirs: bool = False
    ) -> Generator[tuple[TextIOWrapper, dict], None, None]:
        """
        A generator that yields a file object and a data dict for exporting.

        Parameters
        ----------
        dest_dir : str, optional
            the directory to export to, by default OUTPUT_DIR
        mkdirs : bool, optional
            whether to create the directory if it doesn't exist, by default False

        Yields
        ------
        tuple[TextIOWrapper, dict]
            a tuple of the file object and the data dict
        """

        for team_name in get_team_names():
            exporter = cls(team_name, dest_dir=dest_dir)
            exporter.build_contents()
            with exporter.export(mkdirs=mkdirs) as (file, data):
                yield file, data


def resolve_cli_opts() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export team .cfg files to JSON or YAML."
    )

    parser.add_argument(
        "--team", type=str, help="Team name to export configuration for."
    )
    parser.add_argument(
        "--pull",
        action="store_true",
        help="Pull latest changes from remote before exporting.",
    )
    parser.add_argument(
        "--all", action="store_true", help="Export all team configurations."
    )
    parser.add_argument(
        "--format",
        type=str,
        choices=["json", "yaml"],
        default="json",
        help="Format to export the configuration (default: json).",
    )
    return parser.parse_args()


def try_pull() -> None:
    try:
        subprocess.run(["git", "pull"], check=True)
    except subprocess.CalledProcessError:
        print("! could not pull latest changes from remote !")
        if not input("proceed without pulling? (y/n): ").lower().startswith("y"):
            print("aborting...")
            sys.exit(1)


def get_export_handler(
    format: str,
    file: TextIOWrapper,
    data: dict[str, dict],
) -> None:
    if format == "json":
        json.dump(data, file, indent=4)
    elif format == "yaml":
        yaml.safe_dump(data, file, default_flow_style=False, indent=4)
    else:
        raise ValueError(f"Unsupported format: {format}. Use 'json' or 'yaml'.")
    print(f">> exported {file.name} <<")


def export_team(opts: argparse.Namespace) -> None:
    if opts.team not in get_team_names():
        raise unknown_team_name(opts.team)

    exporter = CFGExporter(opts.team, dest_dir=opts.format)
    exporter.build_contents()

    with exporter.export(mkdirs=True) as (file, data):
        get_export_handler(opts.format, file, data)
    print(f'>> {opts.format} export for team "{opts.team}" complete <<')


def main() -> None:
    cli_options = resolve_cli_opts()

    if cli_options.pull:
        try_pull()

    if cli_options.team:
        export_team(cli_options)

    if cli_options.all:
        for file, data in CFGExporter.export_all_generator(
            dest_dir=cli_options.format, mkdirs=True
        ):
            get_export_handler(cli_options.format, file, data)


if __name__ == "__main__":
    main()
