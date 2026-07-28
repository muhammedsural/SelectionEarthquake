from pathlib import Path
import tomllib


def test_cli_uses_short_descriptive_command_name():
    """CLI giriş noktasının kısa ve açıklayıcı adını doğrular."""
    project_root = Path(__file__).resolve().parents[1]
    with (project_root / "pyproject.toml").open("rb") as config_file:
        project_config = tomllib.load(config_file)

    scripts = project_config["project"]["scripts"]

    assert scripts["quake-sel"] == "selection_service.cli:main"
    assert "earthquake-selection-example" not in scripts
