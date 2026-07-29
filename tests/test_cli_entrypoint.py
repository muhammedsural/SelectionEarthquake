from pathlib import Path


def _read_project_scripts(project_file: Path) -> dict[str, str]:
    """Pyproject dosyasındaki komut giriş noktalarını bağımlılıksız okur."""
    content = project_file.read_text(encoding="utf-8")
    scripts_content = content.split("[project.scripts]", maxsplit=1)[1]
    scripts_section = scripts_content.split("\n[", maxsplit=1)[0]

    scripts = {}
    for line in scripts_section.splitlines():
        if "=" not in line:
            continue
        command, target = line.split("=", maxsplit=1)
        scripts[command.strip()] = target.strip().strip('"')
    return scripts


def test_cli_uses_short_descriptive_command_name():
    """CLI giriş noktasının kısa ve açıklayıcı adını doğrular."""
    project_root = Path(__file__).resolve().parents[1]

    scripts = _read_project_scripts(project_root / "pyproject.toml")

    assert scripts["quake-sel"] == "selection_service.cli:main"
    assert "earthquake-selection-example" not in scripts
