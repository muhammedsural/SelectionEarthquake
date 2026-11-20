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


def _args(**overrides):
    import argparse

    values = {"cesmd_email": None, "ngagm_username": None, "ngagm_password": None}
    values.update(overrides)
    return argparse.Namespace(**values)


def test_credentials_are_asked_when_missing_and_terminal_is_interactive():
    from selection_service.cli import collect_credentials
    from selection_service.enums.enums import ProviderName

    asked = []

    def ask(prompt):
        asked.append(prompt)
        return " kullanici "

    secret_prompts = []

    def ask_secret(prompt):
        secret_prompts.append(prompt)
        return "parola"

    result = collect_credentials(
        [ProviderName.NGAW3], _args(), need_download=False, environ={}, interactive=True,
        ask=ask, ask_secret=ask_secret,
    )
    assert result == {"username": "kullanici", "password": "parola"}
    assert len(asked) == 1 and len(secret_prompts) == 1


def test_credentials_prefer_arguments_then_environment_and_skip_questions():
    from selection_service.cli import collect_credentials
    from selection_service.enums.enums import ProviderName

    def boom(_):
        raise AssertionError("must not ask")

    env = {"NGAGM_USERNAME": "envuser", "NGAGM_PASSWORD": "envpw", "CESMD_EMAIL": "env@x.org"}
    result = collect_credentials(
        [ProviderName.NGAW3, ProviderName.CESMD], _args(ngagm_username="arg"), need_download=True,
        environ=env, interactive=True, ask=boom, ask_secret=boom,
    )
    assert result == {"email": "env@x.org", "username": "arg", "password": "envpw"}


def test_cesmd_email_is_only_needed_for_download_and_other_providers_need_nothing():
    from selection_service.cli import collect_credentials
    from selection_service.enums.enums import ProviderName

    def boom(_):
        raise AssertionError("must not ask")

    assert collect_credentials([ProviderName.CESMD], _args(), need_download=False, environ={},
                               interactive=True, ask=boom, ask_secret=boom) == {}
    assert collect_credentials([ProviderName.PEER, ProviderName.AFAD], _args(), need_download=True,
                               environ={}, interactive=True, ask=boom, ask_secret=boom) == {}


def test_non_interactive_terminal_never_asks():
    from selection_service.cli import collect_credentials
    from selection_service.enums.enums import ProviderName

    def boom(_):
        raise AssertionError("must not ask")

    assert collect_credentials([ProviderName.NGAW3, ProviderName.CESMD], _args(), need_download=True,
                               environ={}, interactive=False, ask=boom, ask_secret=boom) == {}


def test_cli_parser_accepts_credential_arguments():
    from selection_service.cli import build_parser

    args = build_parser().parse_args(
        ["--providers", "ngaw3", "--ngagm-username", "u", "--cesmd-email", "a@b.org"]
    )
    assert args.ngagm_username == "u" and args.cesmd_email == "a@b.org" and args.ngagm_password is None
