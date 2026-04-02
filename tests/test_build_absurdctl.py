from pathlib import Path
from types import SimpleNamespace
import runpy


BUILD_SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "build-absurdctl"
MODULE = runpy.run_path(str(BUILD_SCRIPT_PATH))
pep440_version_from_describe = MODULE["pep440_version_from_describe"]
render_pypi_pyproject = MODULE["render_pypi_pyproject"]
write_pypi_staging = MODULE["write_pypi_staging"]
main = MODULE["main"]


def test_pep440_version_from_describe_accepts_exact_tags():
    assert pep440_version_from_describe("0.2.0") == "0.2.0"
    assert pep440_version_from_describe("v0.2.0") == "0.2.0"


def test_pep440_version_from_describe_converts_git_describe_output():
    assert pep440_version_from_describe("0.2.0-0-g11fbcf8") == "0.2.0"
    assert (
        pep440_version_from_describe("0.2.0-25-g11fbcf8")
        == "0.2.0.dev25+g11fbcf8"
    )
    assert (
        pep440_version_from_describe("v0.2.0-1-gdeadbee-dirty")
        == "0.2.0.dev1+gdeadbee.dirty"
    )


def test_render_pypi_pyproject_uses_uv_build_and_console_script():
    pyproject = render_pypi_pyproject("0.2.0")

    assert 'name = "absurdctl"' in pyproject
    assert 'version = "0.2.0"' in pyproject
    assert 'build-backend = "uv_build"' in pyproject
    assert 'absurdctl = "absurdctl:main"' in pyproject
    assert 'module-name = "absurdctl"' in pyproject
    assert 'module-root = ""' in pyproject


def test_write_pypi_staging_creates_generated_package_tree(tmp_path):
    output_dir = tmp_path / "absurdctl-pypi"
    rendered_source = "#!/usr/bin/env python3\nprint('hello')\n"

    write_pypi_staging(
        rendered_source,
        "0.2.0",
        output_dir=output_dir,
        readme_text="# absurdctl\n",
        license_text="Apache-2.0\n",
    )

    assert (output_dir / "absurdctl" / "__init__.py").read_text(
        encoding="utf-8"
    ) == rendered_source
    assert 'name = "absurdctl"' in (output_dir / "pyproject.toml").read_text(
        encoding="utf-8"
    )
    assert (output_dir / "README.md").read_text(encoding="utf-8") == "# absurdctl\n"
    assert (output_dir / "LICENSE").read_text(encoding="utf-8") == "Apache-2.0\n"


def test_main_does_not_require_package_version_for_standalone_only(monkeypatch):
    monkeypatch.setitem(
        main.__globals__,
        "parse_args",
        lambda: SimpleNamespace(standalone_only=True, pypi_only=False),
    )
    monkeypatch.setitem(main.__globals__, "detect_target_version", lambda: "main")
    monkeypatch.setitem(
        main.__globals__,
        "detect_package_version",
        lambda: (_ for _ in ()).throw(AssertionError("should not be called")),
    )
    monkeypatch.setitem(main.__globals__, "collect_migrations", lambda: {})
    monkeypatch.setitem(main.__globals__, "collect_skills", lambda: {})
    monkeypatch.setitem(
        main.__globals__,
        "render_bundled_source",
        lambda *args, **kwargs: "#!/usr/bin/env python3\nprint('ok')\n",
    )

    captured = {}

    def fake_write_standalone_bundle(rendered_source, output_path=None):
        captured["rendered_source"] = rendered_source
        return Path("dist/absurdctl")

    monkeypatch.setitem(
        main.__globals__, "write_standalone_bundle", fake_write_standalone_bundle
    )

    main()

    assert captured["rendered_source"] == "#!/usr/bin/env python3\nprint('ok')\n"
