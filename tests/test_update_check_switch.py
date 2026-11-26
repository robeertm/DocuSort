"""DOCUSORT_UPDATE_CHECK=off: no request to GitHub, no update banner."""
import pytest

from docusort import updater


def _boom(*a, **k):
    raise AssertionError("the update check must not touch the network")


@pytest.mark.parametrize("value", ["off", "0", "false", "NO", " Off "])
def test_switched_off_makes_no_request(monkeypatch, value):
    monkeypatch.setenv("DOCUSORT_UPDATE_CHECK", value)
    monkeypatch.setattr(updater.urllib.request, "urlopen", _boom)
    info = updater.version_info()
    assert info["has_update"] is False
    assert info["check_disabled"] is True


def test_default_still_asks_github(monkeypatch):
    monkeypatch.delenv("DOCUSORT_UPDATE_CHECK", raising=False)
    asked = []
    monkeypatch.setattr(updater, "fetch_latest_release",
                        lambda: asked.append(1) or {"tag_name": "v999.0.0"})
    info = updater.version_info()
    assert asked and info["has_update"] is True
