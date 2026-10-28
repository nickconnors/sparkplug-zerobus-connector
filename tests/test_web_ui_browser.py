"""The web page in a real browser (Playwright's Chromium, headless): every page loads without a console error or a
CSP violation (Alpine's CSP build reports an expression it can't run as an error), and the pages' main actions work
against the connector's server and bridge (``page`` of test_web_sources: a config file, sources that never connect).

Skipped without Playwright (``pip install -r tests/requirements.txt``, then ``python -m playwright install chromium``).
"""
from __future__ import annotations

import asyncio
import os

import pytest
import yaml

from test_web_sources import page, saved, started  # noqa: F401 - page, started: fixtures
from sparkplug_connector.web.web_server import UI_PAGES

playwright_api = pytest.importorskip("playwright.async_api")
pytestmark = pytest.mark.browser


@pytest.fixture
async def browser():
    async with playwright_api.async_playwright() as p:
        try:
            b = await p.chromium.launch()
        except Exception as e:  # noqa: BLE001 - no browser installed here
            pytest.skip(f"Chromium not available: {e}")
        yield b
        await b.close()


@pytest.fixture
async def tab(browser, page):
    """A browser tab on the connector, recording console errors and page errors. The browser starts first: ``page``
    moves HOME (the credential store's directory), where Playwright looks for its browsers."""
    http, path, bridge = page
    base = str(http._base_url).rstrip("/")
    context = await browser.new_context(base_url=base)
    tab = await context.new_page()
    tab.errors = []
    tab.on("console", lambda m: tab.errors.append(m.text) if m.type == "error" else None)
    tab.on("pageerror", lambda e: tab.errors.append(str(e)))
    yield tab, path, bridge
    await context.close()


async def test_every_page_loads_without_errors(tab):
    t, _, _ = tab
    for name in UI_PAGES:
        await t.goto("/" if name == "index" else f"/{name}.html")
        await t.wait_for_selector(".sidebar nav a[aria-current='page']")
        await playwright_api.expect(t.locator(".pill")).not_to_have_text("Loading…")       # (no eval: the CSP refuses it)
        await asyncio.sleep(0.3)                                  # the first poll and the page's own loads
        if os.environ.get("UI_SCREENSHOTS"):                      # a directory: a screenshot of each page there
            await t.screenshot(path=os.path.join(os.environ["UI_SCREENSHOTS"], f"{name}.png"), full_page=True)
        assert t.errors == [], (name, t.errors)
        assert await t.locator(".sidebar nav a").count() == len(UI_PAGES)


async def test_status_lists_the_sources(tab):
    t, _, _ = tab
    await t.goto("/")
    await t.wait_for_selector("text=Sources")
    rows = t.locator("main table tbody tr")
    await rows.first.wait_for()
    assert sorted([await r.locator("strong").inner_text() for r in await rows.all()]) == ["m", "sp"]
    assert await t.locator(".pill").inner_text() == "Running · Zerobus off"
    assert t.errors == []


async def test_a_source_is_added_through_the_dialog(tab):
    t, path, bridge = tab
    await t.goto("/sources.html")
    await t.click("text=Add source")
    await t.fill("#s-name", "plant2")
    await t.fill("#s-endpoint", "mqtts://broker.example:8883")
    await t.fill("#s-user", "plant")
    await t.fill("#s-phid", "databricks-zerobus")
    if os.environ.get("UI_SCREENSHOTS"):
        await t.screenshot(path=os.path.join(os.environ["UI_SCREENSHOTS"], "source-dialog.png"), full_page=True)
    await t.click("dialog[open] footer >> text=Save")
    await t.wait_for_selector("td >> text=plant2")
    s = saved(path)["plant2"]
    assert (s["endpoint"], s["username"], s["primary_host_id"], s["topics"]) == (
        "mqtts://broker.example:8883", "plant", "databricks-zerobus", ["spBv1.0/#"])
    assert "password" not in s and t.errors == []
    # the edit dialog shows what was saved
    row = t.locator("tr", has=t.locator("text=plant2"))
    await row.locator("button[aria-label='Edit source']").click()
    assert await t.input_value("#s-endpoint") == "mqtts://broker.example:8883"
    assert await t.locator("#s-name").is_disabled()


async def test_the_dialog_refuses_what_the_server_would(tab):
    t, path, _ = tab
    before = path.read_text()
    await t.goto("/sources.html")
    await t.click("text=Add source")
    await t.fill("#s-name", "bad name")
    await t.fill("#s-endpoint", "http://x")
    await t.click("dialog[open] footer >> text=Save")
    assert "Letters, digits" in await t.locator("#s-name + small").inner_text()
    assert "mqtt://host:1883" in await t.locator("#s-endpoint + small").inner_text()
    assert path.read_text() == before and t.errors == []


async def test_settings_are_reviewed_saved_and_the_restart_banner_shows(tab):
    t, path, _ = tab
    await t.goto("/settings.html")
    await t.wait_for_selector("summary >> text=Diagnostics")
    assert await t.locator("details.section").count() == 4                   # every section of /api/settings
    await t.check("[id='diagnostics.enabled']")
    await t.click("text=Review and save…")
    await t.wait_for_selector("dialog[open] .diff >> text=Diagnostics")
    await t.click("dialog[open] footer >> text=Save")
    await t.wait_for_selector(".banner >> text=take effect when the connector restarts")
    assert yaml.safe_load(path.read_text())["diagnostics"] == {"enabled": True} and t.errors == []


async def test_a_certificate_is_uploaded_and_offered_to_the_sources(tab, tmp_path):
    t, _, _ = tab
    pem = tmp_path / "plant-ca.pem"
    pem.write_text("-----BEGIN CERTIFICATE-----\n")
    await t.goto("/certs.html")
    await t.set_input_files("input[type=file]", str(pem))
    await t.wait_for_selector("td.mono >> text=plant-ca.pem")
    await t.goto("/sources.html")
    await t.click("text=Add source")
    assert "plant-ca.pem" in await t.locator("#s-ca option").all_inner_texts()
    assert t.errors == []


async def test_edge_nodes_and_a_rebirth_request(tab):
    t, _, bridge = tab
    bridge.clients["sp"].nodes = [{"group_id": "Plant1", "edge_node_id": "Line A", "online": True, "since": 1790696925611,
                                   "changed_by": "NBIRTH", "bdseq": 3,
                                   "devices": [{"device_id": "Press", "online": True, "since": 1790696925611,
                                                "changed_by": "DBIRTH"}]}]
    await t.goto("/edge-nodes.html")
    await t.wait_for_selector("td >> text=Line A")
    assert await t.locator(".tag >> text=Press").count() == 1
    await t.click("button:has-text(\"Request rebirth\")")
    await t.wait_for_selector(".toast >> text=NCMD sent")
    assert bridge.clients["sp"].rebirths == [("Plant1", "Line A")] and t.errors == []
