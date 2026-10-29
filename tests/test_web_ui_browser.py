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
    await t.fill("#s-pass", "PLANT2_MQTT_PASSWORD")
    assert "Saved as ${env:PLANT2_MQTT_PASSWORD}" in await t.locator("#s-pass + small").inner_text()
    await t.fill("#s-phid", "databricks-zerobus")
    if os.environ.get("UI_SCREENSHOTS"):
        await t.screenshot(path=os.path.join(os.environ["UI_SCREENSHOTS"], "source-dialog.png"), full_page=True)
    await t.click("dialog[open] footer >> text=Save")
    await t.wait_for_selector("td >> text=plant2")
    s = saved(path)["plant2"]
    assert (s["endpoint"], s["username"], s["primary_host_id"], s["topics"]) == (
        "mqtts://broker.example:8883", "plant", "databricks-zerobus", ["spBv1.0/#"])
    assert s["password"] == "${env:PLANT2_MQTT_PASSWORD}" and t.errors == []
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
    await t.locator("dialog[open] footer").get_by_role("button", name="Save", exact=True).click()
    await t.wait_for_selector(".banner >> text=take effect when the bridge restarts")
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


@pytest.mark.parametrize("is_set", [False, True])
async def test_the_zerobus_page_calls_out_the_secrets_variable(tab, monkeypatch, is_set):
    t, path, _ = tab
    if is_set:
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "from-env")
    else:
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
    await t.goto("/zerobus.html")
    await t.wait_for_selector(".secret-env >> text=DATABRICKS_CLIENT_SECRET")
    if os.environ.get("UI_SCREENSHOTS") and is_set:
        await t.screenshot(path=os.path.join(os.environ["UI_SCREENSHOTS"], "zerobus-secret.png"))
    assert await t.locator("input[type=password]").count() == 0                 # no field for a secret's value
    await t.fill("#z-host", "https://example.cloud.databricks.com")
    await t.fill("#z-endpoint", "123.zerobus.us-east-1.cloud.databricks.com")
    await t.fill("#z-table", "c.s.raw_messages")
    await t.click("button[type=submit] >> text=Save")
    await t.wait_for_selector(".toast")
    assert yaml.safe_load(path.read_text())["zerobus"]["auth"]["client_secret"] == "${env:DATABRICKS_CLIENT_SECRET}"
    note = t.locator(".secret-env + small")
    await playwright_api.expect(note).to_have_text("Set in the connector's environment." if is_set else
                                                   "Not set: set it in the connector's environment, then restart the connector.")
    assert t.errors == []


async def _patched(t, failing: bool):
    """/api/status with Zerobus on and /api/sparkplug/health with a writer error, current (failing) or recovered."""
    import json

    async def status(route):
        body = json.loads(await (await route.fetch()).text())
        body.update(zerobus_enabled=True, zerobus_connected=True)
        await route.fulfill(json=body)

    async def health(route):
        body = json.loads(await (await route.fetch()).text())
        for h in body.values():
            h.update(last_error="opening a Zerobus stream: unreachable", zerobus_last_error_at="2026-10-28T21:04:48.000Z",
                     zerobus_failing_since="2026-10-28T21:03:56.000Z" if failing else None, zerobus_stream_open=not failing)
        await route.fulfill(json=body)
    await t.route("**/api/status", status)
    await t.route("**/api/sparkplug/health", health)


@pytest.mark.parametrize("failing", [True, False])
async def test_a_recovered_writer_error_is_history_not_a_current_error(tab, failing):
    """After an outage the writer keeps its last error: the top bar says "errors" only while it is failing, and the
    Status table shows the last error with its time, in the error colour only while failing."""
    t, _, _ = tab
    await _patched(t, failing)
    await t.goto("/")
    pill = t.locator(".pill")
    await playwright_api.expect(pill).to_have_text("Running · errors" if failing else "Running")
    cell = t.locator("main table tbody tr td.last-error").first
    await playwright_api.expect(cell).to_contain_text("opening a Zerobus stream: unreachable")
    assert "2026" in await cell.inner_text()                                   # with its time
    assert ("error-text" in (await cell.get_attribute("class"))) is failing
    assert t.errors == []


async def test_the_bridge_is_stopped_started_and_restarted_from_the_pages(tab):
    """Status: Stop (its dialog), then Start; Settings: Save and restart, which clears the banner; the banner's
    Restart now after a plain save."""
    t, path, _ = tab
    await t.goto("/")
    pill = t.locator(".pill")
    await playwright_api.expect(pill).to_have_text("Running · Zerobus off")
    await t.get_by_role("button", name="Stop the bridge").click()
    await t.locator("dialog[open]").get_by_role("button", name="Stop").click()
    await playwright_api.expect(pill).to_have_text("Stopped")
    await t.get_by_role("button", name="Start the bridge").click()
    await playwright_api.expect(pill).to_have_text("Running · Zerobus off")

    await t.goto("/settings.html")
    await t.get_by_role("button", name="Expand all").click()
    field = t.locator('[id="zerobus.drain_timeout_seconds"]')
    await field.fill("4")
    await t.get_by_role("button", name="Review and save…").click()
    await t.locator("dialog[open]").get_by_role("button", name="Save and restart").click()
    # the save's own answer first (the banner never shows on this path: waiting for it hidden proves nothing)
    await playwright_api.expect(t.locator(".toast", has_text="The bridge is restarting")).to_be_visible()
    banner = t.locator(".banner")
    await playwright_api.expect(banner).to_be_hidden()
    assert yaml.safe_load(path.read_text())["zerobus"]["drain_timeout_seconds"] == 4

    await field.fill("6")
    await t.get_by_role("button", name="Review and save…").click()
    await t.locator("dialog[open]").get_by_role("button", name="Save", exact=True).click()
    await playwright_api.expect(banner).to_be_visible()
    await banner.get_by_role("button", name="Restart now").click()
    await playwright_api.expect(banner).to_be_hidden()
    await playwright_api.expect(pill).to_have_text("Running · Zerobus off")
    assert t.errors == []


@pytest.mark.parametrize("secret, note", [
    ("${credential:zerobus.client_secret}", "The file has a secret of its own: it stays as written."),
    ("${env:ZB_SECRET:}", "Not set: set it in the connector's environment, then restart the connector."),
])
async def test_a_zerobus_save_keeps_a_secret_the_page_did_not_set(tab, monkeypatch, secret, note):
    """The page only ever writes ${env:DATABRICKS_CLIENT_SECRET} where the file has no secret: a ${credential:...}
    placeholder or an ${env:NAME:default} stays as written when another field is saved."""
    t, path, _ = tab
    monkeypatch.delenv("ZB_SECRET", raising=False)
    raw = yaml.safe_load(path.read_text())
    raw["zerobus"] = {"enabled": False, "auth": {"client_id": "id", "client_secret": secret}}
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    await t.goto("/zerobus.html")
    await playwright_api.expect(t.locator(".secret-env + small")).to_have_text(note)
    await t.fill("#z-host", "https://example.cloud.databricks.com")
    await t.fill("#z-endpoint", "123.zerobus.us-east-1.cloud.databricks.com")
    await t.fill("#z-table", "c.s.raw_messages")
    await t.click("button[type=submit] >> text=Save")
    await t.wait_for_selector(".toast")
    saved = yaml.safe_load(path.read_text())["zerobus"]
    assert saved["auth"]["client_secret"] == secret and saved["workspace_host"] == "https://example.cloud.databricks.com"
    assert t.errors == []


async def test_the_sign_in_button_is_centred_on_the_login_page(tab):
    t, _, _ = tab
    # as with the login on and nobody signed in (else the page goes straight to the connector)
    await t.route("**/api/auth/status", lambda route: route.fulfill(json={"auth_enabled": True, "authenticated": False}))
    await t.goto("/static/login.html")
    button = await t.get_by_role("button", name="Sign in with Databricks").bounding_box()
    card = await t.locator(".auth article").bounding_box()
    assert abs((button["x"] + button["width"] / 2) - (card["x"] + card["width"] / 2)) <= 1
    assert button["width"] < card["width"]                     # a button, not a full-width bar
    assert t.errors == []


async def test_the_restart_banner_stays_in_view_after_a_save_at_the_bottom(tab):
    """The Settings page is long: its Save is at the bottom, where the banner (under the top bar) was out of view."""
    t, _, _ = tab
    await t.set_viewport_size({"width": 1280, "height": 700})
    await t.goto("/settings.html")
    await t.get_by_role("button", name="Expand all").click()
    field = t.locator('[id="zerobus.drain_timeout_seconds"]')
    await field.fill("9")
    review = t.get_by_role("button", name="Review and save…")
    await review.scroll_into_view_if_needed()
    await review.click()
    await t.locator("dialog[open]").get_by_role("button", name="Save", exact=True).click()
    banner = t.locator(".banner")
    await playwright_api.expect(banner).to_be_visible()
    assert await t.evaluate("window.scrollY") > 0                              # still scrolled down
    box = await banner.bounding_box()
    assert 0 <= box["y"] and box["y"] + box["height"] <= 700, box             # inside the window
    assert t.errors == []


async def test_big_counters_are_compact_on_the_tiles_and_exact_on_hover(tab):
    """After a year at 100 rows/s the count is ~3 billion: in full it overflows its tile."""
    import json
    t, _, _ = tab

    async def status(route):
        body = json.loads(await (await route.fetch()).text())
        body.update(backpressure_queue_size=950, bridge_started_at=1793223055026)
        body["metrics"]["zerobus_targets"] = {"w.c.s.raw_messages": {"written": 3_153_600_000}}
        await route.fulfill(json=body)
    await t.route("**/api/status", status)
    await t.goto("/")
    rows = t.locator(".tile", has_text="Rows written")
    await playwright_api.expect(rows.locator(".value")).to_have_text("3.15B")
    assert await rows.locator(".value").get_attribute("title") == "3,153,600,000"
    await playwright_api.expect(t.locator(".tile", has_text="Queued").locator(".value")).to_have_text("950")
    await playwright_api.expect(rows.locator(".sub")).to_contain_text("since the bridge started, ")
    assert "2026" in await rows.locator(".sub").inner_text()
    assert t.errors == []


async def test_rows_written_counts_a_table_shared_by_sources_once(tab):
    """R2-2: each source's rows_written is its table's writer's total, and the tile summed them: two sources on one
    table counted it twice. The tile sums the tables' writers."""
    import json
    t, _, _ = tab

    async def health(route):
        body = json.loads(await (await route.fetch()).text())
        assert len(body) == 2                                       # sp and m, on one table
        for h in body.values():
            h["rows_written"] = 700
        await route.fulfill(json=body)

    async def status(route):
        body = json.loads(await (await route.fetch()).text())
        body["metrics"]["zerobus_targets"] = {"w.c.s.raw_messages": {"written": 700}}
        await route.fulfill(json=body)
    await t.route("**/api/sparkplug/health", health)
    await t.route("**/api/status", status)
    await t.goto("/")
    await playwright_api.expect(t.locator(".tile", has_text="Rows written").locator(".value")).to_have_text("700")
    assert t.errors == []


def _row(t, name):
    import re
    return t.locator("tbody tr").filter(has=t.locator("strong", has_text=re.compile(f"^{name}$")))


async def test_a_source_s_state_says_why_it_is_not_online(tab):
    """A running source showed Online (a primary host's state follows Zerobus) while the MQTT server refused its
    login. Now: Error with the reason; Waiting for Zerobus (a primary host held OFFLINE); Connecting...; Online;
    Subscribed (a plain subscriber)."""
    import json
    t, _, _ = tab
    await t.goto("/")                                   # Zerobus off, nothing connects (the fixture's clients)
    expect = playwright_api.expect
    await expect(_row(t, "sp").locator("td").nth(1)).to_have_text("Waiting for Zerobus")
    await expect(_row(t, "m").locator("td").nth(1)).to_have_text("Connecting…")

    async def health(route):
        body = json.loads(await (await route.fetch()).text())
        body["sp"].update(mqtt_connected=False, mqtt_last_error="MqttCodeError: [code:5] Connection Refused: not authorised.",
                          mqtt_failing_since="2026-10-29T19:57:32.772Z", state="ONLINE")
        body["m"].update(mqtt_connected=True)
        await route.fulfill(json=body)
    await t.route("**/api/sparkplug/health", health)
    await t.reload()
    await expect(_row(t, "sp").locator("td").nth(1)).to_have_text("Error")
    await expect(_row(t, "sp").locator("td.last-error")).to_contain_text("not authorised")
    await expect(_row(t, "m").locator("td").nth(1)).to_have_text("Subscribed")
    await t.goto("/sources.html")
    await expect(_row(t, "sp").locator("td").nth(5)).to_contain_text("Error")
    await expect(_row(t, "sp").locator("td").nth(5)).to_contain_text("not authorised")

    async def online(route):
        body = json.loads(await (await route.fetch()).text())
        body["sp"].update(mqtt_connected=True, mqtt_online=True, state="ONLINE")
        await route.fulfill(json=body)
    await t.unroute("**/api/sparkplug/health")
    await t.route("**/api/sparkplug/health", online)
    await t.goto("/")
    await expect(_row(t, "sp").locator("td").nth(1)).to_have_text("Online")
    assert t.errors == []


async def test_a_read_only_page_disables_its_controls(browser, tmp_path, monkeypatch, started):
    from test_web_ui import page_with
    async with page_with(tmp_path, monkeypatch, read_only=True) as (_, port, _):
        context = await browser.new_context(base_url=f"http://127.0.0.1:{port}")
        t = await context.new_page()
        errors = []
        t.on("pageerror", lambda e: errors.append(str(e)))
        await t.goto("/")
        await playwright_api.expect(t.locator(".topbar .read-only")).to_have_text("read-only")
        await playwright_api.expect(t.get_by_role("button", name="Stop the bridge")).to_be_disabled()
        await playwright_api.expect(t.get_by_role("button", name="Restart")).to_be_disabled()
        await t.goto("/sources.html")
        await playwright_api.expect(t.get_by_role("button", name="Add source")).to_be_disabled()
        await context.close()
        assert errors == []


async def test_a_settings_save_after_someone_else_s_says_reload(tab):
    t, path, _ = tab
    other = await t.context.new_page()
    for p in (t, other):
        await p.goto("/settings.html")
        await p.get_by_role("button", name="Expand all").click()
    for p, value in ((other, "7"), (t, "8")):
        await p.locator('[id="zerobus.drain_timeout_seconds"]').fill(value)
        await p.get_by_role("button", name="Review and save…").click()
        await p.locator("dialog[open]").get_by_role("button", name="Save", exact=True).click()
        if p is other:
            await playwright_api.expect(p.locator(".toast", has_text="Saved")).to_be_visible()
    await playwright_api.expect(t.locator(".form-error")).to_contain_text("changed since")
    assert yaml.safe_load(path.read_text())["zerobus"]["drain_timeout_seconds"] == 7             # the first save kept
    assert [e for e in t.errors if "status of 412" not in e] == []      # (Chrome logs the expected 412 itself)
