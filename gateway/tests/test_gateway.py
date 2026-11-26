"""Gateway ingest: envelope validation, storage, auth token, live broadcast."""
import asyncio
import json
import sqlite3

import pytest
from aiohttp.test_utils import TestClient, TestServer

import orb_gateway as g


def make_gw(tmp_path, token=""):
    server = {
        "host": "127.0.0.1",
        "port": 0,
        "data_dir": str(tmp_path),
        "log_file": str(tmp_path / "presence.jsonl"),
        "db_file": str(tmp_path / "orb.db"),
    }
    return g.Gateway(server=server, ingest_token=token)


def scan(node="node-01"):
    return {"v": 1, "node": node, "type": "scan", "ts": 1754316000,
            "payload": {"ap_bssid": "a8:f5:dd:ca:ac:3c",
                        "devices": [{"mac": "24:5f:9f:db:9f:72", "rssi": -62,
                                     "name": "watch", "company_id": 258}]}}


def app_for(gw):
    from aiohttp import web
    app = web.Application()
    app.router.add_post("/ingest", gw.handle_ingest)
    app.router.add_get("/ws", gw.handle_ws)
    return app


@pytest.mark.parametrize("bad", [
    None, [], {"v": 2, "node": "n", "type": "scan", "payload": {}},
    {"v": 1, "type": "scan", "payload": {}},
    {"v": 1, "node": "n", "type": "scan", "payload": "x"},
])
def test_invalid_envelopes_rejected(tmp_path, bad):
    status, body = make_gw(tmp_path).ingest(bad, "10.0.0.2")
    assert status == 400


def test_scan_is_stored(tmp_path):
    gw = make_gw(tmp_path)
    status, _ = gw.ingest(scan(), "10.0.0.2")
    assert status == 200
    c = sqlite3.connect(tmp_path / "orb.db")
    row = c.execute("SELECT node_id, mac, rssi FROM sightings").fetchone()
    assert row == ("node-01", "24:5f:9f:db:9f:72", -62)
    assert (tmp_path / "presence.jsonl").read_text().count("\n") == 1


def run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def test_token_required_when_configured(tmp_path):
    async def go():
        gw = make_gw(tmp_path, token="s3cret")
        async with TestClient(TestServer(app_for(gw))) as cl:
            r = await cl.post("/ingest", json=scan())
            assert r.status == 401
            r = await cl.post("/ingest", json=scan(), headers={"X-Mosaic-Token": "wrong"})
            assert r.status == 401
            r = await cl.post("/ingest", json=scan(), headers={"X-Mosaic-Token": "s3cret"})
            assert r.status == 200
            r = await cl.get("/ws")
            assert r.status == 401
            ws = await cl.ws_connect("/ws?token=s3cret")
            await ws.close()
    run(go())


def test_no_token_means_open(tmp_path):
    async def go():
        gw = make_gw(tmp_path)
        async with TestClient(TestServer(app_for(gw))) as cl:
            r = await cl.post("/ingest", json=scan())
            assert r.status == 200
    run(go())


def test_ws_subscriber_receives_broadcast(tmp_path):
    """Regression: send_str was called without await, so nothing arrived."""
    async def go():
        gw = make_gw(tmp_path)
        async with TestClient(TestServer(app_for(gw))) as cl:
            ws = await cl.ws_connect("/ws")
            for _ in range(50):  # wait until the server registered us
                if gw.subscribers:
                    break
                await asyncio.sleep(0.01)
            r = await cl.post("/ingest", json=scan("node-07"))
            assert r.status == 200
            msg = await asyncio.wait_for(ws.receive(), timeout=2)
            rec = json.loads(msg.data)
            assert rec["node"] == "node-07"
            assert rec["type"] == "scan"
            await ws.close()
    run(go())


def csi_raw(node="csi-01", frames=3):
    import base64
    amp = base64.b64encode(bytes(range(64))).decode()
    return {"v": 1, "node": node, "type": "csi_raw", "ts": 1754316000,
            "payload": {"n_sub": 64, "hz": 100, "channel": 6, "dropped": 0,
                        "frames": [[i * 10, -50, amp] for i in range(frames)]}}


def app_full(gw):
    app = app_for(gw)
    app.router.add_get("/live", gw.handle_live)
    app.router.add_get("/viz", gw.handle_viz)
    return app


def test_csi_raw_is_live_only(tmp_path):
    """csi_raw goes to /live viewers, is not stored, and is not echoed to /ws."""
    async def go():
        gw = make_gw(tmp_path)
        async with TestClient(TestServer(app_full(gw))) as cl:
            viewer = await cl.ws_connect("/live")
            node_ws = await cl.ws_connect("/ws")
            for _ in range(50):
                if gw.viewers and gw.subscribers:
                    break
                await asyncio.sleep(0.01)
            await node_ws.send_str(json.dumps(csi_raw()))
            ack = await asyncio.wait_for(node_ws.receive(), timeout=2)
            assert json.loads(ack.data) == {"ok": True}
            msg = await asyncio.wait_for(viewer.receive(), timeout=2)
            rec = json.loads(msg.data)
            assert rec["type"] == "csi_raw" and len(rec["payload"]["frames"]) == 3
            # the node must not get its own high-rate stream echoed back
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(node_ws.receive(), timeout=0.3)
            # a normal scan still reaches both
            r = await cl.post("/ingest", json=scan())
            assert r.status == 200
            assert json.loads((await asyncio.wait_for(viewer.receive(), 2)).data)["type"] == "scan"
            assert json.loads((await asyncio.wait_for(node_ws.receive(), 2)).data)["type"] == "scan"
            await viewer.close(); await node_ws.close()
        c = sqlite3.connect(tmp_path / "orb.db")
        assert c.execute("SELECT COUNT(*) FROM events WHERE type='csi_raw'").fetchone()[0] == 0
        assert "csi_raw" not in (tmp_path / "presence.jsonl").read_text()
        assert not (tmp_path / "csi_raw.jsonl").exists()
    run(go())


def test_csi_raw_record_opt_in(tmp_path):
    gw = make_gw(tmp_path)
    gw.csi_raw_record = True
    assert gw.ingest(csi_raw(), "10.0.0.9")[0] == 200
    assert (tmp_path / "csi_raw.jsonl").read_text().count("\n") == 1


def test_viz_page_served_and_live_requires_token(tmp_path):
    async def go():
        gw = make_gw(tmp_path, token="t")
        async with TestClient(TestServer(app_full(gw))) as cl:
            r = await cl.get("/viz")
            assert r.status == 200
            assert "CSI наживо" in await r.text()
            assert (await cl.get("/live")).status == 401
            ws = await cl.ws_connect("/live?token=t")
            await ws.close()
    run(go())
