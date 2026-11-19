"""停用排除链路的双门测试。

按排除专册顺序：
1. test_deactivated_source_must_not_raise_water_level —— 排除失败测。
   停用源再来到达不得抬水位，历史到达只读保留。实现前必须红。
2. test_deactivate_and_refresh_contention_leaves_trace —— 双路抢刷失败测。
   停用操作与水位刷新抢写须排队占锁，后到只记「停用排除争用」。
   缺排除争用留痕必须红。

两测双红之后，才允许改排除写路径。
"""
import threading
import time

import pytest
from fastapi.testclient import TestClient

import app as lag
from app import app, init_db


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    init_db()
    yield


@pytest.fixture()
def client():
    return TestClient(app)


@pytest.fixture()
def head(client):
    tok = client.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
    return {"Authorization": "Bearer " + tok}


def _arrivals_of(client, head, source, scope=None):
    url = "/api/arrivals" + (f"?scope={scope}" if scope else "")
    return [a for a in client.get(url, headers=head).json()["items"] if a["source"] == source]


def test_deactivated_source_must_not_raise_water_level(client, head):
    # 停用：走时限台源行菜单对应的排除接口
    r = client.post("/api/sources/订单源/deactivate", headers=head)
    assert r.status_code == 200, r.text
    assert r.json()["output_mode"] == "停用排除"

    hist_before = _arrivals_of(client, head, "订单源", scope="all")

    # 停用后来到大滞后到达：不得抬水位
    r = client.post(
        "/api/arrivals",
        headers=head,
        json={"source": "订单源", "source_clock": "2026-11-19T08:00:00", "ingest_clock": "2026-11-19T10:00:00"},
    )
    assert r.status_code == 409, r.text

    # 水位保持「停用排除」，未被抬高
    src = [s for s in client.get("/api/sources?scope=all", headers=head).json()["items"] if s["name"] == "订单源"][0]
    assert src["output_mode"] == "停用排除"
    assert src["deactivated"] == 1

    # 历史到达只读保留：未新增、未物理删
    hist_after = _arrivals_of(client, head, "订单源", scope="all")
    assert [a["id"] for a in hist_after] == [a["id"] for a in hist_before]


def test_deactivate_and_refresh_contention_leaves_trace(client, head):
    before = len(client.get("/api/arrivals?scope=all", headers=head).json()["items"])

    # 主线程先占写锁，保证停用与刷新两路都撞锁排队
    lag.EXCLUSION_LOCK.acquire()
    results = {}

    def do_deactivate():
        results["deactivate"] = client.post("/api/sources/支付源/deactivate", headers=head)

    def do_refresh():
        results["refresh"] = client.post(
            "/api/arrivals",
            headers=head,
            json={"source": "物流源", "source_clock": "2026-11-19T07:00:00", "ingest_clock": "2026-11-19T08:10:00"},
        )

    t1 = threading.Thread(target=do_deactivate)
    t2 = threading.Thread(target=do_refresh)
    t1.start()
    t2.start()
    time.sleep(0.5)  # 让两路都走到抢锁点
    lag.EXCLUSION_LOCK.release()
    t1.join(timeout=10)
    t2.join(timeout=10)
    assert not t1.is_alive() and not t2.is_alive(), "排队占锁未放行"

    # 两路最终都完成（不同源，无先后歧义）
    assert results["deactivate"].status_code == 200, results["deactivate"].text
    assert results["refresh"].status_code == 200, results["refresh"].text

    # 后到只记「停用排除争用」：缺留痕则红
    events = client.get("/api/exclusion/events", headers=head).json()["items"]
    contention = [e for e in events if e["action"] == "停用排除争用"]
    assert len(contention) >= 1, f"缺排除争用留痕: {events}"

    # 史迹未被动过
    after = len(client.get("/api/arrivals?scope=all", headers=head).json()["items"])
    assert after == before + 1


def test_five_surfaces_exclude_deactivated_and_history_readable(client, head):
    # 造齐五处素材：支付源有到达、有补齐账，然后停用
    client.post("/api/catchup/支付源", headers=head)
    assert client.post("/api/sources/支付源/deactivate", headers=head).status_code == 200

    # 1) 源目录 + 2) 策略附页（同源数据）须排除停用
    names = [s["name"] for s in client.get("/api/sources", headers=head).json()["items"]]
    assert "支付源" not in names and "订单源" in names

    # 3) 水位卡片只聚合在营源
    s = client.get("/api/summary", headers=head).json()
    assert s["total"] == 2 and s["deactivated"] == 1 and s["degraded"] == 0

    # 4) 到达列表须排除停用
    srcs = {a["source"] for a in client.get("/api/arrivals", headers=head).json()["items"]}
    assert "支付源" not in srcs

    # 5) 对账账册须排除停用
    srcs = {k["source"] for k in client.get("/api/catchups", headers=head).json()["items"]}
    assert "支付源" not in srcs

    # 史迹可读：scope=all 只读保留停用源及其到达与账
    allsrc = {x["name"]: x for x in client.get("/api/sources?scope=all", headers=head).json()["items"]}
    assert allsrc["支付源"]["deactivated"] == 1 and allsrc["支付源"]["output_mode"] == "停用排除"
    hist = {a["source"] for a in client.get("/api/arrivals?scope=all", headers=head).json()["items"]}
    assert "支付源" in hist
    ledg = {k["source"] for k in client.get("/api/catchups?scope=all", headers=head).json()["items"]}
    assert "支付源" in ledg


def test_physical_delete_of_history_refused_and_logged(client, head):
    before = len(client.get("/api/arrivals?scope=all", headers=head).json()["items"])
    r = client.delete("/api/arrivals/1", headers=head)
    assert r.status_code == 409 and "禁止物理删史" in r.json()["detail"]
    events = client.get("/api/exclusion/events", headers=head).json()["items"]
    assert any(e["action"] == "排除偏了" for e in events)
    after = len(client.get("/api/arrivals?scope=all", headers=head).json()["items"])
    assert after == before  # 历史到达一条不少


def test_path_mixing_refused_and_logged(client, head):
    r = client.post("/api/sources/订单源/deactivate", headers=head, json={"path": "乙"})
    assert r.status_code == 409 and "混用" in r.json()["detail"]
    events = client.get("/api/exclusion/events", headers=head).json()["items"]
    assert any(e["action"] == "排除偏了" and "混用" in e["detail"] for e in events)
    src = [s for s in client.get("/api/sources?scope=all", headers=head).json()["items"] if s["name"] == "订单源"][0]
    assert src["deactivated"] == 0  # 被拒绝，未停用


def test_manual_pinned_and_double_deactivate_rejected(client, head):
    m = client.get("/api/exclusion/manual", headers=head).json()
    assert m["path"] == "甲" and m["pinned_at"]
    assert client.put("/api/exclusion/manual", headers=head, json={}).status_code == 405

    assert client.post("/api/sources/物流源/deactivate", headers=head).status_code == 200
    r = client.post("/api/sources/物流源/deactivate", headers=head)
    assert r.status_code == 409 and "已停用排除" in r.json()["detail"]
    events = client.get("/api/exclusion/events", headers=head).json()["items"]
    assert len([e for e in events if e["action"] == "停用排除" and e["source"] == "物流源"]) == 1


def test_catchup_on_deactivated_source_rejected(client, head):
    assert client.post("/api/sources/订单源/deactivate", headers=head).status_code == 200
    r = client.post("/api/catchup/订单源", headers=head)
    assert r.status_code == 409 and "停用排除" in r.json()["detail"]
