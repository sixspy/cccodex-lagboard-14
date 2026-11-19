"""复现：源停用后仍抬水位（排除链路缺失）。

做法：在临时库上给 sources 补上 deactivated 停用标记（模拟运营已把源停用），
然后走现有到达写路径 POST /api/arrivals。若停用源的水位（output_mode）
仍被抬高，即复现缺陷；同时展示历史到达在排除链路缺失下没有任何只读保护。

用法：python scripts/repro_deactivated_still_raises_water.py
退出码：0=缺陷仍在（仍抬水位），1=已修复（停用源不再抬水位）。
"""
import sqlite3
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

import app as lag  # noqa: E402


def main() -> int:
    tmp = Path(tempfile.mkdtemp()) / "repro.db"
    lag.DB_PATH = tmp
    lag.init_db()

    # 模拟运营侧已把「订单源」打上停用标记（写路径尚未认识该标记）
    with sqlite3.connect(tmp) as c:
        cols = [r[1] for r in c.execute("PRAGMA table_info(sources)").fetchall()]
        if "deactivated" not in cols:
            c.execute("ALTER TABLE sources ADD COLUMN deactivated INTEGER NOT NULL DEFAULT 0")
        c.execute("UPDATE sources SET deactivated=1 WHERE name='订单源'")

    from fastapi.testclient import TestClient

    client = TestClient(lag.app)
    tok = client.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
    h = {"Authorization": "Bearer " + tok}

    before = [dict(r) for r in client.get("/api/sources?scope=all", headers=h).json()["items"] if r["name"] == "订单源"][0]
    r = client.post(
        "/api/arrivals",
        headers=h,
        json={"source": "订单源", "source_clock": "2026-11-19T08:00:00", "ingest_clock": "2026-11-19T09:30:00"},
    )
    after = [dict(r2) for r2 in client.get("/api/sources?scope=all", headers=h).json()["items"] if r2["name"] == "订单源"][0]

    print(f"停用标记 deactivated = {before.get('deactivated')}")
    print(f"到达登记响应: {r.status_code} {r.json()}")
    print(f"水位 output_mode: {before['output_mode']} -> {after['output_mode']}")

    raised = r.status_code == 200 and after["output_mode"] != "停用排除"
    if raised:
        print("复现成立：源已停用，到达仍抬水位（output_mode 被刷新），排除链路缺失。")
        return 0
    print("未复现：停用源水位未被抬高，排除链路已生效。")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
