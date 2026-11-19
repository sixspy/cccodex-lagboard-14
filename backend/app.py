
import hashlib, secrets, sqlite3, threading
from pathlib import Path
from fastapi import Body, Header, HTTPException
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

# 停用排除专册（封板钉死，见 docs/停用排除说明.md）：甲路径，甲乙不混用。
MANUAL_PATH = "甲"
MANUAL_NOTE = "停用标记与水位聚合字段同库一次提交排除该源，历史到达只读保留；不启用影子表，甲乙不混用。"
# 停用操作与水位刷新抢写：排队占锁，后到只记「停用排除争用」。
EXCLUSION_LOCK = threading.Lock()

def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()

def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))

class LoginIn(BaseModel):
    username: str
    password: str

def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u

def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")

def record_event(source: str, action: str, detail: str):
    """排除留痕：独立连接落库，不随业务事务回滚。"""
    with connect() as c:
        c.execute("INSERT INTO exclusion_events(source,action,detail,created_at) VALUES (?,?,?,?)",
                  (source, action, detail, now_iso()))

def acquire_exclusion_write(op: str, source: str):
    """排队占锁：抢写时先到者持锁，后到者只记「停用排除争用」再排队。"""
    if not EXCLUSION_LOCK.acquire(blocking=False):
        record_event(source, "停用排除争用", f"{op} 与在写方抢写，排队占锁")
        EXCLUSION_LOCK.acquire()

def release_exclusion_write():
    EXCLUSION_LOCK.release()

def refuse_deviation(source: str, detail: str):
    """偏离停用排除册：拒绝并记「排除偏了」。"""
    record_event(source, "排除偏了", detail)
    raise HTTPException(409, "偏离停用排除册：" + detail)

from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

PRODUCT = "源到达SLA"
SKELETON = "多源到达时钟对照 SLA 分级，超限则冻结或降级输出，补齐后恢复并对账"

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS sources(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT UNIQUE NOT NULL,
          sla_minutes INTEGER NOT NULL,
          output_mode TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS arrivals(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source TEXT NOT NULL,
          source_clock TEXT NOT NULL,
          ingest_clock TEXT NOT NULL,
          lag_minutes INTEGER NOT NULL,
          level TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS catchups(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source TEXT NOT NULL,
          status TEXT NOT NULL,
          restored_mode TEXT NOT NULL,
          note TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS exclusion_events(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source TEXT NOT NULL,
          action TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS exclusion_manual(
          id INTEGER PRIMARY KEY CHECK(id=1),
          path TEXT NOT NULL,
          note TEXT NOT NULL,
          pinned_at TEXT NOT NULL
        );
        """)
        cols = [r["name"] for r in conn.execute("PRAGMA table_info(sources)").fetchall()]
        if "deactivated" not in cols:
            conn.execute("ALTER TABLE sources ADD COLUMN deactivated INTEGER NOT NULL DEFAULT 0")
            conn.execute("ALTER TABLE sources ADD COLUMN deactivated_at TEXT")
        if conn.execute("SELECT COUNT(*) c FROM exclusion_manual").fetchone()["c"] == 0:
            conn.execute("INSERT INTO exclusion_manual VALUES (1,?,?,?)",
                         (MANUAL_PATH, MANUAL_NOTE, now_iso()))
        if conn.execute("SELECT COUNT(*) c FROM sources").fetchone()["c"] == 0:
            conn.executemany("INSERT INTO sources(name,sla_minutes,output_mode) VALUES (?,?,?)", [
                ("订单源", 15, "可写"), ("支付源", 20, "降级只读"), ("物流源", 30, "冻结"),
            ])
            conn.execute("INSERT INTO arrivals(source,source_clock,ingest_clock,lag_minutes,level) VALUES (?,?,?,?,?)",
                         ("支付源","2026-11-16T08:00:00","2026-11-16T08:35:00",35,"告警"))
            conn.execute("INSERT INTO arrivals(source,source_clock,ingest_clock,lag_minutes,level) VALUES (?,?,?,?,?)",
                         ("物流源","2026-11-16T07:00:00","2026-11-16T09:10:00",130,"违约"))

def level_of(lag, sla):
    if lag <= sla: return "正常"
    if lag <= sla * 2: return "告警"
    return "违约"

def mode_of(level):
    return {"正常":"可写", "告警":"降级只读", "违约":"冻结"}[level]

@app.on_event("startup")
def _s(): init_db()

@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM sources").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "sources": n, "db": str(DB_PATH)}

@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username,role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row: raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok]=row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}

@app.get("/api/sources")
def sources(scope: str = "live", authorization: str | None = Header(default=None)):
    require_user(authorization)
    # 源目录默认排除停用；scope=all 仅供史迹只读
    sql = "SELECT * FROM sources" + ("" if scope == "all" else " WHERE deactivated=0") + " ORDER BY id"
    with connect() as c:
        return {"items":[dict(r) for r in c.execute(sql).fetchall()]}

@app.get("/api/summary")
def summary(authorization: str | None = Header(default=None)):
    require_user(authorization)
    # 水位卡片只聚合在营源，停用源排除在外
    with connect() as c:
        rows = c.execute("SELECT output_mode, COUNT(*) c FROM sources WHERE deactivated=0 GROUP BY output_mode").fetchall()
        d = {r["output_mode"]: r["c"] for r in rows}
        deact = c.execute("SELECT COUNT(*) c FROM sources WHERE deactivated=1").fetchone()["c"]
    return {"total": sum(d.values()), "writable": d.get("可写", 0),
            "degraded": d.get("降级只读", 0), "frozen": d.get("冻结", 0), "deactivated": deact}

class DeactivateIn(BaseModel):
    path: str | None = None

@app.post("/api/sources/{source}/deactivate")
def deactivate(source: str, body: DeactivateIn | None = Body(default=None),
               authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        manual = c.execute("SELECT path FROM exclusion_manual WHERE id=1").fetchone()
    if body and body.path and body.path != manual["path"]:
        refuse_deviation(source, f"试图走{body.path}路径影子表排除，与钉死的{manual['path']}路径混用")
    acquire_exclusion_write("停用操作", source)
    try:
        # 甲路径：停用标记与水位聚合字段同库一次提交，历史到达只读保留
        with connect() as c:
            src = c.execute("SELECT * FROM sources WHERE name=?", (source,)).fetchone()
            if not src: raise HTTPException(404, "未知源")
            if src["deactivated"]: raise HTTPException(409, "已停用排除")
            now = now_iso()
            c.execute("UPDATE sources SET deactivated=1, deactivated_at=?, output_mode='停用排除' WHERE name=?",
                      (now, source))
            c.execute("INSERT INTO exclusion_events(source,action,detail,created_at) VALUES (?,?,?,?)",
                      (source, "停用排除", "甲路径：停用标记与水位聚合字段同库一次提交，历史到达只读保留", now))
            return {"ok": True, "source": source, "output_mode": "停用排除"}
    finally:
        release_exclusion_write()

class ArrivalIn(BaseModel):
    source: str
    source_clock: str
    ingest_clock: str

@app.post("/api/arrivals")
def add_arrival(body: ArrivalIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    acquire_exclusion_write("水位刷新", body.source)
    try:
        with connect() as c:
            src = c.execute("SELECT * FROM sources WHERE name=?", (body.source,)).fetchone()
            if not src: raise HTTPException(404, "未知源")
            if src["deactivated"]: raise HTTPException(409, "源已停用排除，水位不再刷新")
            # lag by minute rough parse
            def parse(t):
                return datetime.fromisoformat(t)
            lag = int((parse(body.ingest_clock) - parse(body.source_clock)).total_seconds() // 60)
            if lag < 0: lag = 0
            lv = level_of(lag, src["sla_minutes"])
            mode = mode_of(lv)
            c.execute("INSERT INTO arrivals(source,source_clock,ingest_clock,lag_minutes,level) VALUES (?,?,?,?,?)",
                      (body.source, body.source_clock, body.ingest_clock, lag, lv))
            c.execute("UPDATE sources SET output_mode=? WHERE name=?", (mode, body.source))
            return {"lag_minutes": lag, "level": lv, "output_mode": mode}
    finally:
        release_exclusion_write()

@app.get("/api/arrivals")
def arrivals(scope: str = "live", authorization: str | None = Header(default=None)):
    require_user(authorization)
    # 到达列表默认排除停用源；scope=all 史迹只读
    sql = ("SELECT * FROM arrivals ORDER BY id DESC" if scope == "all" else
           """SELECT a.* FROM arrivals a JOIN sources s ON s.name=a.source
              WHERE s.deactivated=0 ORDER BY a.id DESC""")
    with connect() as c:
        return {"items":[dict(r) for r in c.execute(sql).fetchall()]}

@app.delete("/api/arrivals/{arrival_id}")
def delete_arrival(arrival_id: int, authorization: str | None = Header(default=None)):
    require_user(authorization)
    # 排除专册禁止物理删史：拒绝并记「排除偏了」
    refuse_deviation("—", f"试图物理删除历史到达 #{arrival_id}，排除专册禁止物理删史")

@app.post("/api/catchup/{source}")
def catchup(source: str, authorization: str | None = Header(default=None)):
    require_user(authorization)
    now = now_iso()
    acquire_exclusion_write("补齐恢复", source)
    try:
        with connect() as c:
            src = c.execute("SELECT * FROM sources WHERE name=?", (source,)).fetchone()
            if not src: raise HTTPException(404, "未知源")
            if src["deactivated"]: raise HTTPException(409, "源已停用排除，对账账册不再登记补齐")
            c.execute("UPDATE sources SET output_mode='可写' WHERE name=?", (source,))
            c.execute("INSERT INTO catchups(source,status,restored_mode,note,created_at) VALUES (?,?,?,?,?)",
                      (source, "成功", "可写", "补齐后恢复可写", now))
            return {"ok": True, "output_mode": "可写"}
    finally:
        release_exclusion_write()

@app.get("/api/catchups")
def catchups(scope: str = "live", authorization: str | None = Header(default=None)):
    require_user(authorization)
    # 对账账册默认排除停用源；scope=all 史迹只读
    sql = ("SELECT * FROM catchups ORDER BY id DESC" if scope == "all" else
           """SELECT k.* FROM catchups k JOIN sources s ON s.name=k.source
              WHERE s.deactivated=0 ORDER BY k.id DESC""")
    with connect() as c:
        return {"items":[dict(r) for r in c.execute(sql).fetchall()]}

@app.get("/api/exclusion/events")
def exclusion_events(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM exclusion_events ORDER BY id DESC").fetchall()]}

@app.get("/api/exclusion/manual")
def exclusion_manual(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return dict(c.execute("SELECT path,note,pinned_at FROM exclusion_manual WHERE id=1").fetchone())

@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")
app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
