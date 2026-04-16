import os
import time
from html import escape
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from zoneinfo import ZoneInfo
from urllib.parse import urlencode

from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from dotenv import load_dotenv
import uvicorn

from db import (
    init_db,
    get_groups,
    get_transactions,
    get_dashboard_stats,
    get_access_users_page,
    count_access_users_filtered,
    get_access_user_by_id,
    extend_access_user,
    set_access_user_permanent,
    remove_access_user,
    get_rental_orders_by_status,
    mark_rental_order_rejected,
    get_rental_order,
    approve_rental_order,
)



# ================= ENV =================
load_dotenv()

# Ưu tiên WEB_TOKEN để khớp với app.py
WEB_TOKEN = (os.getenv("WEB_TOKEN") or os.getenv("WEB_ADMIN_TOKEN") or "").strip()
PORT = int(os.getenv("PORT", "8080"))

BEIJING_TZ = ZoneInfo("Asia/Shanghai")


# ================= APP =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Ledger Web", version="1.0.0", lifespan=lifespan)


# ================= AUTH =================
def check_token(token: str | None):
    """
    Nếu WEB_TOKEN trống thì cho xem tự do.
    Nếu có token thì phải truyền ?token=...
    """
    if not WEB_TOKEN:
        return True
    return token == WEB_TOKEN


def require_token(token: str | None):
    if not check_token(token):
        raise HTTPException(status_code=401, detail="Unauthorized")


# ================= HELPERS =================
def fmt_num(x):
    if x is None:
        return "0"
    try:
        x = float(x)
        if abs(x - int(x)) < 1e-9:
            return str(int(x))
        return f"{x:.2f}".rstrip("0").rstrip(".")
    except Exception:
        return str(x)


def fmt_ts(ts):
    if not ts:
        return "-"
    try:
        ts = int(ts)
        if ts > 10_000_000_000:
            ts = ts // 1000
        return datetime.fromtimestamp(ts, BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "-"


def summarize_transactions(txs):
    income = [t for t in txs if t[6] == "income" and not t[14]]
    payout = [t for t in txs if t[6] == "payout" and not t[14]]
    reserve = [t for t in txs if t[6] == "reserve" and not t[14]]

    total_income_unit = sum((t[8] or 0) for t in income)
    total_payout_unit = sum((t[8] or 0) for t in payout)
    total_reserve_unit = sum((t[8] or 0) for t in reserve)

    due = total_income_unit + total_reserve_unit
    paid = total_payout_unit
    pending = due - paid

    total_raw_income = sum((abs(t[7]) or 0) for t in income if t[7] is not None)

    return {
        "income_count": len(income),
        "payout_count": len(payout),
        "reserve_count": len(reserve),
        "total_income_unit": total_income_unit,
        "total_payout_unit": total_payout_unit,
        "total_reserve_unit": total_reserve_unit,
        "due": due,
        "paid": paid,
        "pending": pending,
        "total_raw_income": total_raw_income,
        "undone_count": len([t for t in txs if t[14]]),
    }


def parse_web_date(date_str: str | None):
    """
    date_str: YYYY-MM-DD
    Theo Asia/Shanghai
    """
    if not date_str:
        dt = datetime.now(BEIJING_TZ)
    else:
        try:
            parsed = datetime.strptime(date_str, "%Y-%m-%d")
            dt = parsed.replace(tzinfo=BEIJING_TZ)
        except Exception:
            dt = datetime.now(BEIJING_TZ)

    start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1) - timedelta(seconds=1)

    return {
        "date_str": start.strftime("%Y-%m-%d"),
        "start_ts": int(start.timestamp()),
        "end_ts": int(end.timestamp()),
        "start_dt": start,
        "end_dt": end,
    }


def get_group_title_map():
    return {int(chat_id): title for chat_id, title in get_groups()}


def build_url(path: str, token: str | None = None, **params):
    q = {}
    for k, v in params.items():
        if v is not None:
            q[k] = v
    if token:
        q["token"] = token

    if q:
        return f"{path}?{urlencode(q)}"
    return path


def kind_label(kind: str):
    return {
        "income": "入款",
        "payout": "下发",
        "reserve": "寄存",
    }.get(kind, kind or "-")


def tx_row_class(kind: str, undone: bool):
    if undone:
        return "undone"
    if kind == "income":
        return "income"
    if kind == "payout":
        return "payout"
    if kind == "reserve":
        return "reserve"
    return ""
    
def access_status(expires_at):
    now_ts = int(time.time())
    if expires_at is None:
        return "permanent", "永久"
    if int(expires_at) > now_ts:
        return "active", "有效"
    return "expired", "已过期"
  

def page_shell(title: str, body_html: str):
    html = f"""
    <!doctype html>
    <html lang="zh-CN">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>{escape(title)}</title>
        <style>
            :root {{
                --bg: #0f172a;
                --panel: #111827;
                --panel-2: #1f2937;
                --border: #374151;
                --text: #e5e7eb;
                --muted: #9ca3af;
                --blue: #2563eb;
                --blue-2: #60a5fa;
                --green: #16a34a;
                --red: #dc2626;
                --yellow: #d97706;
                --gray: #6b7280;
            }}

            * {{
                box-sizing: border-box;
            }}

            body {{
                margin: 0;
                padding: 20px;
                font-family: Arial, sans-serif;
                background: var(--bg);
                color: var(--text);
            }}

            .container {{
                max-width: 1600px;
                margin: auto;
                background: var(--panel);
                border-radius: 16px;
                padding: 20px;
                box-shadow: 0 10px 30px rgba(0,0,0,.35);
            }}

            h1, h2, h3 {{
                margin: 0 0 10px 0;
            }}

            .muted {{
                color: var(--muted);
                font-size: 14px;
                line-height: 1.6;
            }}

            .topbar {{
                display: flex;
                justify-content: space-between;
                gap: 12px;
                align-items: center;
                flex-wrap: wrap;
                margin-bottom: 16px;
            }}

            .btn {{
                display: inline-block;
                padding: 10px 14px;
                border-radius: 10px;
                background: var(--blue);
                color: white;
                text-decoration: none;
                border: 0;
                cursor: pointer;
                font-size: 14px;
            }}

            .btn:hover {{
                opacity: .92;
                text-decoration: none;
            }}

            .btn.secondary {{
                background: var(--panel-2);
                border: 1px solid var(--border);
            }}

            .tag {{
                display: inline-block;
                padding: 4px 8px;
                border-radius: 999px;
                background: var(--panel-2);
                color: white;
                font-size: 12px;
                border: 1px solid var(--border);
            }}

            .tag.ok {{
                background: rgba(22,163,74,.15);
                border-color: rgba(22,163,74,.4);
                color: #86efac;
            }}

            .tag.warn {{
                background: rgba(217,119,6,.15);
                border-color: rgba(217,119,6,.4);
                color: #fdba74;
            }}

            .tag.bad {{
                background: rgba(220,38,38,.15);
                border-color: rgba(220,38,38,.4);
                color: #fca5a5;
            }}

            .stats {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                gap: 12px;
                margin: 18px 0;
            }}

            .card {{
                background: var(--panel-2);
                border-radius: 12px;
                padding: 14px;
                border: 1px solid var(--border);
            }}

            .card .label {{
                color: var(--muted);
                font-size: 13px;
            }}

            .card .value {{
                font-size: 22px;
                font-weight: 700;
                margin-top: 6px;
                word-break: break-word;
            }}

            .filters {{
                display: flex;
                gap: 10px;
                flex-wrap: wrap;
                align-items: center;
                margin: 14px 0 6px;
            }}

            input[type="date"] {{
                background: #0b1220;
                color: var(--text);
                border: 1px solid var(--border);
                padding: 10px 12px;
                border-radius: 10px;
            }}

            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 14px;
                overflow: hidden;
                border-radius: 12px;
            }}

            th, td {{
                border-bottom: 1px solid var(--border);
                padding: 10px 8px;
                font-size: 14px;
                text-align: left;
                vertical-align: top;
                white-space: nowrap;
            }}

            th {{
                background: var(--panel-2);
                color: #f9fafb;
                position: sticky;
                top: 0;
                z-index: 1;
            }}

            tr:hover td {{
                background: rgba(255,255,255,0.03);
            }}

            .table-wrap {{
                overflow-x: auto;
                margin-top: 12px;
            }}

            .row-income td:first-child {{
                border-left: 4px solid #16a34a;
            }}

            .row-payout td:first-child {{
                border-left: 4px solid #dc2626;
            }}

            .row-reserve td:first-child {{
                border-left: 4px solid #d97706;
            }}

            .row-undone td {{
                opacity: .62;
                text-decoration: line-through;
            }}

            a {{
                color: var(--blue-2);
                text-decoration: none;
            }}

            a:hover {{
                text-decoration: underline;
            }}

            code {{
                background: rgba(255,255,255,.06);
                padding: 2px 6px;
                border-radius: 6px;
            }}

            .empty {{
                text-align: center;
                color: var(--muted);
                padding: 20px 0;
            }}

            @media (max-width: 768px) {{
                body {{
                    padding: 10px;
                }}

                .container {{
                    padding: 14px;
                    border-radius: 12px;
                }}

                .stats {{
                    grid-template-columns: repeat(2, minmax(0, 1fr));
                }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            {body_html}
        </div>
    </body>
    </html>
    """
    return HTMLResponse(html)


# ================= RENDER PAGES =================
def render_groups_page(token: str | None = None):
    groups = get_groups()
    today = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")

    rows = ""
    for chat_id, title in groups:
        link = build_url(f"/group/{chat_id}", token=token, date=today)
        rows += f"""
        <tr>
            <td>{escape(title or 'Unnamed')}</td>
            <td><span class="tag">{chat_id}</span></td>
            <td>
                <a class="btn" href="{link}">查看历史</a>
            </td>
        </tr>
        """

    if not rows:
        rows = """
        <tr>
            <td colspan="3" class="empty">暂无群组记录</td>
        </tr>
        """

    token_note = (
        '<span class="tag ok">已启用 Token 保护</span>'
        if WEB_TOKEN else
        '<span class="tag warn">未设置 Token，当前网页为公开访问</span>'
    )

    body = f"""
    <div class="topbar">
        <div>
            <h1>📋 群组列表</h1>
            <div class="muted">
                选择一个群组，查看对应日期的交易历史。<br>
                当前时间：{escape(datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S"))}（北京时间）
            </div>
        </div>
        <div>{token_note}</div>
    </div>

    <table>
        <thead>
            <tr>
                <th>群组名称</th>
                <th>Chat ID</th>
                <th>操作</th>
            </tr>
        </thead>
        <tbody>
            {rows}
        </tbody>
    </table>
    """
    return page_shell("群组列表", body)


def render_group_history_page(chat_id: int, date_str: str | None = None, token: str | None = None):
    groups = get_group_title_map()
    group_title = groups.get(int(chat_id), f"Group {chat_id}")

    day = parse_web_date(date_str)

    txs = get_transactions(
        chat_id,
        start_ts=day["start_ts"],
        end_ts=day["end_ts"],
        include_undone=True,
    )
    stats = summarize_transactions(txs)

    income_txs = [t for t in txs if t[6] == "income" and not t[14]]
    payout_txs = [t for t in txs if t[6] == "payout" and not t[14]]
    reserve_txs = [t for t in txs if t[6] == "reserve" and not t[14]]

    prev_day = (day["start_dt"] - timedelta(days=1)).strftime("%Y-%m-%d")
    next_day = (day["start_dt"] + timedelta(days=1)).strftime("%Y-%m-%d")
    today = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")

    token_input = f'<input type="hidden" name="token" value="{escape(token)}">' if token else ""

    rows_html = ""
    if txs:
        for tx in txs:
            (
                tx_id, c_id, user_id, username, display_name, target_name, kind,
                raw_amount, unit_amount, rate_used, fee_used, note, original_text,
                created_at, undone
            ) = tx

            tm = datetime.fromtimestamp(created_at, BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
            status = '<span class="tag ok">正常</span>' if not undone else '<span class="tag bad">已撤销</span>'

            row_class = tx_row_class(kind, undone)
            row_class = {
                "income": "row-income",
                "payout": "row-payout",
                "reserve": "row-reserve",
                "undone": "row-undone",
            }.get(row_class, "")

            rows_html += f"""
            <tr class="{row_class}">
                <td>{escape(tm)}</td>
                <td>{escape(kind_label(kind))}</td>
                <td>{escape(fmt_num(raw_amount) if raw_amount is not None else '-')}</td>
                <td>{escape(fmt_num(unit_amount))}U</td>
                <td>{escape(fmt_num(rate_used))}</td>
                <td>{escape(fmt_num(fee_used))}%</td>
                <td>{escape(display_name or '')}</td>
                <td>{escape(target_name or '')}</td>
                <td>{escape(username or '')}</td>
                <td>{escape(note or '')}</td>
                <td>{escape(original_text or '')}</td>
                <td>{status}</td>
            </tr>
            """
    else:
        rows_html = """
        <tr>
            <td colspan="12" class="empty">当天暂无交易记录</td>
        </tr>
        """

    back_link = build_url("/groups", token=token)

    today_link = build_url(f"/group/{chat_id}", token=token, date=today)
    prev_link = build_url(f"/group/{chat_id}", token=token, date=prev_day)
    next_link = build_url(f"/group/{chat_id}", token=token, date=next_day)

    body = f"""
    <div class="topbar">
        <div>
            <h1>📘 群组交易历史</h1>
            <div class="muted">
                群组：<b>{escape(group_title)}</b> |
                Chat ID：<span class="tag">{chat_id}</span> |
                日期：<span class="tag">{escape(day["date_str"])}</span>
            </div>
        </div>
        <div>
            <a class="btn secondary" href="{back_link}">← 返回群组列表</a>
        </div>
    </div>

    <form class="filters" method="get" action="/group/{chat_id}">
        {token_input}
        <label for="date">选择日期：</label>
        <input type="date" id="date" name="date" value="{escape(day["date_str"])}">
        <button class="btn" type="submit">查看</button>
        <a class="btn secondary" href="{today_link}">今天</a>
        <a class="btn secondary" href="{prev_link}">前一天</a>
        <a class="btn secondary" href="{next_link}">后一天</a>
    </form>

    <div class="stats">
        <div class="card">
            <div class="label">总记录数</div>
            <div class="value">{len(txs)}</div>
        </div>
        <div class="card">
            <div class="label">正常入款</div>
            <div class="value">{fmt_num(stats["total_income_unit"])}U</div>
        </div>
        <div class="card">
            <div class="label">正常下发</div>
            <div class="value">{fmt_num(stats["total_payout_unit"])}U</div>
        </div>
        <div class="card">
            <div class="label">正常寄存</div>
            <div class="value">{fmt_num(stats["total_reserve_unit"])}U</div>
        </div>
        <div class="card">
            <div class="label">待下发</div>
            <div class="value">{fmt_num(stats["pending"])}U</div>
        </div>
        <div class="card">
            <div class="label">入款笔数</div>
            <div class="value">{len(income_txs)}</div>
        </div>
        <div class="card">
            <div class="label">下发笔数</div>
            <div class="value">{len(payout_txs)}</div>
        </div>
        <div class="card">
            <div class="label">寄存笔数</div>
            <div class="value">{len(reserve_txs)}</div>
        </div>
        <div class="card">
            <div class="label">撤销记录</div>
            <div class="value">{stats["undone_count"]}</div>
        </div>
        <div class="card">
            <div class="label">原始入款总额</div>
            <div class="value">{fmt_num(stats["total_raw_income"])}</div>
        </div>
        <div class="card">
            <div class="label">应下发</div>
            <div class="value">{fmt_num(stats["due"])}U</div>
        </div>
        <div class="card">
            <div class="label">已下发</div>
            <div class="value">{fmt_num(stats["paid"])}U</div>
        </div>
    </div>

    <div class="muted">
        统计范围：<b>{escape(day["start_dt"].strftime("%Y-%m-%d %H:%M:%S"))}</b>
        至
        <b>{escape(day["end_dt"].strftime("%Y-%m-%d %H:%M:%S"))}</b>
        （北京时间）
    </div>

    <h2 style="margin-top:16px;">交易明细</h2>
    <div class="table-wrap">
        <table>
            <thead>
                <tr>
                    <th>时间</th>
                    <th>类型</th>
                    <th>Raw</th>
                    <th>U</th>
                    <th>Rate</th>
                    <th>Fee</th>
                    <th>记录人</th>
                    <th>Target</th>
                    <th>Username</th>
                    <th>备注</th>
                    <th>原文</th>
                    <th>状态</th>
                </tr>
            </thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>
    </div>
    """
    return page_shell(f"{group_title} - 交易历史", body)


# ================= ROUTES =================
def render_dashboard_page(token: str | None = None):
    stats = get_dashboard_stats()

    users_link = build_url("/users", token=token)
    orders_link = build_url("/orders", token=token)
    groups_link = build_url("/groups", token=token)

    body = f"""
    <div class="topbar">
        <div>
            <h1>📊 管理后台</h1>
            <div class="muted">用户、订单、群组统一管理</div>
        </div>
        <div>
            <a class="btn secondary" href="{users_link}">用户管理</a>
            <a class="btn secondary" href="{orders_link}">订单管理</a>
            <a class="btn secondary" href="{groups_link}">群组账单</a>
        </div>
    </div>

    <div class="stats">
        <div class="card">
            <div class="label">总用户</div>
            <div class="value">{stats["total_users"]}</div>
        </div>
        <div class="card">
            <div class="label">有效用户</div>
            <div class="value">{stats["active_users"]}</div>
        </div>
        <div class="card">
            <div class="label">永久用户</div>
            <div class="value">{stats["permanent_users"]}</div>
        </div>
        <div class="card">
            <div class="label">已过期</div>
            <div class="value">{stats["expired_users"]}</div>
        </div>
        <div class="card">
            <div class="label">待支付订单</div>
            <div class="value">{stats["pending_orders"]}</div>
        </div>
    </div>
    """
    return page_shell("管理后台", body)

def render_users_page(token: str | None = None, keyword: str | None = None, status: str | None = None):
    rows = get_access_users_page(limit=100, offset=0, keyword=keyword, status=status)
    total = count_access_users_filtered(keyword=keyword, status=status)

    users_html = ""
    for user_id, username, granted_by, granted_at, expires_at in rows:
        st_key, st_label = access_status(expires_at)
        tag_class = "ok" if st_key in ("active", "permanent") else "bad"
        detail_link = build_url(f"/user/{user_id}", token=token)

        users_html += f"""
        <tr>
            <td><code>{user_id}</code></td>
            <td>{escape(username or "-")}</td>
            <td>{escape(fmt_ts(granted_at))}</td>
            <td>{escape(fmt_ts(expires_at) if expires_at else "永久")}</td>
            <td><span class="tag {tag_class}">{st_label}</span></td>
            <td><a class="btn" href="{detail_link}">查看 / 修改</a></td>
        </tr>
        """

    if not users_html:
        users_html = '<tr><td colspan="6" class="empty">暂无用户</td></tr>'

    dashboard_link = build_url("/dashboard", token=token)
    body = f"""
    <div class="topbar">
        <div>
            <h1>👤 用户管理</h1>
            <div class="muted">共 {total} 个用户</div>
        </div>
        <div>
            <a class="btn secondary" href="{dashboard_link}">返回后台</a>
        </div>
    </div>

    <form class="filters" method="get" action="/users">
        {'<input type="hidden" name="token" value="' + escape(token) + '">' if token else ''}
        <input type="text" name="keyword" value="{escape(keyword or '')}" placeholder="搜索 user_id / username" style="background:#0b1220;color:#e5e7eb;border:1px solid #374151;padding:10px 12px;border-radius:10px;">
        <select name="status" style="background:#0b1220;color:#e5e7eb;border:1px solid #374151;padding:10px 12px;border-radius:10px;">
            <option value="">全部状态</option>
            <option value="active" {"selected" if status=="active" else ""}>有效</option>
            <option value="expired" {"selected" if status=="expired" else ""}>已过期</option>
            <option value="permanent" {"selected" if status=="permanent" else ""}>永久</option>
        </select>
        <button class="btn" type="submit">查询</button>
    </form>

    <div class="table-wrap">
        <table>
            <thead>
                <tr>
                    <th>User ID</th>
                    <th>Username</th>
                    <th>授权时间</th>
                    <th>到期时间</th>
                    <th>状态</th>
                    <th>操作</th>
                </tr>
            </thead>
            <tbody>{users_html}</tbody>
        </table>
    </div>
    """
    return page_shell("用户管理", body)

def render_user_detail_page(user_id: int, token: str | None = None, message: str | None = None):
    row = get_access_user_by_id(user_id)
    if not row:
        return page_shell("用户不存在", f'<div class="empty">用户 <code>{user_id}</code> 不存在</div>')

    user_id, username, granted_by, granted_at, expires_at = row
    st_key, st_label = access_status(expires_at)
    tag_class = "ok" if st_key in ("active", "permanent") else "bad"

    users_link = build_url("/users", token=token)

    def action_url(path):
        return build_url(path, token=token)

    body = f"""
    <div class="topbar">
        <div>
            <h1>🧾 用户详情</h1>
            <div class="muted">直接在网页续期、设永久、移除权限</div>
        </div>
        <div>
            <a class="btn secondary" href="{users_link}">返回用户列表</a>
        </div>
    </div>

    {f'<div class="card" style="margin-bottom:16px;"><b>{escape(message)}</b></div>' if message else ''}

    <div class="stats">
        <div class="card"><div class="label">User ID</div><div class="value"><code>{user_id}</code></div></div>
        <div class="card"><div class="label">Username</div><div class="value">{escape(username or "-")}</div></div>
        <div class="card"><div class="label">授权时间</div><div class="value">{escape(fmt_ts(granted_at))}</div></div>
        <div class="card"><div class="label">到期时间</div><div class="value">{escape(fmt_ts(expires_at) if expires_at else "永久")}</div></div>
        <div class="card"><div class="label">状态</div><div class="value"><span class="tag {tag_class}">{st_label}</span></div></div>
    </div>

    <div class="card" style="margin-top:16px;">
        <h3>快捷操作</h3>
        <div class="filters">
            <a class="btn" href="{action_url(f'/user/{user_id}/grant/1m')}">续期 1个月</a>
            <a class="btn" href="{action_url(f'/user/{user_id}/grant/3m')}">续期 3个月</a>
            <a class="btn" href="{action_url(f'/user/{user_id}/grant/6m')}">续期 6个月</a>
            <a class="btn" href="{action_url(f'/user/{user_id}/grant/1y')}">续期 1年</a>
            <a class="btn secondary" href="{action_url(f'/user/{user_id}/grant/permanent')}">设为永久</a>
            <a class="btn" style="background:#dc2626;" href="{action_url(f'/user/{user_id}/revoke')}">移除权限</a>
        </div>
    </div>
    """
    return page_shell(f"用户 {user_id}", body)

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard_page(token: str | None = Query(default=None)):
    require_token(token)
    return render_dashboard_page(token=token)


@app.get("/users", response_class=HTMLResponse)
def users_page(
    token: str | None = Query(default=None),
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
):
    require_token(token)
    return render_users_page(token=token, keyword=keyword, status=status)


@app.get("/user/{user_id}", response_class=HTMLResponse)
def user_detail_page(
    user_id: int,
    token: str | None = Query(default=None),
    msg: str | None = Query(default=None),
):
    require_token(token)
    return render_user_detail_page(user_id=user_id, token=token, message=msg)


@app.get("/user/{user_id}/grant/{plan}")
def user_grant_action(
    user_id: int,
    plan: str,
    token: str | None = Query(default=None),
):
    require_token(token)

    if plan == "1m":
        extend_access_user(user_id, 30 * 24 * 3600)
        msg = "已续期 1个月"
    elif plan == "3m":
        extend_access_user(user_id, 90 * 24 * 3600)
        msg = "已续期 3个月"
    elif plan == "6m":
        extend_access_user(user_id, 180 * 24 * 3600)
        msg = "已续期 6个月"
    elif plan == "1y":
        extend_access_user(user_id, 365 * 24 * 3600)
        msg = "已续期 1年"
    elif plan == "permanent":
        set_access_user_permanent(user_id)
        msg = "已设为永久"
    else:
        msg = "未知操作"

    return RedirectResponse(url=build_url(f"/user/{user_id}", token=token, msg=msg), status_code=303)


@app.get("/user/{user_id}/revoke")
def user_revoke_action(
    user_id: int,
    token: str | None = Query(default=None),
):
    require_token(token)
    remove_access_user(user_id)
    return RedirectResponse(url=build_url("/users", token=token), status_code=303)


def render_orders_page(token: str | None = None, status: str | None = None):
    if status in ("pending", "paid", "rejected"):
        rows = get_rental_orders_by_status(status, limit=100)
        title = f"订单管理 - {status}"
    else:
        rows = get_rental_orders_by_status(None, limit=100)
        title = "订单管理"

    dashboard_link = build_url("/dashboard", token=token)
    html_rows = ""

    for row in rows:
        try:
            order_code, user_id, username, full_name, category_title, plan_label, amount, st, created_at, paid_at, expires_at = row
        except Exception:
            continue

        approve_link = build_url(f"/order/{order_code}", token=token) + "/approve"
        reject_link = build_url(f"/order/{order_code}", token=token) + "/reject"

        actions = "-"
        if st == "pending":
            actions = (
                f'<a class="btn" href="{build_url(f"/order/{order_code}/approve", token=token)}">确认付款</a> '
                f'<a class="btn" style="background:#dc2626;" href="{build_url(f"/order/{order_code}/reject", token=token)}">拒绝</a>'
            )

        html_rows += f"""
        <tr>
            <td><code>{escape(str(order_code))}</code></td>
            <td><code>{escape(str(user_id))}</code></td>
            <td>{escape(str(username or "-"))}</td>
            <td>{escape(str(category_title or "-"))}</td>
            <td>{escape(str(plan_label or "-"))}</td>
            <td>{escape(fmt_num(amount))}U</td>
            <td>{escape(str(st or "-"))}</td>
            <td>{escape(fmt_ts(created_at))}</td>
            <td>{actions}</td>
        </tr>
        """

    if not html_rows:
        html_rows = '<tr><td colspan="9" class="empty">暂无订单</td></tr>'

    body = f"""
    <div class="topbar">
        <div>
            <h1>📦 {escape(title)}</h1>
        </div>
        <div>
            <a class="btn secondary" href="{dashboard_link}">返回后台</a>
        </div>
    </div>

    <div class="filters">
        <a class="btn secondary" href="{build_url('/orders', token=token)}">全部</a>
        <a class="btn secondary" href="{build_url('/orders', token=token, status='pending')}">待支付</a>
        <a class="btn secondary" href="{build_url('/orders', token=token, status='paid')}">已支付</a>
        <a class="btn secondary" href="{build_url('/orders', token=token, status='rejected')}">已拒绝</a>
    </div>

    <div class="table-wrap">
        <table>
            <thead>
                <tr>
                    <th>订单号</th>
                    <th>User ID</th>
                    <th>Username</th>
                    <th>类型</th>
                    <th>套餐</th>
                    <th>金额</th>
                    <th>状态</th>
                    <th>创建时间</th>
                    <th>操作</th>
                </tr>
            </thead>
            <tbody>{html_rows}</tbody>
        </table>
    </div>
    """
    return page_shell(title, body)
    
@app.get("/orders", response_class=HTMLResponse)
def orders_page(
    token: str | None = Query(default=None),
    status: str | None = Query(default=None),
):
    require_token(token)
    return render_orders_page(token=token, status=status)

@app.get("/order/{order_code}/approve")
def order_approve_action(order_code: str, token: str | None = Query(default=None)):
    require_token(token)
    approve_rental_order(order_code)
    return RedirectResponse(url=build_url("/orders", token=token, status="pending"), status_code=303)

@app.get("/order/{order_code}/reject")
def order_reject_action(order_code: str, token: str | None = Query(default=None)):
    require_token(token)
    row = get_rental_order(order_code)
    if row:
        _, _, _, _, _, _, _, _, _, status, _, _, _, _ = row
        if status == "pending":
            mark_rental_order_rejected(order_code)
    return RedirectResponse(url=build_url("/orders", token=token, status="pending"), status_code=303)


@app.get("/", include_in_schema=False)
def home(token: str | None = Query(default=None)):
    url = "/dashboard"
    if token:
        url += "?" + urlencode({"token": token})
    return RedirectResponse(url=url)



@app.get("/groups", response_class=HTMLResponse)
def groups_page(token: str | None = Query(default=None)):
    require_token(token)
    return render_groups_page(token=token)


@app.get("/group/{chat_id}", response_class=HTMLResponse)
def group_history(
    chat_id: int,
    date: str | None = Query(default=None),
    token: str | None = Query(default=None),
):
    require_token(token)
    return render_group_history_page(chat_id, date_str=date, token=token)


@app.get("/healthz")
def healthz():
    return {"ok": True}


# ================= RUN =================
if __name__ == "__main__":
    uvicorn.run("web:app", host="0.0.0.0", port=PORT)
