import os
import time
from datetime import datetime
from contextlib import contextmanager

import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
PGSSLMODE = os.getenv("PGSSLMODE", "").strip()

POOL_MIN = int(os.getenv("PG_POOL_MIN", "1") or 1)
POOL_MAX = int(os.getenv("PG_POOL_MAX", "10") or 10)

_pool: ThreadedConnectionPool | None = None


def _ensure_pool():
    global _pool
    if _pool is not None:
        return
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is missing")

    kwargs = {}
    if PGSSLMODE:
        kwargs["sslmode"] = PGSSLMODE

    _pool = ThreadedConnectionPool(
        minconn=POOL_MIN,
        maxconn=POOL_MAX,
        dsn=DATABASE_URL,
        **kwargs,
    )


def get_conn():
    _ensure_pool()
    assert _pool is not None
    return _pool.getconn()


def put_conn(conn):
    _ensure_pool()
    assert _pool is not None
    _pool.putconn(conn)


@contextmanager
def get_db(commit: bool = False):
    conn = None
    cur = None
    old_autocommit = None
    try:
        conn = get_conn()
        old_autocommit = getattr(conn, "autocommit", False)
        conn.autocommit = not commit

        cur = conn.cursor()
        yield conn, cur

        if commit:
            conn.commit()
    except Exception:
        if conn is not None and commit:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.autocommit = old_autocommit
            except Exception:
                pass
            put_conn(conn)


def init_db():
    with get_db(commit=True) as (_, cur):
        cur.execute("""
        CREATE TABLE IF NOT EXISTS admins (
            user_id BIGINT PRIMARY KEY,
            role TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS groups (
            chat_id BIGINT PRIMARY KEY,
            name TEXT,
            updated_at BIGINT DEFAULT 0
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            chat_id BIGINT,
            key TEXT,
            value TEXT,
            PRIMARY KEY(chat_id, key)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS operators (
            id BIGSERIAL PRIMARY KEY,
            chat_id BIGINT,
            user_id BIGINT,
            username TEXT,
            role TEXT DEFAULT 'operator',
            UNIQUE(chat_id, user_id),
            UNIQUE(chat_id, username)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS members (
            chat_id BIGINT,
            user_id BIGINT,
            username TEXT,
            name TEXT,
            last_seen BIGINT,
            PRIMARY KEY(chat_id, user_id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id BIGSERIAL PRIMARY KEY,
            chat_id BIGINT,
            user_id BIGINT,
            username TEXT,
            display_name TEXT,
            target_name TEXT,
            kind TEXT,
            raw_amount DOUBLE PRECISION,
            unit_amount DOUBLE PRECISION,
            rate_used DOUBLE PRECISION,
            fee_used DOUBLE PRECISION,
            note TEXT,
            original_text TEXT,
            created_at BIGINT,
            undone BOOLEAN DEFAULT FALSE
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS access_users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            granted_by BIGINT,
            granted_at BIGINT,
            expires_at BIGINT,
            reminder_1h_sent BOOLEAN DEFAULT FALSE
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS trial_claims (
            user_id BIGINT PRIMARY KEY,
            username TEXT DEFAULT '',
            claimed_at BIGINT NOT NULL
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS wallet_checks (
            id BIGSERIAL PRIMARY KEY,
            chat_id BIGINT,
            user_id BIGINT,
            username TEXT,
            full_name TEXT,
            address TEXT,
            trx_balance DOUBLE PRECISION,
            usdt_balance DOUBLE PRECISION,
            tx_count INTEGER,
            created_at BIGINT NOT NULL
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS rental_orders (
            id BIGSERIAL PRIMARY KEY,
            order_code TEXT UNIQUE,
            user_id BIGINT,
            username TEXT,
            full_name TEXT,
            category_key TEXT,
            category_title TEXT,
            plan_key TEXT,
            plan_label TEXT,
            amount DOUBLE PRECISION,
            status TEXT DEFAULT 'pending',
            created_at BIGINT,
            paid_at BIGINT,
            expires_at BIGINT,
            note TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS expiry_notices (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT,
            notice_key TEXT,
            created_at BIGINT,
            UNIQUE(user_id, notice_key)
        )
        """)

        # indexes
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_groups_updated_at
        ON groups(updated_at DESC)
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_members_chat_last_seen
        ON members(chat_id, last_seen DESC)
        """)

        # transactions: common report / fetch patterns
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_transactions_chat_created
        ON transactions(chat_id, created_at ASC)
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_transactions_chat_undone_created_id_desc
        ON transactions(chat_id, undone, created_at DESC, id DESC)
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_transactions_chat_undone_created_id_asc
        ON transactions(chat_id, undone, created_at ASC, id ASC)
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_transactions_user_created
        ON transactions(user_id, created_at DESC)
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_access_users_expires_at
        ON access_users(expires_at)
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_wallet_checks_created
        ON wallet_checks(created_at DESC)
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_wallet_checks_chat_user_created
        ON wallet_checks(chat_id, user_id, created_at DESC)
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_rental_orders_status_created
        ON rental_orders(status, created_at DESC)
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_rental_orders_user_created
        ON rental_orders(user_id, created_at DESC)
        """)


# ================= ADMIN =================
def add_admin(user_id, role="admin"):
    with get_db(commit=True) as (_, cur):
        cur.execute("""
        INSERT INTO admins(user_id, role)
        VALUES (%s, %s)
        ON CONFLICT(user_id) DO UPDATE SET role=EXCLUDED.role
        """, (int(user_id), role))


def remove_admin(user_id):
    with get_db(commit=True) as (_, cur):
        cur.execute("DELETE FROM admins WHERE user_id=%s", (int(user_id),))


def get_admin(user_id):
    with get_db() as (_, cur):
        cur.execute("SELECT role FROM admins WHERE user_id=%s", (int(user_id),))
        row = cur.fetchone()
        return row[0] if row else None


def get_all_admins():
    with get_db() as (_, cur):
        cur.execute("SELECT user_id, role FROM admins ORDER BY user_id ASC")
        return cur.fetchall()


# ================= GROUP =================
def save_group(chat_id, name):
    now_ts = int(time.time())
    with get_db(commit=True) as (_, cur):
        cur.execute("""
        INSERT INTO groups(chat_id, name, updated_at)
        VALUES (%s, %s, %s)
        ON CONFLICT(chat_id) DO UPDATE SET
            name=EXCLUDED.name,
            updated_at=EXCLUDED.updated_at
        """, (int(chat_id), name or "", now_ts))


def get_groups():
    with get_db() as (_, cur):
        cur.execute("""
        SELECT chat_id, name
        FROM groups
        ORDER BY updated_at DESC, chat_id ASC
        """)
        return cur.fetchall()


# ================= SETTINGS =================
def set_setting(chat_id, key, value):
    with get_db(commit=True) as (_, cur):
        cur.execute("""
        INSERT INTO settings(chat_id, key, value)
        VALUES (%s, %s, %s)
        ON CONFLICT(chat_id, key) DO UPDATE SET value=EXCLUDED.value
        """, (int(chat_id), str(key), str(value)))


def get_setting(chat_id, key, default=None):
    with get_db() as (_, cur):
        cur.execute("""
        SELECT value
        FROM settings
        WHERE chat_id=%s AND key=%s
        """, (int(chat_id), str(key)))
        row = cur.fetchone()
        return row[0] if row else default


def delete_setting(chat_id, key):
    with get_db(commit=True) as (_, cur):
        cur.execute("DELETE FROM settings WHERE chat_id=%s AND key=%s", (int(chat_id), str(key)))


def set_button_config(chat_id, idx, text, url):
    set_setting(chat_id, f"btn{idx}_text", text)
    set_setting(chat_id, f"btn{idx}_url", url)


def get_button_config(chat_id, idx):
    text = get_setting(chat_id, f"btn{idx}_text", "")
    url = get_setting(chat_id, f"btn{idx}_url", "")
    return text, url


def get_all_button_configs(chat_id):
    data = []
    for i in range(1, 5):
        text, url = get_button_config(chat_id, i)
        if text and url:
            data.append((text, url))
    return data


# ================= OPERATORS =================
def add_operator(chat_id, user_id=None, username=None, role="operator"):
    with get_db(commit=True) as (_, cur):
        if user_id is not None:
            cur.execute("""
            INSERT INTO operators(chat_id, user_id, username, role)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT(chat_id, user_id) DO UPDATE SET
                username=EXCLUDED.username,
                role=EXCLUDED.role
            """, (int(chat_id), int(user_id), username or "", role))

        elif username:
            cur.execute("""
            INSERT INTO operators(chat_id, user_id, username, role)
            VALUES (%s, NULL, %s, %s)
            ON CONFLICT(chat_id, username) DO UPDATE SET
                role=EXCLUDED.role
            """, (int(chat_id), username, role))


def remove_operator(chat_id, user_id=None, username=None):
    with get_db(commit=True) as (_, cur):
        if user_id is not None:
            cur.execute("DELETE FROM operators WHERE chat_id=%s AND user_id=%s", (int(chat_id), int(user_id)))
        elif username is not None:
            cur.execute("DELETE FROM operators WHERE chat_id=%s AND username=%s", (int(chat_id), username))


def clear_operators(chat_id):
    with get_db(commit=True) as (_, cur):
        cur.execute("DELETE FROM operators WHERE chat_id=%s", (int(chat_id),))


def get_operators(chat_id):
    with get_db() as (_, cur):
        cur.execute("""
        SELECT user_id, username, role
        FROM operators
        WHERE chat_id=%s
        ORDER BY id ASC
        """, (int(chat_id),))
        return cur.fetchall()


def get_global_operators():
    with get_db() as (_, cur):
        cur.execute("""
        SELECT user_id, username, role
        FROM operators
        WHERE chat_id=-1
        ORDER BY id ASC
        """)
        return cur.fetchall()


def is_operator(chat_id, user_id=None, username=None):
    with get_db() as (_, cur):
        if user_id is not None:
            cur.execute("""
            SELECT 1
            FROM operators
            WHERE (chat_id=%s OR chat_id=-1)
              AND user_id=%s
            LIMIT 1
            """, (int(chat_id), int(user_id)))
            return cur.fetchone() is not None

        if username:
            cur.execute("""
            SELECT 1
            FROM operators
            WHERE (chat_id=%s OR chat_id=-1)
              AND LOWER(username)=LOWER(%s)
            LIMIT 1
            """, (int(chat_id), username))
            return cur.fetchone() is not None

        return False


# ================= MEMBERS =================
def save_member(chat_id, user_id, username, name):
    now_ts = int(time.time())
    with get_db(commit=True) as (_, cur):
        cur.execute("""
        INSERT INTO members(chat_id, user_id, username, name, last_seen)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT(chat_id, user_id) DO UPDATE SET
            username=EXCLUDED.username,
            name=EXCLUDED.name,
            last_seen=EXCLUDED.last_seen
        """, (int(chat_id), int(user_id), username or "", name or "", now_ts))


def get_members(chat_id):
    with get_db() as (_, cur):
        cur.execute("""
        SELECT chat_id, user_id, username, name, last_seen
        FROM members
        WHERE chat_id=%s
        ORDER BY last_seen DESC, user_id ASC
        """, (int(chat_id),))
        return cur.fetchall()


# ================= TRANSACTIONS =================
def add_transaction(
    chat_id,
    user_id,
    username,
    display_name,
    target_name,
    kind,
    raw_amount,
    unit_amount,
    rate_used,
    fee_used,
    note,
    original_text,
):
    created_at = int(time.time())
    with get_db(commit=True) as (_, cur):
        cur.execute("""
        INSERT INTO transactions(
            chat_id, user_id, username, display_name, target_name, kind,
            raw_amount, unit_amount, rate_used, fee_used, note, original_text,
            created_at, undone
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,FALSE)
        RETURNING id
        """, (
            int(chat_id),
            int(user_id),
            username or "",
            display_name or "",
            target_name or "",
            kind or "",
            raw_amount,
            unit_amount,
            rate_used,
            fee_used,
            note or "",
            original_text or "",
            created_at,
        ))
        return cur.fetchone()[0]


def get_transaction(tx_id):
    with get_db() as (_, cur):
        cur.execute("""
        SELECT id, chat_id, user_id, username, display_name, target_name, kind,
               raw_amount, unit_amount, rate_used, fee_used, note, original_text,
               created_at, undone
        FROM transactions
        WHERE id=%s
        """, (int(tx_id),))
        return cur.fetchone()


def get_last_transaction(chat_id):
    with get_db() as (_, cur):
        cur.execute("""
        SELECT id, chat_id, user_id, username, display_name, target_name, kind,
               raw_amount, unit_amount, rate_used, fee_used, note, original_text,
               created_at, undone
        FROM transactions
        WHERE chat_id=%s AND undone=FALSE
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """, (int(chat_id),))
        return cur.fetchone()


def undo_transaction(tx_id):
    with get_db(commit=True) as (_, cur):
        cur.execute("UPDATE transactions SET undone=TRUE WHERE id=%s", (int(tx_id),))


def clear_transactions(chat_id):
    with get_db(commit=True) as (_, cur):
        cur.execute("DELETE FROM transactions WHERE chat_id=%s", (int(chat_id),))


def get_transactions(chat_id, start_ts=None, end_ts=None, user_id=None, keyword=None, include_undone=False):
    sql = """
    SELECT id, chat_id, user_id, username, display_name, target_name, kind,
           raw_amount, unit_amount, rate_used, fee_used, note, original_text,
           created_at, undone
    FROM transactions
    WHERE chat_id=%s
    """
    params = [int(chat_id)]

    if not include_undone:
        sql += " AND undone=FALSE"

    if start_ts is not None:
        sql += " AND created_at >= %s"
        params.append(int(start_ts))

    if end_ts is not None:
        sql += " AND created_at <= %s"
        params.append(int(end_ts))

    if user_id is not None:
        sql += " AND user_id = %s"
        params.append(int(user_id))

    if keyword:
        sql += """
        AND (
            display_name ILIKE %s OR
            target_name ILIKE %s OR
            username ILIKE %s OR
            note ILIKE %s OR
            original_text ILIKE %s
        )
        """
        kw = f"%{keyword}%"
        params.extend([kw, kw, kw, kw, kw])

    sql += " ORDER BY created_at ASC, id ASC"

    with get_db() as (_, cur):
        cur.execute(sql, tuple(params))
        return cur.fetchall()


# ================= TRIAL / ACCESS =================
def set_trial_code(code):
    set_setting(-1, "trial_code", code or "")


def get_trial_code():
    return get_setting(-1, "trial_code", "")


def has_trial_claimed(user_id):
    with get_db() as (_, cur):
        cur.execute("SELECT 1 FROM trial_claims WHERE user_id=%s", (int(user_id),))
        return cur.fetchone() is not None


def mark_trial_claimed(user_id, username=""):
    with get_db(commit=True) as (_, cur):
        cur.execute("""
        INSERT INTO trial_claims(user_id, username, claimed_at)
        VALUES (%s, %s, %s)
        ON CONFLICT(user_id) DO UPDATE SET
            username=EXCLUDED.username,
            claimed_at=EXCLUDED.claimed_at
        """, (int(user_id), username or "", int(time.time())))


def has_claimed_free_trial(user_id):
    return has_trial_claimed(user_id)


def mark_claimed_free_trial(user_id):
    return mark_trial_claimed(user_id, "")


def add_access_user(user_id, username="", granted_by=None, expires_at=None):
    now_ts = int(time.time())
    with get_db(commit=True) as (_, cur):
        cur.execute("""
        INSERT INTO access_users(user_id, username, granted_by, granted_at, expires_at, reminder_1h_sent)
        VALUES (%s, %s, %s, %s, %s, FALSE)
        ON CONFLICT(user_id) DO UPDATE SET
            username=EXCLUDED.username,
            granted_by=EXCLUDED.granted_by,
            granted_at=EXCLUDED.granted_at,
            expires_at=EXCLUDED.expires_at,
            reminder_1h_sent=FALSE
        """, (int(user_id), username or "", granted_by, now_ts, expires_at))


def remove_access_user(user_id):
    with get_db(commit=True) as (_, cur):
        cur.execute("DELETE FROM access_users WHERE user_id=%s", (int(user_id),))


def has_access_user(user_id):
    with get_db() as (_, cur):
        cur.execute("SELECT expires_at FROM access_users WHERE user_id=%s", (int(user_id),))
        row = cur.fetchone()

    if not row:
        return False

    expires_at = row[0]
    if expires_at is None:
        return True
    return int(time.time()) < int(expires_at)


def get_access_users():
    with get_db() as (_, cur):
        cur.execute("""
        SELECT user_id, username, granted_by, granted_at, expires_at
        FROM access_users
        ORDER BY granted_at DESC
        """)
        return cur.fetchall()


def get_expired_access_users(now_ts=None):
    if now_ts is None:
        now_ts = int(time.time())

    with get_db() as (_, cur):
        cur.execute("""
        SELECT user_id, username, expires_at
        FROM access_users
        WHERE expires_at IS NOT NULL AND expires_at <= %s
        """, (int(now_ts),))
        return cur.fetchall()


def get_access_user_by_id(user_id):
    with get_db() as (_, cur):
        cur.execute("""
        SELECT user_id, username, granted_by, granted_at, expires_at
        FROM access_users
        WHERE user_id = %s
        """, (int(user_id),))
        return cur.fetchone()


# ================= WALLET CHECKS =================
def add_wallet_check(chat_id, user_id, username, full_name, address, trx_balance, usdt_balance, tx_count):
    with get_db(commit=True) as (_, cur):
        cur.execute("""
        INSERT INTO wallet_checks(
            chat_id, user_id, username, full_name, address,
            trx_balance, usdt_balance, tx_count, created_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            int(chat_id),
            int(user_id),
            username or "",
            full_name or "",
            address or "",
            trx_balance,
            usdt_balance,
            tx_count,
            int(time.time()),
        ))


def get_wallet_checks_page(limit=10, offset=0):
    with get_db() as (_, cur):
        cur.execute("""
        SELECT id, chat_id, user_id, username, full_name, address,
               trx_balance, usdt_balance, tx_count, created_at
        FROM wallet_checks
        ORDER BY id DESC
        LIMIT %s OFFSET %s
        """, (int(limit), int(offset)))
        return cur.fetchall()


def count_wallet_checks():
    with get_db() as (_, cur):
        cur.execute("SELECT COUNT(*) FROM wallet_checks")
        row = cur.fetchone()
        return row[0] if row else 0


# ================= RENTAL ORDERS =================
def _make_order_code_from_id(order_id: int) -> str:
    today = datetime.now().strftime("%Y%m%d")
    return f"RB{today}-{int(order_id):06d}"


def create_rental_order(user_id, username, full_name, category_key, category_title, plan_key, plan_label, amount, note=""):
    created_at = int(time.time())
    with get_db(commit=True) as (_, cur):
        cur.execute("""
        INSERT INTO rental_orders (
            order_code, user_id, username, full_name,
            category_key, category_title, plan_key, plan_label,
            amount, status, created_at, paid_at, expires_at, note
        )
        VALUES (NULL, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', %s, NULL, NULL, %s)
        RETURNING id
        """, (
            int(user_id),
            username or "",
            full_name or "",
            category_key or "",
            category_title or "",
            plan_key or "",
            plan_label or "",
            amount,
            created_at,
            note or "",
        ))
        row_id = cur.fetchone()[0]
        order_code = _make_order_code_from_id(row_id)

        cur.execute("""
        UPDATE rental_orders
        SET order_code=%s
        WHERE id=%s
        """, (order_code, row_id))

        return order_code


def get_rental_order(order_code):
    with get_db() as (_, cur):
        cur.execute("""
        SELECT order_code, user_id, username, full_name, category_key, category_title,
               plan_key, plan_label, amount, status, created_at, paid_at, expires_at, note
        FROM rental_orders
        WHERE order_code = %s
        """, (order_code,))
        return cur.fetchone()


def get_pending_rental_orders(limit=50):
    with get_db() as (_, cur):
        cur.execute("""
        SELECT order_code, user_id, username, full_name, category_title, plan_label, amount, created_at
        FROM rental_orders
        WHERE status = 'pending'
        ORDER BY created_at DESC
        LIMIT %s
        """, (int(limit),))
        return cur.fetchall()


def get_rental_orders_by_status(status=None, limit=50):
    with get_db() as (_, cur):
        if status:
            cur.execute("""
            SELECT order_code, user_id, username, full_name, category_title, plan_label,
                   amount, status, created_at, paid_at, expires_at
            FROM rental_orders
            WHERE status = %s
            ORDER BY created_at DESC
            LIMIT %s
            """, (status, int(limit)))
        else:
            cur.execute("""
            SELECT order_code, user_id, username, full_name, category_title, plan_label,
                   amount, status, created_at, paid_at, expires_at
            FROM rental_orders
            ORDER BY created_at DESC
            LIMIT %s
            """, (int(limit),))
        return cur.fetchall()


def mark_rental_order_paid(order_code, expires_at=None):
    with get_db(commit=True) as (_, cur):
        cur.execute("""
        UPDATE rental_orders
        SET status = 'paid', paid_at = %s, expires_at = %s
        WHERE order_code = %s
        """, (int(time.time()), expires_at, order_code))


def mark_rental_order_rejected(order_code):
    with get_db(commit=True) as (_, cur):
        cur.execute("""
        UPDATE rental_orders
        SET status = 'rejected'
        WHERE order_code = %s
        """, (order_code,))


# ================= EXPIRY NOTICES =================
def has_expiry_notice(user_id, notice_key):
    with get_db() as (_, cur):
        cur.execute("""
        SELECT 1
        FROM expiry_notices
        WHERE user_id = %s AND notice_key = %s
        LIMIT 1
        """, (int(user_id), str(notice_key)))
        return cur.fetchone() is not None


def add_expiry_notice(user_id, notice_key):
    with get_db(commit=True) as (_, cur):
        cur.execute("""
        INSERT INTO expiry_notices(user_id, notice_key, created_at)
        VALUES (%s, %s, %s)
        ON CONFLICT(user_id, notice_key) DO NOTHING
        """, (int(user_id), str(notice_key), int(time.time())))

# ================= ACCESS USER ADMIN =================
def count_access_users():
    with get_db() as (_, cur):
        cur.execute("SELECT COUNT(*) FROM access_users")
        row = cur.fetchone()
        return row[0] if row else 0


def count_active_access_users(now_ts=None):
    if now_ts is None:
        now_ts = int(time.time())

    with get_db() as (_, cur):
        cur.execute("""
        SELECT COUNT(*)
        FROM access_users
        WHERE expires_at IS NULL OR expires_at > %s
        """, (int(now_ts),))
        row = cur.fetchone()
        return row[0] if row else 0


def count_expired_access_users(now_ts=None):
    if now_ts is None:
        now_ts = int(time.time())

    with get_db() as (_, cur):
        cur.execute("""
        SELECT COUNT(*)
        FROM access_users
        WHERE expires_at IS NOT NULL AND expires_at <= %s
        """, (int(now_ts),))
        row = cur.fetchone()
        return row[0] if row else 0


def count_permanent_access_users():
    with get_db() as (_, cur):
        cur.execute("""
        SELECT COUNT(*)
        FROM access_users
        WHERE expires_at IS NULL
        """)
        row = cur.fetchone()
        return row[0] if row else 0


def get_access_users_page(limit=20, offset=0, keyword=None, status=None):
    sql = """
    SELECT user_id, username, granted_by, granted_at, expires_at
    FROM access_users
    WHERE 1=1
    """
    params = []

    if keyword:
        sql += " AND (CAST(user_id AS TEXT) ILIKE %s OR username ILIKE %s)"
        kw = f"%{keyword}%"
        params.extend([kw, kw])

    now_ts = int(time.time())
    if status == "active":
        sql += " AND (expires_at IS NULL OR expires_at > %s)"
        params.append(now_ts)
    elif status == "expired":
        sql += " AND expires_at IS NOT NULL AND expires_at <= %s"
        params.append(now_ts)
    elif status == "permanent":
        sql += " AND expires_at IS NULL"

    sql += " ORDER BY granted_at DESC, user_id DESC LIMIT %s OFFSET %s"
    params.extend([int(limit), int(offset)])

    with get_db() as (_, cur):
        cur.execute(sql, tuple(params))
        return cur.fetchall()


def count_access_users_filtered(keyword=None, status=None):
    sql = """
    SELECT COUNT(*)
    FROM access_users
    WHERE 1=1
    """
    params = []

    if keyword:
        sql += " AND (CAST(user_id AS TEXT) ILIKE %s OR username ILIKE %s)"
        kw = f"%{keyword}%"
        params.extend([kw, kw])

    now_ts = int(time.time())
    if status == "active":
        sql += " AND (expires_at IS NULL OR expires_at > %s)"
        params.append(now_ts)
    elif status == "expired":
        sql += " AND expires_at IS NOT NULL AND expires_at <= %s"
        params.append(now_ts)
    elif status == "permanent":
        sql += " AND expires_at IS NULL"

    with get_db() as (_, cur):
        cur.execute(sql, tuple(params))
        row = cur.fetchone()
        return row[0] if row else 0


def extend_access_user(user_id, seconds, username="", granted_by=None):
    now_ts = int(time.time())

    with get_db() as (_, cur):
        cur.execute("""
        SELECT user_id, username, granted_by, granted_at, expires_at
        FROM access_users
        WHERE user_id=%s
        """, (int(user_id),))
        row = cur.fetchone()

    if row:
        _, old_username, old_granted_by, old_granted_at, expires_at = row
        base_ts = now_ts
        if expires_at and int(expires_at) > now_ts:
            base_ts = int(expires_at)

        new_exp = base_ts + int(seconds)
        add_access_user(
            user_id=user_id,
            username=username or old_username or "",
            granted_by=granted_by if granted_by is not None else old_granted_by,
            expires_at=new_exp,
        )
        return new_exp

    new_exp = now_ts + int(seconds)
    add_access_user(
        user_id=user_id,
        username=username or "",
        granted_by=granted_by,
        expires_at=new_exp,
    )
    return new_exp


def set_access_user_permanent(user_id, username="", granted_by=None):
    with get_db() as (_, cur):
        cur.execute("""
        SELECT username, granted_by
        FROM access_users
        WHERE user_id=%s
        """, (int(user_id),))
        row = cur.fetchone()

    old_username = row[0] if row else ""
    old_granted_by = row[1] if row else None

    add_access_user(
        user_id=user_id,
        username=username or old_username or "",
        granted_by=granted_by if granted_by is not None else old_granted_by,
        expires_at=None,
    )


def get_dashboard_stats():
    return {
        "total_users": count_access_users(),
        "active_users": count_active_access_users(),
        "expired_users": count_expired_access_users(),
        "permanent_users": count_permanent_access_users(),
        "pending_orders": len(get_pending_rental_orders(limit=1000)),
    }
def plan_duration_seconds(plan_key):
    if plan_key == "1m":
        return 30 * 24 * 60 * 60
    if plan_key == "3m":
        return 90 * 24 * 60 * 60
    if plan_key == "6m":
        return 180 * 24 * 60 * 60
    if plan_key == "1y":
        return 365 * 24 * 60 * 60
    return 30 * 24 * 60 * 60


def calc_renew_expire_at(user_id, plan_key):
    now_ts = int(time.time())
    duration = plan_duration_seconds(plan_key)

    access_row = get_access_user_by_id(user_id)
    current_exp = None
    if access_row and len(access_row) >= 5:
        current_exp = access_row[4]

    base_ts = now_ts
    if current_exp and int(current_exp) > now_ts:
        base_ts = int(current_exp)

    return base_ts + duration


def approve_rental_order(order_code, granted_by=None):
    row = get_rental_order(order_code)
    if not row:
        return None, None, "订单不存在"

    (
        order_code, user_id, username, full_name, category_key, category_title,
        plan_key, plan_label, amount, status, created_at, paid_at, expires_at, note
    ) = row

    if status == "paid":
        return row, expires_at, "订单已支付"

    new_expires_at = calc_renew_expire_at(user_id, plan_key)

    mark_rental_order_paid(order_code, expires_at=new_expires_at)
    add_access_user(
        user_id=user_id,
        username=username or "",
        granted_by=granted_by,
        expires_at=new_expires_at,
    )

    return row, new_expires_at, None
