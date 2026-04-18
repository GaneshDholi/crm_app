import frappe
import json
from frappe.utils import today, add_days, getdate, now_datetime
from datetime import datetime


def execute(filters=None):
    columns = get_columns()
    data = get_data(filters)
    return columns, data


def get_columns():
    return [
        {"label": "Activity Tree", "fieldname": "activity",  "fieldtype": "Data", "width": 300},
        {"label": "Idle Time",     "fieldname": "idle_time", "fieldtype": "Data", "width": 120},
        {"label": "DocType",       "fieldname": "doctype",   "fieldtype": "Data", "width": 120},
        {"label": "DocType Link",  "fieldname": "docname",   "fieldtype": "Data", "width": 180},
        {"label": "Action",        "fieldname": "action",    "fieldtype": "Data", "width": 400},
    ]


def parse_datetime_safe(val):
    if not val:
        return None
    if hasattr(val, 'hour'):
        return val
    s = str(val).strip()[:19]
    for fmt in ("%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None


def parse_duration_to_secs(raw):
    """Parse call_duration like '5s', '1m30s', '90', '1:30' → seconds."""
    if not raw:
        return 0
    s = str(raw).strip()
    # "5s" or "90s"
    if s.endswith("s") and "m" not in s:
        try:
            return int(s[:-1])
        except:
            return 0
    # "1m30s" or "1m"
    if "m" in s:
        try:
            parts = s.replace("s", "").split("m")
            mins = int(parts[0]) if parts[0] else 0
            secs = int(parts[1]) if len(parts) > 1 and parts[1] else 0
            return mins * 60 + secs
        except:
            return 0
    # "1:30" or "0:05"
    if ":" in s:
        parts = s.split(":")
        try:
            if len(parts) == 2:
                return int(parts[0]) * 60 + int(parts[1])
            elif len(parts) == 3:
                return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        except:
            return 0
    # plain number
    try:
        return int(float(s))
    except:
        return 0


def get_data(filters):
    if not filters:
        filters = {}

    data = []

    # ── DATE FILTER ──────────────────────────────────────────────────────────
    raw_date = str(filters.get("date") or "Today").strip().lower()

    if raw_date == "today":
        date_from = getdate(today())
        date_to   = date_from
    elif raw_date == "yesterday":
        date_from = getdate(add_days(today(), -1))
        date_to   = date_from
    elif "7" in raw_date:
        date_to   = getdate(today())
        date_from = getdate(add_days(today(), -6))
    else:
        date_from = getdate(today())
        date_to   = date_from

    # ── USER FILTER ──────────────────────────────────────────────────────────
    activity_user        = filters.get("activity_user") or ""
    full_name            = ""
    if activity_user:
        full_name = frappe.db.get_value("User", activity_user, "full_name") or ""

    # ── CALL LOGS ────────────────────────────────────────────────────────────
    # call_start_time stored as DD-MM-YYYY HH:MM:SS string
    call_conditions = """
        STR_TO_DATE(call_start_time, '%d-%m-%Y %H:%i:%s')
            BETWEEN %(from)s AND %(to)s
        AND call_start_time IS NOT NULL
        AND call_start_time != ''
    """
    call_params = {
        "from": str(date_from) + " 00:00:00",
        "to":   str(date_to)   + " 23:59:59"
    }

    if activity_user and full_name:
        # call_from stores Full Name ("Varsha Goyal")
        call_conditions += " AND call_from = %(call_from_name)s"
        call_params["call_from_name"] = full_name

    try:
        call_logs = frappe.db.sql(f"""
            SELECT
                call_from,
                lead_id,
                call_type,
                call_duration,
                call_start_time
            FROM `tabCall Logs List`
            WHERE {call_conditions}
            ORDER BY STR_TO_DATE(call_start_time, '%d-%m-%Y %H:%i:%s') DESC
            LIMIT 2000
        """, call_params, as_dict=True)
    except Exception as e:
        frappe.log_error(f"Sales Effort Report – Call Logs fetch failed: {e}")
        call_logs = []

    # ── BUILD CALL DURATION MAP ───────────────────────────────────────────────
    # keyed by full_name since call_from stores full name
    call_duration_map = {}

    for c in call_logs:
        if not c.call_start_time:
            continue

        user_key      = c.call_from or "Unknown"
        start_dt      = parse_datetime_safe(c.call_start_time)
        duration_secs = parse_duration_to_secs(c.call_duration)

        if user_key not in call_duration_map:
            call_duration_map[user_key] = []

        if start_dt:
            call_duration_map[user_key].append({
                "start":         start_dt,
                "duration_secs": duration_secs
            })

    # ── ACTIVITIES ───────────────────────────────────────────────────────────
    act_filters = {
        "parenttype":    "Lead",
        "activity_time": ["between", [str(date_from), str(date_to)]]
    }
    if activity_user:
        act_filters["comment_by"] = activity_user   # stores email

    try:
        activities = frappe.get_list(
            "Lead Activity",
            fields=["activity_time", "activity_comment", "comment_by", "owner", "parent"],
            filters=act_filters,
            limit=2000,
            order_by="activity_time desc"
        )
    except Exception as e:
        frappe.log_error(f"Sales Effort Report – Lead Activity fetch failed: {e}")
        activities = []

    # ── VERSION LOG ──────────────────────────────────────────────────────────
    ver_filters = {
        "ref_doctype": "Lead",
        "creation": ["between", [
            str(date_from) + " 00:00:00",
            str(date_to)   + " 23:59:59"
        ]]
    }
    if activity_user:
        ver_filters["owner"] = activity_user

    try:
        lead_changes = frappe.get_all(
            "Version",
            filters=ver_filters,
            fields=["creation", "data", "owner", "docname"],
            limit=2000
        )
    except Exception as e:
        frappe.log_error(f"Sales Effort Report – Version fetch failed: {e}")
        lead_changes = []

    # ── BUILD TIMELINE ────────────────────────────────────────────────────────
    timeline       = []
    user_name_cache = {}

    def get_display_name(email):
        if not email:
            return "Unknown"
        if email not in user_name_cache:
            user_name_cache[email] = frappe.db.get_value("User", email, "full_name") or email
        return user_name_cache[email]

    for a in activities:
        if not a.activity_time:
            continue
        email    = a.comment_by or a.owner or ""
        timeline.append({
            "time":       a.activity_time,
            "user":       get_display_name(email),
            "user_key":   get_display_name(email),   # full name → matches call_duration_map key
            "doctype":    "Lead",
            "docname":    a.get("parent") or "",
            "action":     a.activity_comment or "",
        })

    for v in lead_changes:
        if not v.data:
            continue
        try:
            d = json.loads(v.data)
        except Exception:
            continue
        for change in d.get("changed", []):
            field_name = change[0] or ""
            old_val    = change[1]
            new_val    = change[2]
            email      = v.owner or ""
            timeline.append({
                "time":     v.creation,
                "user":     get_display_name(email),
                "user_key": get_display_name(email),
                "doctype":  "Lead",
                "docname":  v.get("docname") or "",
                "action":   f"{field_name}: {old_val} → {new_val}",
            })

    # ── SORT newest → oldest ──────────────────────────────────────────────────
    timeline.sort(key=lambda x: x["time"] or now_datetime(), reverse=True)

    # ── TREE OUTPUT ───────────────────────────────────────────────────────────
    last_user     = None
    last_row_date = None
    prev_time     = None
    prev_row_user = None

    for row in timeline:
        dt = row["time"]
        if not dt:
            continue

        dt        = parse_datetime_safe(dt) or dt
        user      = row["user"]       # full name for display
        user_key  = row["user_key"]   # full name for call map lookup
        row_date  = getdate(dt)
        time_str  = dt.strftime("%H:%M")

        # ── USER heading ──
        if user != last_user:
            data.append({
                "activity":  f"▶ {user}",
                "indent":    0,
                "idle_time": "", "doctype": "", "docname": "", "action": ""
            })
            last_user     = user
            last_row_date = None
            prev_time     = None
            prev_row_user = None

        # ── DATE heading ──
        if row_date != last_row_date:
            data.append({
                "activity":  row_date.strftime("%d-%b-%y"),
                "indent":    1,
                "idle_time": "", "doctype": "", "docname": "", "action": ""
            })
            last_row_date = row_date
            prev_time     = None   # reset — prevents cross-date idle bug
            prev_row_user = None

        # ── IDLE TIME ──
        idle = ""
        if prev_time and prev_row_user == user_key and getdate(prev_time) == row_date:
            gap_secs         = (prev_time - dt).total_seconds()
            calls_in_gap     = 0
            call_secs_in_gap = 0

            for c in call_duration_map.get(user_key, []):
                if c["start"] and dt <= c["start"] <= prev_time:
                    calls_in_gap     += 1
                    call_secs_in_gap += c["duration_secs"]

            buffer_secs    = calls_in_gap * 60
            true_idle_secs = gap_secs - call_secs_in_gap - buffer_secs
            true_idle_mins = true_idle_secs / 60

            if true_idle_mins <= 0:
                idle = "0 Mins"
            elif true_idle_mins <= 120:
                idle = f"{int(true_idle_mins)} Mins"

        prev_time     = dt
        prev_row_user = user_key

        data.append({
            "activity":  time_str,
            "idle_time": idle,
            "doctype":   row["doctype"],
            "docname":   row["docname"],
            "action":    row["action"],
            "indent":    2,
        })

    return data