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
    """Parse datetime from string safely, return None if fails."""
    if not val:
        return None
    if hasattr(val, 'hour'):
        return val
    for fmt in ("%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(val).strip(), fmt)
        except Exception:
            continue
    return None


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
    activity_user = filters.get("activity_user") or ""
    full_name     = ""
    if activity_user:
        full_name = frappe.db.get_value("User", activity_user, "full_name") or ""

    # ── CALL LOGS ────────────────────────────────────────────────────────────
    call_conditions = """
        DATE(call_start_time) BETWEEN %(from)s AND %(to)s
        AND call_start_time IS NOT NULL
    """
    call_params = {
        "from": str(date_from),
        "to":   str(date_to)
    }

    if activity_user:
        call_conditions += " AND (from_number = %(user)s OR from_number = %(full_name)s)"
        call_params["user"]      = activity_user
        call_params["full_name"] = full_name

    try:
        call_logs = frappe.db.sql(f"""
            SELECT
                from_number,
                lead_id,
                call_type,
                call_duration,
                call_start_time
            FROM `tabCall Logs List`
            WHERE {call_conditions}
            ORDER BY call_start_time DESC
            LIMIT 2000
        """, call_params, as_dict=True)
    except Exception as e:
        frappe.log_error(f"Sales Effort Report – Call Logs fetch failed: {e}")
        call_logs = []

    # ── ACTIVITIES ───────────────────────────────────────────────────────────
    act_filters = {
        "activity_time": ["between", [str(date_from), str(date_to)]]
    }
    if activity_user:
        act_filters["comment_by"] = activity_user

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
    timeline          = []
    call_duration_map = {}  # user → list of {start, duration_secs}

    # CALL LOGS → parse duration and add to timeline
    for c in call_logs:
        if not c.call_start_time:
            continue

        user_key  = c.from_number or "Unknown"
        start_dt  = parse_datetime_safe(c.call_start_time)
        end_dt    = parse_datetime_safe(c.call_duration)   # stored as end datetime

        duration_secs = 0
        duration_str  = "Unknown"

        if start_dt and end_dt and end_dt > start_dt:
            duration_secs = int((end_dt - start_dt).total_seconds())
            mins          = duration_secs // 60
            secs          = duration_secs % 60
            duration_str  = f"{mins}m {secs}s"

        # Duration map for idle calculation
        if user_key not in call_duration_map:
            call_duration_map[user_key] = []

        if start_dt:
            call_duration_map[user_key].append({
                "start":         start_dt,
                "duration_secs": duration_secs
            })

            timeline.append({
                "time":    start_dt,
                "user":    user_key,
                "doctype": "Lead",
                "docname": c.lead_id or "",
                "action":  f"📞 Call: {c.call_type or 'Unknown'} | Duration: {duration_str}",
            })

    # ACTIVITIES → add to timeline
    for a in activities:
        if not a.activity_time:
            continue
        user_val = a.comment_by or a.owner or "Unknown"
        timeline.append({
            "time":    a.activity_time,
            "user":    user_val,
            "doctype": "Lead",
            "docname": a.get("parent") or "",
            "action":  f"Activity: {a.activity_comment or ''}",
        })

    # VERSION LOG → add to timeline
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
            timeline.append({
                "time":    v.creation,
                "user":    v.owner or "Unknown",
                "doctype": "Lead",
                "docname": v.get("docname") or "",
                "action":  f"{field_name}: {old_val} → {new_val}",
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

        dt       = parse_datetime_safe(dt) or dt
        user     = row["user"]
        row_date = getdate(dt)
        time_str = dt.strftime("%H:%M")

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
            prev_time     = None
            prev_row_user = None

        # ── IDLE TIME (call-aware + 1 min buffer) ──
        idle = ""
        if prev_time and prev_row_user == user and getdate(prev_time) == row_date:
            gap_secs = (prev_time - dt).total_seconds()

            # Count calls and their total duration inside this gap
            calls_in_gap     = 0
            call_secs_in_gap = 0
            for c in call_duration_map.get(user, []):
                if c["start"] and dt <= c["start"] <= prev_time:
                    calls_in_gap     += 1
                    call_secs_in_gap += c["duration_secs"]

            # 1 min buffer per call (dialing, picking next lead, etc.)
            buffer_secs = calls_in_gap * 60

            true_idle_secs = gap_secs - call_secs_in_gap - buffer_secs
            true_idle_mins = true_idle_secs / 60

            if true_idle_mins <= 0:
                idle = "0 Mins"
            elif true_idle_mins <= 120:
                idle = f"{int(true_idle_mins)} Mins"

        prev_time     = dt
        prev_row_user = user

        # ── DATA ROW ──
        data.append({
            "activity":  time_str,
            "idle_time": idle,
            "doctype":   row["doctype"],
            "docname":   row["docname"],
            "action":    row["action"],
            "indent":    2,
        })

    return data