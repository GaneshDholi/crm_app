import frappe
from frappe.utils import today, add_days, getdate, now_datetime

def execute(filters=None):
    columns = get_columns()
    data = get_data(filters)
    return columns, data


def get_columns():
    return [
        {"label": "Activity Tree", "fieldname": "activity", "fieldtype": "Data", "width": 300},
        {"label": "Idle Time", "fieldname": "idle_time", "fieldtype": "Data", "width": 120},
        {"label": "DocType", "fieldname": "doctype", "fieldtype": "Data", "width": 120},
        {"label": "DocType Link", "fieldname": "docname", "fieldtype": "Data", "width": 180},
        {"label": "Action", "fieldname": "action", "fieldtype": "Data", "width": 400},
    ]


def get_data(filters):
    if not filters:
        filters = {}

    data = []

    # ---------------- DATE FILTER ----------------
    date_val = str(filters.get("date") or "today").lower()

    if date_val == "today":
        target_date = getdate(today())
    elif date_val == "yesterday":
        target_date = getdate(add_days(today(), -1))
    else:
        target_date = getdate(filters.get("date"))

    # ---------------- USER FILTER ----------------
    activity_user = filters.get("activity_user")
    full_name = ""

    if activity_user:
        full_name = frappe.db.get_value("User", activity_user, "full_name") or ""

    # ---------------- CALL LOGS ----------------
    call_conditions = "DATE(call_start_time) = %(date)s AND call_start_time IS NOT NULL"
    params = {"date": target_date}

    if full_name:
        call_conditions += " AND from_number = %(user)s"
        params["user"] = full_name

    calls = frappe.db.sql(f"""
        SELECT 
            from_number,
            lead_id,
            call_type,
            call_duration,
            call_start_time
        FROM `tabCall Logs List`
        WHERE {call_conditions}
        ORDER BY call_start_time DESC
    """, params, as_dict=True)

    # ---------------- ACTIVITIES ----------------
    activity_filters = {
        "activity_time": ["!=", None]
    }

    activities = frappe.get_list(
        "Lead Activity",
        fields=["activity_time", "activity_comment", "comment_by", "owner"],
        filters=activity_filters,
        limit=500
    )

    # filter by date + user manually (important)
    filtered_activities = []
    for a in activities:
        if not a.activity_time:
            continue

        if getdate(a.activity_time) != target_date:
            continue

        if full_name:
            user_val = a.comment_by or a.owner or ""
            if user_val != activity_user:
                continue

        filtered_activities.append(a)

    # ---------------- MERGE ----------------
    timeline = []

    for c in calls:
        if not c.call_start_time:
            continue

        timeline.append({
            "time": c.call_start_time,
            "type": "call",
            "user": c.from_number or "Unknown",
            "data": c
        })

    for a in filtered_activities:
        timeline.append({
            "time": a.activity_time,
            "type": "activity",
            "user": a.comment_by or a.owner or "Unknown",
            "data": a
        })

    # ---------------- SAFE SORT ----------------
    timeline = sorted(
        timeline,
        key=lambda x: x.get("time") or now_datetime(),
        reverse=True
    )

    # ---------------- TREE BUILD ----------------
    last_user = None
    last_date = None
    prev_time = None

    for row in timeline:

        if not row.get("time"):
            continue

        user = row.get("user") or "Unknown"
        dt = row["time"]

        date_val = getdate(dt)
        time_str = dt.strftime("%H:%M")

        # USER LEVEL
        if user != last_user:
            data.append({
                "activity": f"▶ {user}",
                "indent": 0
            })
            last_user = user
            last_date = None
            prev_time = None   # reset idle on new user

        # DATE LEVEL
        if date_val != last_date:
            data.append({
                "activity": str(date_val),
                "indent": 1
            })
            last_date = date_val

        # IDLE TIME
        idle = ""
        if prev_time:
            diff = (prev_time - dt).total_seconds() / 60
            if diff >= 0:
                idle = f"{int(diff)} Mins"

        prev_time = dt

        # ACTION + DOC
        if row["type"] == "call":
            d = row["data"]
            action = f"Call Log: {d.call_type or ''}, Duration: {d.call_duration or 0}"
            doc = d.lead_id or ""
            doctype = "Lead"
        else:
            d = row["data"]
            action = f"Activity: {d.activity_comment or ''}"
            doc = ""
            doctype = "Lead"

        # FINAL ROW
        data.append({
            "activity": time_str,
            "idle_time": idle,
            "doctype": doctype,
            "docname": doc,
            "action": action,
            "indent": 2
        })

    return data