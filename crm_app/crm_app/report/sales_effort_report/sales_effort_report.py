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
        date_from = getdate(today())
        date_to = date_from

    elif date_val == "yesterday":
        date_from = getdate(add_days(today(), -1))
        date_to = date_from

    elif "7" in date_val:  # last 7 days
        date_to = getdate(today())
        date_from = getdate(add_days(today(), -6))

    else:
        date_from = getdate(filters.get("date"))
        date_to = date_from

    # ---------------- USER FILTER ----------------
    activity_user = filters.get("activity_user")
    full_name = ""

    if activity_user:
        full_name = frappe.db.get_value("User", activity_user, "full_name") or ""

    # ---------------- CALL LOGS ----------------
    call_conditions = """
        DATE(call_start_time) BETWEEN %(from)s AND %(to)s
        AND call_start_time IS NOT NULL
    """

    params = {
        "from": date_from,
        "to": date_to
    }

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
    """, params, as_dict=True)

    # ---------------- ACTIVITIES ----------------
    activities = frappe.get_list(
        "Lead Activity",
        fields=["activity_time", "activity_comment", "comment_by", "owner"],
        filters={"activity_time": ["!=", None]},
        limit=1000
    )

    # ---------------- LEAD CHANGES (IMPORTANT) ----------------
    # this brings "Next Contact Date" logs
    lead_changes = frappe.get_all(
        "Version",
        filters={"ref_doctype": "Lead"},
        fields=["creation", "data", "owner"],
        limit=1000
    )

    # ---------------- BUILD TIMELINE ----------------
    timeline = []

    # CALLS
    # for c in calls:
    #     if not c.call_start_time:
    #         continue

    #     timeline.append({
    #         "time": c.call_start_time,
    #         "user": c.from_number or "Unknown",
    #         "doctype": "Lead",
    #         "docname": c.lead_id,
    #         "action": f"Call Log: {c.call_type}, Duration: {c.call_duration}"
    #     })

    # ACTIVITIES
    for a in activities:
        if not a.activity_time:
            continue

        if not (date_from <= getdate(a.activity_time) <= date_to):
            continue

        user_val = a.comment_by or a.owner or "Unknown"

        if full_name and user_val not in [activity_user, full_name]:
            continue

        timeline.append({
            "time": a.activity_time,
            "user": user_val,
            "doctype": "Lead",
            "docname": "",
            "action": f"Activity: {a.activity_comment}"
        })

    # NEXT CONTACT DATE CHANGES 🔥
    import json

    for v in lead_changes:
        if not v.data:
            continue

        try:
            d = json.loads(v.data)
        except:
            continue

        if not d.get("changed"):
            continue

        for change in d.get("changed", []):
            if "contact" in change[0].lower():
                timeline.append({
                    "time": v.creation,
                    "user": v.owner or "Unknown",
                    "doctype": "Lead",
                    "docname": "",
                    "action": f"Next Contact Date: {change[1]} → {change[2]}"
                })

    # ---------------- SORT ----------------
    timeline = sorted(
        timeline,
        key=lambda x: x["time"] or frappe.utils.now_datetime(),
        reverse=True
    )

    # ---------------- TREE ----------------
    last_user = None
    last_date = None
    prev_time = None

    for row in timeline:

        dt = row["time"]
        if not dt:
            continue

        user = row["user"]
        date_val = getdate(dt)
        time_str = dt.strftime("%H:%M")

        # USER
        if user != last_user:
            data.append({"activity": f"▶ {user}", "indent": 0})
            last_user = user
            last_date = None
            prev_time = None

        # DATE
        if date_val != last_date:
            data.append({"activity": str(date_val), "indent": 1})
            last_date = date_val
            prev_time = None   # FIX

        # IDLE TIME (FIXED)
        idle = ""
        if prev_time:
            # only calculate within same user & same date
            if user == last_user and date_val == last_date:
                diff = (prev_time - dt).total_seconds() / 60

                # avoid huge wrong values
                if 0 <= diff <= 120:
                    idle = f"{int(diff)} Mins"

        prev_time = dt

        # FINAL ROW
        data.append({
            "activity": time_str,
            "idle_time": idle,
            "doctype": row["doctype"],
            "docname": row["docname"],
            "action": row["action"],
            "indent": 2
        })

    return data
