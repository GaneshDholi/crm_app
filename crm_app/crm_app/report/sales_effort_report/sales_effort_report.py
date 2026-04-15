import frappe
from frappe.utils import today, add_days, getdate

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

    # Date filter
    date_val = str(filters.get("date") or "today").lower()

    if date_val == "today":
        target_date = getdate(today())
    elif date_val == "yesterday":
        target_date = getdate(add_days(today(), -1))
    else:
        target_date = getdate(filters.get("date"))

    # User filter
    activity_user = filters.get("activity_user")

    full_name = ""
    if activity_user:
        full_name = frappe.db.get_value("User", activity_user, "full_name")

    # Get Call Logs
    calls = frappe.db.sql("""
        SELECT 
            from_number,
            lead_id,
            call_type,
            call_duration,
            call_start_time
        FROM `tabCall Logs List`
        WHERE DATE(call_start_time) = %(date)s
        ORDER BY call_start_time DESC
    """, {"date": target_date}, as_dict=True)

    # Get Activities
    activities = frappe.get_list("Lead Activity",
        fields=["activity_time", "activity_comment", "comment_by", "owner"],
        limit=500
    )

    # Merge both
    timeline = []

    for c in calls:
        timeline.append({
            "time": c.call_start_time,
            "type": "call",
            "user": c.from_number,
            "data": c
        })

    for a in activities:
        timeline.append({
            "time": a.activity_time,
            "type": "activity",
            "user": a.comment_by or a.owner,
            "data": a
        })

    # Sort by time
    timeline = sorted(timeline, key=lambda x: x["time"], reverse=True)

    # GROUPING (User → Date → Time)
    last_user = None
    last_date = None
    last_time = None

    prev_time = None

    for row in timeline:

        user = row["user"]
        date = row["time"].date()
        time_str = row["time"].strftime("%H:%M")

        # USER ROW
        if user != last_user:
            data.append({
                "activity": f"▶ {user}",
                "indent": 0
            })
            last_user = user
            last_date = None

        # DATE ROW
        if date != last_date:
            data.append({
                "activity": f"{date}",
                "indent": 1
            })
            last_date = date

        # IDLE TIME
        idle = ""
        if prev_time:
            diff = (prev_time - row["time"]).total_seconds() / 60
            idle = f"{int(diff)} Mins"

        prev_time = row["time"]

        # ACTION TEXT
        if row["type"] == "call":
            d = row["data"]
            action = f"Call Log: {d.call_type}, Duration: {d.call_duration}"
            doc = d.lead_id
            doctype = "Lead"
        else:
            d = row["data"]
            action = f"Activity: {d.activity_comment}"
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
