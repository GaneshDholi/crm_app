import frappe
from frappe.utils import today, add_days, getdate

def execute(filters=None):
    columns = get_columns()
    data = get_data(filters)
    return columns, data

def get_columns():
    return [
        {
            "label": "Call From",
            "fieldname": "from_number",
            "fieldtype": "Data",
            "width": 150
        },
        {
            "label": "Lead ID",
            "fieldname": "lead_id",
            "fieldtype": "Data",
            "width": 160
        },
        {
            "label": "Call To",
            "fieldname": "to_number",
            "fieldtype": "Data",
            "width": 130
        },
        {
            "label": "Call Type",
            "fieldname": "call_type",
            "fieldtype": "Data",
            "width": 100
        },
        {
            "label": "Call Channel",
            "fieldname": "call_channel",
            "fieldtype": "Data",
            "width": 110
        },
        {
            "label": "Duration",
            "fieldname": "call_duration",
            "fieldtype": "Data",
            "width": 100
        },
        {
            "label": "Call Start Time",
            "fieldname": "call_start_time",
            "fieldtype": "Datetime",
            "width": 160
        },
    ]

def get_data(filters):
    if not filters:
        filters = {}

    # Resolve date
    date_val = filters.get("date") or "Today"
    date_val = str(date_val).strip().lower()

    if date_val == "today":
        resolved_date = getdate(today())
    elif date_val == "yesterday":
        resolved_date = getdate(add_days(today(), -1))
    else:
        resolved_date = getdate(filters.get("date"))

    date_from = str(add_days(resolved_date, -7))
    date_to   = str(resolved_date)

    # Get full name from email
    activity_user = filters.get("activity_user", "")
    full_name = ""
    if activity_user:
        full_name = frappe.db.get_value(
            "User", activity_user, "full_name"
        ) or ""

    # Build conditions
    conditions = """
        DATE(call_start_time) BETWEEN %(date_from)s AND %(date_to)s
    """
    params = {
        "date_from": date_from,
        "date_to": date_to
    }

    if full_name:
        conditions += " AND from_number = %(from_number)s"
        params["from_number"] = full_name

    data = frappe.db.sql(f"""
        SELECT
            from_number,
            lead_id,
            to_number,
            call_type,
            call_channel,
            call_duration,
            call_start_time
        FROM `tabCall Logs List`
        WHERE {conditions}
        ORDER BY call_start_time DESC
    """, params, as_dict=True)

    return data