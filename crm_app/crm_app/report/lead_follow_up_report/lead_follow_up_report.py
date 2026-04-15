import frappe
from collections import defaultdict


def execute(filters=None):
    filters = filters or {}

    columns = [
        {"label": "Pivot Tree", "fieldname": "pivot", "fieldtype": "Data", "width": 300},
        {"label": "Count", "fieldname": "count", "fieldtype": "Int", "width": 100},
        {"label": "Repeat", "fieldname": "repeat", "fieldtype": "Check", "width": 80},
        {"label": "Activities", "fieldname": "activities", "fieldtype": "Small Text", "width": 300},
        {"label": "Contact No.", "fieldname": "contact_number", "fieldtype": "Data", "width": 150},
        {"label": "Created", "fieldname": "creation", "fieldtype": "Datetime", "width": 150},
        {"label": "Open Days", "fieldname": "open_days", "fieldtype": "Int", "width": 100},
        {"label": "LCD", "fieldname": "lcd", "fieldtype": "Data", "width": 120},
    ]

    # ---------------- FILTERS ---------------- #
    conditions = []
    values = {}

    if filters.get("owner"):
        conditions.append("l.lead_owner = %(owner)s")
        values["owner"] = filters.get("owner")

    if filters.get("status"):
        conditions.append("l.lead_stage = %(status)s")
        values["status"] = filters.get("status")

    if filters.get("contact_number"):
        conditions.append("l.contact_number = %(contact_number)s")
        values["contact_number"] = filters.get("contact_number")

    if filters.get("date_range") == "Last 7 Days":
        conditions.append("l.creation >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)")

    where_clause = " AND ".join(conditions)
    if where_clause:
        where_clause = "WHERE " + where_clause

    # ---------------- FETCH LEADS ---------------- #
    leads = frappe.db.sql(f"""
        SELECT 
            l.name,
            l.lead_owner,
            l.lead_stage,
            DATE(l.creation) as lead_date,
            l.contact_number,
            l.creation,
            DATEDIFF(CURDATE(), l.creation) as open_days,

            (
                SELECT COUNT(*) 
                FROM `tabLead` l2 
                WHERE l2.contact_number = l.contact_number
                AND l2.name != l.name
                AND IFNULL(l.contact_number, '') != ''
            ) as repeat_count

        FROM `tabLead` l
        {where_clause}
        ORDER BY l.creation DESC
    """, values, as_dict=True)

    # ---------------- GET ALL ACTIVITIES + CALLS ---------------- #
    def get_activities_map(leads):
        activity_map = defaultdict(list)

        lead_names = [l.name for l in leads]
        if not lead_names:
            return activity_map

        # Lead Activities
        activities = frappe.get_all(
            "Lead Activity",
            filters={"parent": ["in", lead_names]},
            fields=["parent", "activity_comment", "activity_time"],
            order_by="activity_time desc"
        )

        # Call Logs
        calls = frappe.db.sql("""
            SELECT lead_id, call_type, call_duration, call_start_time
            FROM `tabCall Logs List`
            WHERE lead_id IN %(leads)s
        """, {"leads": lead_names}, as_dict=True)

        # Merge Activities
        for a in activities:
            if not a.activity_time:
                continue
            txt = f"[{a.activity_time.strftime('%d-%b-%Y %I:%M %p')}] {a.activity_comment or ''}"
            activity_map[a.parent].append(txt)

        # Merge Calls
        for c in calls:
            if not c.call_start_time:
                continue
            txt = f"[{c.call_start_time.strftime('%d-%b-%Y %I:%M %p')}] Call {c.call_type}, {c.call_duration or 0}s"
            activity_map[c.lead_id].append(txt)

        return activity_map

    activity_map = get_activities_map(leads)

    # ---------------- TREE BUILD ---------------- #
    hierarchy = filters.get("lead_details") or "Lead Owner/Date/Lead Name"
    levels = hierarchy.split("/")

    def get_value(level, row):
        if "Owner" in level:
            return row.get("lead_owner") or "No Owner"
        elif "Date" in level:
            return str(row.get("lead_date"))
        elif "Stage" in level:
            return row.get("lead_stage") or "No Stage"
        elif "Lead Name" in level:
            return row.get("name")
        return "N/A"

    data = []

    def build_tree(rows, level_index=0, parent=None, indent=0):
        if level_index >= len(levels):
            return

        grouped = defaultdict(list)

        for row in rows:
            key = get_value(levels[level_index], row)
            grouped[key].append(row)

        for key, group in grouped.items():
            node_id = f"{parent or 'root'}::{key}"

            if level_index == len(levels) - 1:
                for row in group:
                    activities_text = "\n".join(activity_map.get(row.get("name"), [])) or "-"

                    data.append({
                        "pivot": row.get("name"),
                        "indent": indent,
                        "parent": parent,
                        "id": f"{parent}::{row.get('name')}",
                        "contact_number": row.get("contact_number"),
                        "creation": row.get("creation"),
                        "open_days": row.get("open_days"),
                        "lcd": "N/A",
                        "repeat": 1 if row.get("repeat_count", 0) > 0 else None,
                        "activities": activities_text
                    })
            else:
                data.append({
                    "pivot": key,
                    "indent": indent,
                    "parent": parent,
                    "id": node_id,
                    "count": len(group)
                })
                build_tree(group, level_index + 1, node_id, indent + 1)

    build_tree(leads)

    return columns, data