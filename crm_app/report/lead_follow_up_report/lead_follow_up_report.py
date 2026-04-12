import frappe
from collections import defaultdict

def execute(filters=None):
    filters = filters or {}

    columns = [
        {"label": "Pivot Tree", "fieldname": "pivot", "fieldtype": "Data", "width": 300},
        {"label": "Count", "fieldname": "count", "fieldtype": "Int", "width": 100},
        {"label": "Repeat", "fieldname": "repeat", "fieldtype": "Check", "width": 80},
        {"label": "Activities", "fieldname": "activities", "fieldtype": "Data", "width": 250},
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

    # ---------------- DATA FETCH ---------------- #
    leads = frappe.db.sql(f"""
        SELECT 
            l.name,
            l.lead_owner,
            l.lead_stage,
            DATE(l.creation) as lead_date,
            l.contact_number,
            l.creation,
            DATEDIFF(CURDATE(), l.creation) as open_days,

            -- 🔥 ACTIVITY (last comment)
            (
                SELECT a.activity_comment
                FROM `tabLead Activity` a
                WHERE a.parent = l.name
                ORDER BY a.activity_time DESC
                LIMIT 1
            ) as last_activity,

            -- 🔥 REPEAT (same contact count)
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

    # ---------------- DYNAMIC HIERARCHY ---------------- #
    # Default: 3-level hierarchy (shallower tree, more visible by default)
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

            # Leaf level (show details)
            if level_index == len(levels) - 1:
                for row in group:
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
                        "activities": row.get("last_activity") or "-"
                    })
            else:
                # Add node (only for non-leaf levels)
                data.append({
                    "pivot": key,
                    "indent": indent,
                    "parent": parent,
                    "id": node_id,
                    "count": len(group)
                })
                build_tree(group, level_index + 1, node_id, indent + 1)

    # Build tree
    build_tree(leads)

    return columns, data