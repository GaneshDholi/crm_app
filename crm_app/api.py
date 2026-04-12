import frappe
from collections import defaultdict
import json

@frappe.whitelist()
def get_lead_report_data(filters=None):

    filters = frappe.parse_json(filters) if filters else {}

    leads = frappe.get_all(
        "Lead",
        fields=[
            "name",
            "contact_name",
            "contact_number",
            "lead_owner",
            "lead_stage",
            "creation"
        ]
    )

    tree = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    for lead in leads:
        owner = lead.get("lead_owner") or "No Owner"
        date = str(lead.get("creation").date()) if lead.get("creation") else "No Date"
        stage = lead.get("lead_stage") or "No Stage"

        tree[owner][date][stage].append(lead)

    result = []

    for owner, dates in tree.items():

        owner_id = f"owner::{owner}"

        owner_count = sum(len(stages) for d in dates.values() for stages in d.values())

        result.append({
            "pivot": owner,
            "count": owner_count,
            "indent": 0,
            "parent": None,
            "id": owner_id
        })

        for date, stages in dates.items():

            date_id = f"{owner_id}::date::{date}"

            date_count = sum(len(s) for s in stages.values())

            result.append({
                "pivot": date,
                "count": date_count,
                "indent": 1,
                "parent": owner_id,
                "id": date_id
            })

            for stage, leads_list in stages.items():

                stage_id = f"{date_id}::stage::{stage}"

                result.append({
                    "pivot": stage,
                    "count": len(leads_list),
                    "indent": 2,
                    "parent": date_id,
                    "id": stage_id
                })

                for lead in leads_list:
                    result.append({
                        "pivot": lead.get("name"),
                        "indent": 3,
                        "parent": stage_id,
                        "id": f"{stage_id}::{lead.get('name')}",
                        "contact_number": lead.get("contact_number"),
                        "contact_name": lead.get("contact_name")
                    })

    return {
        "columns": [
            {"label": "Pivot Tree", "fieldname": "pivot", "fieldtype": "Data", "width": 300},
            {"label": "Count", "fieldname": "count", "fieldtype": "Int", "width": 100},
            {"label": "Contact No.", "fieldname": "contact_number", "fieldtype": "Data", "width": 150},
            {"label": "Contact Name", "fieldname": "contact_name", "fieldtype": "Data", "width": 150}
        ],
        "data": result
    }


@frappe.whitelist(allow_guest=True)
def get_lead_by_number(phone):

    # Search in Contact Number
    lead = frappe.db.get_value(
        "Lead",
        {"contact_number": phone},
        "name"
    )

    # If not found → search in Alternate Number
    if not lead:
        lead = frappe.db.get_value(
            "Lead",
            {"alternate_contact_number": phone},
            "name"
        )

    if lead:
        return {
            "status": "found",
            "lead_id": lead
        }

    return {
        "status": "not_found"
    }


# -----------------------------
# 2. ADD CALL ACTIVITY
# -----------------------------
@frappe.whitelist(allow_guest=True)
def add_call_activity():
    data = frappe.local.form_dict

    doc = frappe.get_doc({
        "doctype": "Call Activity",
        "lead": data.get("lead"),
        "user": data.get("user"),
        "activity_time": data.get("activity_time"),
        "activity_type": "Call",
        "comment": data.get("comment"),
        "status": data.get("status")
    })

    doc.insert(ignore_permissions=True)

    return {"status": "success"}


# -----------------------------
# 3. CREATE CALL LOG
# -----------------------------
@frappe.whitelist(allow_guest=True)
def create_call_log():
    data = json.loads(frappe.request.data)

    from_number = data.get("from_number")
    to_number = data.get("to_number")

    # Find Lead from numbers
    lead = frappe.db.get_value("Lead", {"mobile_no": from_number}, "name") \
        or frappe.db.get_value("Lead", {"mobile_no": to_number}, "name")

    call = frappe.get_doc({
        "doctype": "Call Log",

        "call_id": data.get("call_id"),
        "user": data.get("user"),

        "customer_number": to_number,
        "contact_name": data.get("contact_name"),
        "lead": lead,

        "call_type": data.get("call_type"),
        "call_status": data.get("call_status"),

        "call_start_time": data.get("call_start_time"),
        "call_end_time": data.get("call_end_time"),
        "call_duration": data.get("call_duration"),

        "call_channel": data.get("call_channel"),
        "device_id": data.get("device_id"),

        "notes": data.get("notes")
    })

    call.insert(ignore_permissions=True)

    return {
        "status": "success",
        "call_log_id": call.name,
        "lead": lead
    }