import frappe
from collections import defaultdict
from frappe.utils import now,today
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

def intercept_magic_date():
    # 1. Safety check: Ensure we have a form dictionary in the request
    if not hasattr(frappe.local, 'form_dict') or not frappe.local.form_dict:
        return

    # 2. Scope check: We only want to run this for CRM Leads to save server resources
    req_path = frappe.request.path.lower() if frappe.request else ""
    if "lead" not in req_path and frappe.local.form_dict.get('doctype') != 'CRM Lead':
        return

    # 3. Intercept direct URL parameters: ?next_contact_date=1111-11-11
    if frappe.local.form_dict.get("next_contact_date") == "1111-11-11":
        frappe.local.form_dict["next_contact_date"] = today()

    # 4. Intercept standard Frappe API JSON filters (used by the Vue frontend)
    filters = frappe.local.form_dict.get("filters")
    if filters:
        try:
            if isinstance(filters, str):
                parsed = json.loads(filters)
                modified = False
                
                # Handle Array Format: [["CRM Lead", "next_contact_date", "=", "1111-11-11"]]
                if isinstance(parsed, list):
                    for f in parsed:
                        if len(f) >= 4 and f[1] == "next_contact_date" and f[3] == "1111-11-11":
                            f[3] = today()
                            modified = True
                
                # Handle Dictionary Format: {"next_contact_date": "1111-11-11"}
                elif isinstance(parsed, dict):
                    if parsed.get("next_contact_date") == "1111-11-11":
                        parsed["next_contact_date"] = today()
                        modified = True
                        
                # If we swapped the date, repackage the JSON and inject it back into the request
                if modified:
                    frappe.local.form_dict["filters"] = json.dumps(parsed)
        except Exception:
            # If the filter isn't valid JSON, fail silently and let Frappe handle it normally
            pass

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


@frappe.whitelist(allow_guest=True)
def add_call_activity():
    data = frappe.local.form_dict

    # 1. Get the 2 fields from the POST request
    mobile_number = data.get("mobile_number")
    comment = data.get("comment")

    # Safety check
    if not mobile_number or not comment:
        return {"status": "failed", "message": "Mobile number and comment are required."}

    # 2. Find the Lead ID using the mobile number
    lead_id = frappe.db.get_value("Lead", {"contact_number": mobile_number}, "name")
    
    if not lead_id:
        lead_id = frappe.db.get_value("Lead", {"alternate_contact_number": mobile_number}, "name")

    if not lead_id:
        return {
            "status": "not_found", 
            "message": f"No Lead found in the system for number: {mobile_number}"
        }

    # 3. Load the existing Lead document
    lead_doc = frappe.get_doc("Lead", lead_id)

    # 4. Append a new row to the "activity_summary" child table
    # We use the exact field names from your Client Script!
    lead_doc.append("activity_summary", {
        "activity_comment": comment,
        "comment_by": "Administrator",
        "activity_time": now()
    })

    # 5. Save the Lead document with the new row
    lead_doc.save(ignore_permissions=True)
    
    # Commit the database changes (Crucial when updating existing docs via external API)
    frappe.db.commit()

    return {
        "status": "success",
        "message": "Activity added to Lead successfully",
        "lead_id": lead_id
    }



@frappe.whitelist(allow_guest=True)
def create_call_log():
    try:
        data = json.loads(frappe.request.data)
    except Exception:
        data = frappe.local.form_dict

    from_val = data.get("from_number")
    to_val = data.get("to_number")

    # --- Helper to find the "Display Name" for the field ---
    def get_display_name(number):
        if not number: return "Unknown"
        
        # 1. Search Leads
        lead_name = frappe.db.get_value("Lead", {"contact_number": number}, "name") or \
                    frappe.db.get_value("Lead", {"alternate_contact_number": number}, "name")
        if lead_name: return lead_name
        
        # 2. Search Sales Person (Ensure your Sales Person DocType has a 'mobile_no' or similar field)
        sales_name = frappe.db.get_value("Sales Person", {"user": number}, "sales_name") or \
                     frappe.db.get_value("Sales Person", {"mobile_no": number}, "sales_name")
        if sales_name: return sales_name
        
        return number # Fallback to number if no name is found

    # --- Fetch Names ---
    display_from = get_display_name(from_val)
    display_to = get_display_name(to_val)

    # --- Create the Entry with Names in the Number Slots ---
    doc = frappe.get_doc({
        "doctype": "Call Logs List",
        "from_number": display_from,   # Saves "Ganesh" or "Varsha Goyal"
        "to_number": display_to,       # Saves "Ram" or "Ganesh"
        "call_type": data.get("call_type"),
        "call_channel": data.get("call_channel"),
        "call_start_time": data.get("call_start_time") or now(),
        "call_duration": data.get("call_duration"),
        "user": "Administrator"
    })

    doc.insert(ignore_permissions=True)
    frappe.db.commit()

    return {
        "status": "success",
        "entry": doc.name,
        "saved_as": f"{display_from} to {display_to}"
    }