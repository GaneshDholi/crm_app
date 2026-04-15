frappe.query_reports["Sales Effort Report"] = {
    filters: [
        {
            fieldname: "activity_user",
            label: "User",
            fieldtype: "Link",
            options: "User"
        },
        {
            fieldname: "date",
            label: "Date",
            fieldtype: "Select",
            options: ["Today", "Yesterday"],
            default: "Today"
        }
    ]
};