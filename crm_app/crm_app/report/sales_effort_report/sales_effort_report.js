frappe.query_reports["Sales Effort Report"] = {
    filters: [
        {
            fieldname: "activity_user",
            label: "User",
            fieldtype: "Link",
            options: "User",
            width: 200
        },
        {
            fieldname: "date",
            label: "Date",
            fieldtype: "Select",
            options: [
                "Today",
                "Yesterday",
                "Last 7 Days"
            ],
            default: "Today",
            width: 150
        }
    ]
};