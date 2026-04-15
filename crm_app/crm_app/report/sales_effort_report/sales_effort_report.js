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
        options: ["Today", "Yesterday", "Last 7 Days"],
        default: "Today"
    }
]