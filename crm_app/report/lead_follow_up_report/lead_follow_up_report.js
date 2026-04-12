frappe.query_reports["Lead Follow Up Report"] = {
    tree: true,
    name_field: "pivot",
    parent_field: "parent",
    initial_depth: 2,

    // 🔥 BUTTONS ON LOAD
    onload: function(report) {

        // ✅ Refresh / Rebuild Button
        report.page.add_inner_button("🔄 Rebuild", function() {
            frappe.show_alert({message: "Rebuilding report...", indicator: "blue"});
            report.refresh();
        });
    },

    // 🎨 FORMAT UI
    formatter: function(value, row, column, data, default_formatter) {
        value = default_formatter(value, row, column, data);

        if (!data) return value;

        if (data.indent === 0) {
            value = `<span style="font-weight:bold; font-size:14px;">${value}</span>`;
        }

        if (data.indent === 1) {
            value = `<span style="font-weight:600;">${value}</span>`;
        }

        if (data.indent === 2) {
            value = `<span style="color:#555;">${value}</span>`;
        }

        if (data.indent === 3) {
            value = `<span style="color:#888;">${value}</span>`;
        }

        if (column.fieldname === "repeat") {
            if (data.indent < 3) {
                return "";   // remove checkbox
            }
        }

        return value;
    },

    // 📊 TABLE OPTIONS
    get_datatable_options(options) {
        return Object.assign(options, {
            checkboxColumn: true,
            dynamicRowHeight: true
        });
    }
};