# DAG Documentation

Owned by: **Team Bravo**

**🔴 Critical Priority**: Business-critical; failure impacts key operations and requires immediate attention.

---

## ⏱️ SLA
This DAG is expected to complete within **2 hours** of its scheduled start time.  
Please review logs and notify stakeholders if the SLA is breached.

## 📋 Stakeholders
- **Finance Team**: example.person@company.com
- **Dev Z Team**: #example Slack Channel

## 🌐 Downstream Applications
- **rETL Application**: Segment (loads data to downstream systems)
  - Link
- **Dashboards**: Looker (supports the `Sales Performance` dashboard)
  - Link

## 🔧 Troubleshooting Steps
1. **Late Arriving Files**:
   - Sometimes, the vendor doesn't deliver data on time
   - In this case, a failure is acceptable and any data missed from that day's daily run will be picked up on the subsequent run

## 🔗 Additional Notes
- This DAG is planned to be deprecated Q1 2025 after the release of XYZ product
- If the DAG suddenly starts taking 2-4x as long to complete, do XYZ

## 🛠️ Developer Contacts
- **Primary Developers**: Jacob Yablonski (jacob.yablonski@example.com) and Example Developer (example.developer@example.com)

---

#### Criticality Levels
1. **🟢 Low Priority**: Non-essential; can tolerate failure for several days.
2. **🟡 Medium Priority**: Important but can tolerate short-term failure (1–2 days).
3. **🟠 High Priority**: Essential; must be fixed within hours of failure.
4. **🔴 Critical Priority**: Business-critical; failure impacts key operations and requires immediate attention.
