# Connector Plugin Example

Demonstrates the **composable Connector plugin API** — multiple connectors install into one Analyst, join across boundaries, and contribute widgets to a unified dashboard.

## What This Shows

1. **Two independent connectors** that each work standalone:
   - `ProjectConnector` — task tracking (tasks + time logs)
   - `TeamConnector` — team directory (members + departments)

2. **Composability** — both install into one Analyst via `analyst.use()`:
   ```python
   analyst.use(ProjectConnector(tasks_path="...", time_logs_path="..."))
   analyst.use(TeamConnector(csv_path="..."))
   ```

3. **Cross-connector relationships** — link entities across connectors:
   ```python
   analyst.add_relationship(Relationship(
       "tasks", "team_members", on=("assignee", "username")
   ))
   ```

4. **Cross-connector queries** — questions neither connector could answer alone:
   - "Hours logged by department"
   - "Task completion rate by role"
   - "Estimate accuracy by department"

## Run

```bash
pip install -e ".[plotting]"
python dashboard.py
```

## Structure

```
connector-plugin/
├── connectors.py     # Two Connector subclasses
├── dashboard.py      # Unified dashboard from both connectors
├── data/
│   ├── tasks.json    # Project tasks
│   ├── time_logs.csv # Time tracking entries
│   └── team.csv      # Team directory
└── dashboard.md      # Generated output
```

## Building Your Own Connector

```python
from dashboardmd import Connector, DashboardWidget, Entity, Dimension, Measure

class MyConnector(Connector):
    def name(self) -> str:
        return "my_connector"

    def sources(self) -> dict[str, SourceHandler]:
        return {"my_table": MySource(...)}

    def entities(self) -> list[Entity]:
        return [Entity("my_table", dimensions=[...], measures=[...])]

    def widgets(self) -> list[DashboardWidget]:
        return [DashboardWidget(name="Overview", title="...", build=self._build)]
```

Install it alongside any other connector — they all compose.
