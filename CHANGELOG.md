# Changelog

## [0.1.0] 2025-11-03 - Initial Release

### Added

#### Core Framework
- ✅ Modular code structure (`src/` directory)
- ✅ Support for multiple task types:
  - Notebook Tasks
  - SQL Query Tasks (inline SQL)
  - SQL File Tasks (SQL from files)
- ✅ Dynamic job generation from metadata
- ✅ Job lifecycle management (create/update/track)
- ✅ Execution order and dependency management
- ✅ Comprehensive error handling and logging

#### Examples
- ✅ 5 SQL task examples:
  1. Data Quality Check (null rate validation)
  2. Daily Aggregations
  3. Bronze to Silver Transformation
  4. Data Freshness Check
  5. Incremental Load
- ✅ Sample ingestion notebook demonstrating framework contract
- ✅ Comprehensive metadata examples YAML

#### Documentation
- ✅ README with quick start guide
- ✅ Code review documentation
- ✅ Architecture analysis

### Changed

#### Code Organization
- ✅ Refactored monolithic notebook into modular Python packages
- ✅ Separated concerns:
  - `constants.py`: Configuration constants
  - `utils.py`: Utility functions
  - `task_builders.py`: Task creation logic
  - `orchestrator.py`: Main orchestration functions
- ✅ Improved code reusability and maintainability

#### Metadata Schema
- ✅ Enhanced to support `task_type` field
- ✅ Backward compatible with existing notebook-only configs
- ✅ Supports SQL task configurations

### Technical Details

#### Task Type Support

**Notebook Tasks:**
```yaml
transformation_config:
  task_type: "notebook"
  notebook_path: "/path/to/notebook"
```

**SQL Query Tasks:**
```yaml
transformation_config:
  task_type: "sql_query"
  sql_task:
    warehouse_id: "your-warehouse-id"
    sql_query: "SELECT * FROM table"
    parameters: {}
```

**SQL File Tasks:**
```yaml
transformation_config:
  task_type: "sql_file"
  sql_task:
    warehouse_id: "your-warehouse-id"
    sql_file_path: "/Workspace/path/to/file.sql"
    parameters: {}
```

### Folder Structure

```
.
├── lakeflow_job_meta/    # Main package
├── examples/              # Example files and templates
│   ├── orchestrator_example.ipynb  # Orchestrator example
│   ├── sql_query_task/   # SQL query task examples
│   ├── sql_file_task/    # SQL file task examples
│   ├── notebook_task/   # Notebook task examples
│   └── metadata_examples.yaml
├── docs/                 # Documentation
└── tests/               # Tests
```

### Next Steps (Planned)

- [ ] Python Script Tasks
- [ ] Pipeline Tasks (Delta Live Tables)
- [ ] Enhanced error handling with retry logic
- [ ] Execution monitoring dashboard
- [ ] Data quality framework
- [ ] Unit tests

