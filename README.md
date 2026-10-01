# AI Research Platform Backend

Backend services for the AI Research Experimentation Platform.

## Planned scope

- Organizations, users, roles, project membership, and access control.
- Project workspaces, dataset ingestion, and versioned dataset snapshots.
- Integration with Popper research runs, human decisions, and research outputs.
- Notifications, collaboration, publication releases, and usage/cost monitoring.

Popper owns the AI research runtime, scientific state, verification, evidence,
and provenance. This backend owns platform workflows and references Popper's
research records.

## Related repositories

- `capstone-project-docs`: business requirements, product requirements, and diagrams.
- `popper`: AI research core and runtime.

## Development status

Repository bootstrap only. The backend framework, database, authentication
provider, and API contracts will be defined before implementation.
