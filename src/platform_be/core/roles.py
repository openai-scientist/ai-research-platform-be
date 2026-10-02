"""The fixed roles of the Platform, the same four the PRD names.

Access is decided by role alone: there is no permission table or role editor.
A user holds at most one platform role and at most one role in each project.
"""

from enum import StrEnum


class PlatformRole(StrEnum):
    """System-wide role. Maps to "Admin / System Administrator" in the PRD."""

    PLATFORM_ADMIN = "platform_admin"


class ProjectRole(StrEnum):
    """Role of a member inside one project. Same names as the PRD."""

    MANAGER = "project_manager"
    RESEARCHER = "researcher"
    REVIEWER = "reviewer"
