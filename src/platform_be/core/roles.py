"""The fixed roles of the Platform, one enum per scope.

Access is decided by role alone: there is no permission table or role editor.
A user holds at most one role per scope instance (the platform, one organization,
one project).
"""

from enum import StrEnum


class PlatformRole(StrEnum):
    """System-wide role. Maps to "Admin / System Administrator" in the PRD."""

    PLATFORM_ADMIN = "platform_admin"


class OrganizationRole(StrEnum):
    """Role of a member inside one organization (tenant)."""

    ADMIN = "organization_admin"
    MEMBER = "organization_member"


class ProjectRole(StrEnum):
    """Role of a member inside one project. Same names as the PRD."""

    MANAGER = "project_manager"
    RESEARCHER = "researcher"
    REVIEWER = "reviewer"
