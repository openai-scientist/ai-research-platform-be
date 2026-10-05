"""The fixed roles of the Platform: the four the PRD names, plus the plain `user`.

Access is decided by role alone: there is no permission table or role editor.
A user holds exactly one platform role and at most one role in each project.
"""

from enum import StrEnum


class PlatformRole(StrEnum):
    """System-wide role. `platform_admin` maps to "Admin / System Administrator" in the PRD."""

    # Every account that is not a Platform Admin. It is not stored: a row in
    # `user_platform_roles` means Platform Admin, and no row means `user`.
    USER = "user"
    PLATFORM_ADMIN = "platform_admin"


def platform_role_from_code(role_code: str | None) -> PlatformRole:
    """The platform role for a stored role code, where no stored row means `user`."""
    return PlatformRole(role_code) if role_code else PlatformRole.USER


class ProjectRole(StrEnum):
    """Role of a member inside one project. Same names as the PRD."""

    MANAGER = "project_manager"
    RESEARCHER = "researcher"
    REVIEWER = "reviewer"
