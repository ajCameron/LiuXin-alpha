"""
Export agent identity and intrinsic profile contracts.

AgentIdentityAPI describes the canonical agent; AgentProfileAPI and its
human/organisation specializations describe intrinsic profile metadata. Concrete
containers are provided separately.

Example:
    >>> AgentIdentityAPI.__name__, HumanAgentProfileAPI.__name__
    ('AgentIdentityAPI', 'HumanAgentProfileAPI')
"""

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.agent_containers.agent_identity_api import (
    AgentIdentityAPI,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.agent_containers.agent_profile_api import (
    AgentProfileAPI,
    HumanAgentProfileAPI,
    OrganisationAgentProfileAPI,
)

__all__ = [
    "AgentIdentityAPI",
    "AgentProfileAPI",
    "HumanAgentProfileAPI",
    "OrganisationAgentProfileAPI",
]
