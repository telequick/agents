"""Shared value types."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class JobAssign:
    """Engine → agent: a call has been routed to this worker."""

    call_id: int
    room_name: str = ""  # dispatched agent/call name — identity, not cosmetic
    agent_id: str = ""
    caller_number: str = ""  # ANI
    called_number: str = ""  # DNIS
    trunk_id: str = ""
    attributes: dict = field(default_factory=dict)
    # Effective resource tags on the agent (key/value, inherited down the
    # platform's parent chain) — the governed cost-allocation / ownership /
    # environment labels. Distinct from `metadata` (free-form operator context).
    tags: dict = field(default_factory=dict)
