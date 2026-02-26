"""
Phase 3: Replay Orchestration & Forensics Module

================================================================================
PURPOSE
================================================================================

This module implements deterministic replay orchestration and forensic tooling
per PHASE3_REPLAY_ORCHESTRATION_FORENSICS_PLAN.md.

Components:
- ReplayOrchestrator: Orchestrates replay from stored raw data
- EquivalenceChecker: Field-by-field canonical event comparison
- ForensicsQueries: Operator-only forensic queries

CRITICAL ARCHITECTURAL CONSTRAINTS:

1. This module is the ONLY component aware of replay mode
2. Layer 2 (StripeProcessor) is completely unaware of replay
3. Replay does NOT write to production tables
4. Results are captured in memory for comparison

See: PHASE3_REPLAY_ORCHESTRATION_FORENSICS_PLAN.md
================================================================================
"""

from replay.equivalence import (
    EquivalenceChecker,
    EquivalenceResult,
    EventDifference,
)
from replay.forensics import (
    ForensicsQueries,
    RawObjectSummary,
    RawObjectVersion,
    LineageTrace,
    ForwardTrace,
)
from replay.orchestrator import (
    ReplayOrchestrator,
    ReplayReport,
    DryRunReport,
    ReplayStatus,
    ReplayMode,
)

__all__ = [
    # Equivalence
    'EquivalenceChecker',
    'EquivalenceResult',
    'EventDifference',
    # Forensics
    'ForensicsQueries',
    'RawObjectSummary',
    'RawObjectVersion',
    'LineageTrace',
    'ForwardTrace',
    # Orchestrator
    'ReplayOrchestrator',
    'ReplayReport',
    'DryRunReport',
    'ReplayStatus',
    'ReplayMode',
]
