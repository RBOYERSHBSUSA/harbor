"""
Module 3 Canonicalization

Phase 3B: Accounting Canonical Facts

This module provides canonicalization for accounting-system data (QBO, etc.)
into the canonical_facts table, compliant with Phase 3 invariants.

Per PHASE3B-ACCOUNTING_CANONICAL_FACTS_CONTRACT.md:
- Accounting facts are immutable, append-only observations
- No matching, inference, or accounting actions
- Deterministic IDs enable replay safety
- Source-agnostic design (QBO is first, not special)
"""

from .qbo_accounting_canonicalizer import (
    QBOAccountingCanonicalizer,
    AccountingCanonicalEventType,
    AccountingCanonicalCreationResult,
    generate_accounting_canonical_id,
    ACCOUNTING_NORMALIZATION_VERSION,
)

__all__ = [
    'QBOAccountingCanonicalizer',
    'AccountingCanonicalEventType',
    'AccountingCanonicalCreationResult',
    'generate_accounting_canonical_id',
    'ACCOUNTING_NORMALIZATION_VERSION',
]
