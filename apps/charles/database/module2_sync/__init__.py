"""
Module 2 Sync: QBO Accounting Sync Wiring

Phase 3B sync layer that fetches accounting objects from QuickBooks Online
and canonicalizes them into canonical_facts.

This module is OBSERVATION ONLY:
- Reads from QBO (Payments, RefundReceipts, CreditMemos, Deposits)
- Writes ONLY to canonical_facts via QBOAccountingCanonicalizer
- NO matching, NO inference, NO QBO writes
"""
