# Module 4: Processor Integration

## Overview

Module 4 handles integration with payment processors (Stripe, Authorize.net). It fetches payout and payment data from processor APIs and inserts it into the company database in a format that Module 3 can process.

## Architecture

```
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│   Stripe API        │     │   Module 4          │     │   charles.db        │
│   (or Authorize.net)│────▶│   Sync Manager      │────▶│   (company DB)      │
│                     │     │                     │     │                     │
│  - /v1/payouts      │     │  - Fetch payouts    │     │  - raw_events       │
│  - /v1/balance_txns │     │  - Track processed  │     │  - canonical_events │
│  - /v1/charges      │     │  - Insert data      │     │  - payout_batches   │
└─────────────────────┘     └─────────────────────┘     └─────────────────────┘
                                                                  │
                                                                  ▼
                                                        ┌─────────────────────┐
                                                        │   Module 3          │
                                                        │   QBO Integration   │
                                                        │                     │
                                                        │  - Create deposits  │
                                                        │  - Post to QBO      │
                                                        └─────────────────────┘
```

## Key Components

### ProcessorBase (processor_base.py)
Abstract base class defining the interface all processors must implement:
- `fetch_payouts()` - List payouts from processor
- `fetch_payout_details()` - Get payout with all payments
- `fetch_payment_details()` - Get single payment details
- `is_payout_complete()` - Check if payout is ready to process

### StripeProcessor (stripe_processor.py)
Stripe-specific implementation using Stripe's REST API:
- Handles authentication with secret key
- Navigates the payout → balance_transaction → charge hierarchy
- Extracts customer details (email, name)
- Calculates gross/fees/net amounts

### ProcessorSyncManager (sync_manager.py)
Orchestrates the sync process:
- Tracks which payouts have been processed (prevents duplicates)
- Converts processor data to database format
- Creates proper audit trail (raw_events → canonical_events → payout_batches)
- Ensures idempotency

## Usage

### Basic Sync

```bash
# Sync last 30 days of Stripe payouts
python scripts/sync_processor.py

# Sync specific number of days
python scripts/sync_processor.py --days 7

# Sync from different processor (when implemented)
python scripts/sync_processor.py --processor authnet
```

### Configuration

Add to your `.env` file:
```
STRIPE_SECRET_KEY=sk_test_xxxxx
```

Get your Stripe key from: https://dashboard.stripe.com/test/apikeys

### Programmatic Usage

```python
from charles.database.module2_database import get_db_manager
from charles.domain.processors import StripeProcessor, ProcessorSyncManager

# Get company database
db_manager = get_db_manager()
company_conn = db_manager.get_company_connection(company_id)

# Initialize processor and sync manager
stripe = StripeProcessor(api_key="sk_test_xxx")
sync_manager = ProcessorSyncManager(company_conn, stripe)

# Sync payouts
results = sync_manager.sync_payouts(
    start_date=date(2024, 12, 1),
    end_date=date(2024, 12, 14)
)

print(f"New payouts: {results['payouts_new']}")
print(f"Payments created: {results['payments_created']}")
```

## Data Flow

### Stripe API Structure
```
Payout (po_xxx)
  └── Balance Transactions (txn_xxx)
        ├── type: charge → Charge (ch_xxx) → Customer (cus_xxx)
        ├── type: fee → Standalone fee
        └── type: payout → The payout itself
```

### Database Structure After Sync
```
payout_batches
  ├── processor_payout_id: "po_xxx"
  ├── gross_amount: "500.00"
  ├── fees: "15.00"
  ├── net_amount: "485.00"
  ├── payment_count: 5
  └── status: "complete"  ← Ready for Module 3

canonical_events (per payment)
  ├── processor_payment_id: "ch_xxx"
  ├── processor_payout_id: "po_xxx"  ← Links to payout
  ├── amount_gross: "100.00"
  ├── fees: "3.00"
  ├── amount_net: "97.00"
  └── status: "pending"

raw_events (audit trail)
  ├── processor: "stripe"
  ├── processor_event_id: "ch_xxx"
  ├── payload: {...}  ← Complete original data
  └── checksum: "abc..."  ← For verification
```

## Idempotency

Module 4 ensures idempotent operation:

1. **Payout tracking**: Before processing, checks `payout_batches` for existing records
2. **Unique constraints**: Database has `UNIQUE(processor, processor_payout_id)`
3. **Safe re-runs**: Running sync multiple times won't create duplicates

## Error Handling

- Invalid credentials → Clear error message
- Network failures → Logged with context
- Partial sync → Already-processed payouts preserved
- API errors → Captured in results['errors']

## Adding New Processors

To add a new processor (e.g., Authorize.net):

1. Create `authnet_processor.py` implementing `ProcessorBase`
2. Implement all abstract methods
3. Update `sync_processor.py` to support the new processor
4. Add credentials handling

```python
class AuthNetProcessor(ProcessorBase):
    PROCESSOR_NAME = 'authorize_net'
    
    def fetch_payouts(self, ...):
        # Authorize.net uses "batches" instead of "payouts"
        # API: getSettledBatchListRequest
        ...
```

## Testing

### With Test Mode
Use Stripe test keys (`sk_test_xxx`) for development:
```bash
STRIPE_SECRET_KEY=sk_test_xxx python scripts/sync_processor.py
```

### Verify Data
After sync, check the database:
```bash
sqlite3 data/company_qbo_XXX/charles.db "SELECT * FROM payout_batches"
sqlite3 data/company_qbo_XXX/charles.db "SELECT COUNT(*) FROM canonical_events"
```

### Then Run Module 3
```bash
python scripts/run_module3.py
```
