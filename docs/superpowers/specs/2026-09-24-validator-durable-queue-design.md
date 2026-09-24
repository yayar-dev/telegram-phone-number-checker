# Durable queue for the local Telegram validator

## Objective

Keep the existing local form and job page while making every submitted number
durable, observable and resumable. A job must never silently lose an item or
turn a Telegram limitation into a negative result.

The implementation remains a local, single-account service. It does not move
work to another account when Telegram applies a limit.

## Current constraints and observed failure

The web app currently keeps job state in `.validator-jobs.json`, executes the
worker in a daemon thread, and derives job progress from an in-memory result
map. This is adequate for short batches but has weak crash recovery and no
durable per-item state. `contacts.importContacts` can return `retry_contacts`
because of a server-side limitation. Retrying that method for the same items
can leave a long batch looking stuck.

Telegram documents `contacts.resolvePhone` as a lookup that does not import a
contact. It must be rate limited to one request every three seconds, and the
same negative reply can represent no account or phone-lookup privacy. The
result therefore cannot be reported as definite absence.

## Product behaviour

The user keeps the current flow:

1. Paste company headers and numbers, then explicitly click `Поставить в очередь`.
2. Open the saved job and click `Запустить обработку`.
3. View progress and download CSV from the same job page.

The job page adds only operational detail:

- counts by final and non-final status;
- current phase (`импорт пакетами`, `прямая проверка`, `ожидание Telegram`);
- current and remaining count in the phase, observed speed and ETA;
- exact FloodWait countdown when Telegram provides one;
- `Пауза`, `Продолжить`, and `Отменить` controls; cancellation preserves all
  completed rows and labels untouched rows `CANCELLED`.

Results are exported at any time. A partial CSV includes every input row, its
current status and the selected validation method.

## State model

Replace JSON persistence with one local SQLite database, `.validator.db`,
using the Python standard library `sqlite3`. No server or new external runtime
is introduced.

### Tables

`jobs`

- `id`, `raw_input`, `state`, `phase`, `message`;
- `created_at`, `started_at`, `updated_at`, `completed_at`;
- `total_rows`, `completed_rows`, `paused_at`, `resume_after`;
- `import_batch_size`, `import_retry_count`, `last_error`.

`job_items`

- `id`, `job_id`, `ordinal` and company context (`company_name`,
  `company_link`);
- `raw_phone`, `normalized_phone`;
- `status`, `method`, `detail`, `attempts`, `next_attempt_at`;
- Telegram fields (`telegram_id`, `username`, `name`, `telegram_link`);
- `started_at`, `completed_at`.

`job_events`

- `id`, `job_id`, `created_at`, `kind`, `message`, `processed`, `total`.

`ordinal` is unique within a job. Several company rows may refer to the same
normalized phone; the worker performs one lookup and atomically applies its
outcome to every still-pending matching row.

## State transitions

Job states are `QUEUED`, `RUNNING`, `WAITING_RATE_LIMIT`, `PAUSED`,
`CANCELLED`, `COMPLETED`, and `FAILED`.

An item is `PENDING`, `IN_PROGRESS`, `FOUND`, `FOUND_NO_USERNAME`,
`UNRESOLVABLE_OR_PRIVACY`, `INVALID_PHONE`, `API_ERROR`, or `CANCELLED`.
Only the final item states contribute to completed progress.

Every state mutation is committed in one SQLite transaction. Before a request,
the worker marks its item set `IN_PROGRESS`; after a reply it persists results
and an event in the same transaction. On restart, unfinished `IN_PROGRESS`
items return to `PENDING`, never to a negative status. This makes a crash or
browser close recoverable.

## Single-account worker

One durable worker owns the configured Telegram session and claims ready work
from SQLite. It never issues simultaneous Telegram requests.

1. It takes up to ten unique `PENDING` valid numbers and calls
   `contacts.importContacts`.
2. Imported users become `FOUND` or `FOUND_NO_USERNAME`. Missing responses are
   recorded as `UNRESOLVABLE_OR_PRIVACY`, because privacy may hide an existing
   account.
3. Items returned in `retry_contacts` move to the `DIRECT_LOOKUP` phase. They
   are not re-imported in a loop.
4. Direct lookup calls `contacts.resolvePhone` one number at a time, with at
   least 3.1 seconds between calls. A positive response becomes `FOUND` or
   `FOUND_NO_USERNAME`; `PHONE_NOT_OCCUPIED` remains
   `UNRESOLVABLE_OR_PRIVACY` because Telegram uses it for private lookups too.
5. A `FloodWait` stores `WAITING_RATE_LIMIT` and the exact `resume_after`
   timestamp. The scheduler resumes automatically after it. Pause or cancel
   is checked before every request and while waiting.

The import batch starts at ten and may reduce to five, then one, only after
server retry signals. It never increases above ten. This improves the chance
of completing the import phase while respecting the observed safe batch size.

## Migration and recovery

On first start the application detects `.validator-jobs.json` and imports each
saved job into SQLite. Existing result maps, including completed rows from the
current 1,010-number job, are translated into final item rows. Deferred or
unprocessed rows become `PENDING` for direct lookup. The JSON source is kept
unchanged as a backup and migration is recorded in `job_events`.

If migration fails, the service does not delete or overwrite JSON and starts
with a visible configuration error. The database is backed up before every
schema migration.

## Security and retention

The server remains bound to `127.0.0.1`. Telegram credentials and session
files stay outside the database and are never served in HTML, CSV, logs or
events. Event logs contain counts and state changes, not phone numbers.

The database stores phone data locally because that is necessary for resume and
CSV export. A configurable retention command will remove completed jobs and
their rows; the initial default is no automatic deletion so ongoing local work
is not lost.

## Verification

Automated tests must cover:

- migration of existing JSON with completed and deferred entries;
- restart while importing, direct lookup and FloodWait;
- no duplicate Telegram request for duplicate normalized phone values;
- direct fallback for every `retry_contacts` item and a 3.1-second limiter;
- pause, resume and cancel without losing completed results;
- partial and final CSV preserving company context and all input rows;
- HTTP smoke: explicit queue confirmation, queued job, start, progress,
  pause/resume/cancel and CSV endpoints.

Manual live validation uses a deliberately small user-authorized sample. It
records only aggregate counts and statuses, never credentials or raw numbers.
