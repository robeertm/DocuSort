# Changelog

All notable changes to DocuSort are documented here.
Dates are ISO; versions follow `MAJOR.MINOR.PATCH`.

This file starts with the first public release. The project was developed
privately before that; the summary under *0.1.0 – 0.55.0* lists what arrived
along the way rather than every single step.

## [1.0.0] - 2026-10-07

A full review of the code base, asked for in one sentence: find the corpses
and the dead code, and make this a finished product. Everything below was
found by measuring — against the running archive where that was possible —
and every fix is held in place by a test rig that fails if it comes back.

### Fixed

🔴 **Every server error message was shown in one language only.** The
biggest single finding. `web/app.py` raised 172 errors with a message, **four**
of which were translated — and **76 places** in the templates display that
message to the user verbatim (`transactions.html` twelve times,
`settings.html` ten). Ten messages were hard-coded German, the rest hard-coded
English. On an Italian installation, every failure spoke German or English.

The cause was mechanical rather than careless: translating needs the
language, the language lives in the request, and many handlers hold no
request — so every message would have needed a new signature. A context
variable, set once per request by the outermost middleware, removes that
excuse. All **142** messages now come from a key, in all five languages, with
their placeholders checked across languages. The same was done for the errors
raised in `retry.py`, `trash.py`, `rclone_setup.py` and `notifier.py`, which
reach the same display paths — **122 keys** in total.

🔴 **A setting that quietly did nothing.** `ai.min_confidence` was read from
config.yaml and then never used anywhere; the threshold that decides whether
a document is filed or sent to review was a fixed `0.65` inside
`Classification.is_confident`. Of 68 settings fields it was the only unread
one — and it decides where every document ends up. The threshold now travels
with the classification, the model is told the same number in its system
prompt, and the two call sites — which had drifted into
`is_confident and confidence > 0` on one side and `is_confident` on the other
— ask one question again. The guard against "confidence 0 counts as sure"
moved into `is_confident` itself, where a threshold of 0 cannot defeat it.

🔴 **A bank statement that yielded nothing said nothing.** `statement` and
`is_statement_candidate` were computed for every document page — a database
query per page load — and the template used neither. Measured on the running
archive: 375 documents filed as bank statements, 134 with no statement read.
117 of those are duplicates, which rightly have none; **17 are filed
statements from which not a single booking was read**. Seventeen statements
missing from the finances, with nothing anywhere saying so. The document page
now states either how many bookings came from the statement, or that none did
and why — and that reclassifying will try again. A gap nobody can see is a
gap nobody will close.

### Removed

🔴 **A second way in.** `POST /api/finance/import-csv`: around 70 lines that
rebuilt `/upload` — form files *and* a raw CSV body, "curl-friendly" by its
own docstring — including a third copy of the inbox-filing logic and without
the translated error messages added in 0.98.3. Nothing called it: not the
interface, not the mail gateway, nothing. There is one way in, and it is
`/upload`, which decides by file type. A second entrance is not a convenience,
it is a second place where behaviour drifts apart — which is exactly what had
happened. The test rig no longer merely proves that `/upload` works; it proves
that it is the only route that accepts a file.

🔴 **216 lines of code that nothing could reach.** Twelve module-level
functions with no caller, plus the cascade behind them: a whole pause
mechanism in `activity.py` (four functions), one whose own docstring said it
was "used by the page-by-page statement extractor" — which had been a corpse
itself — and one that carried its state in its name, `_ki_lage_unbenutzt`.
Also 20 imports that nothing needed any more.

🔴 **301 translation keys nothing could reach**, a quarter of the file. The
start page had been rebuilt around a new set of keys and the old ones stayed
behind; a statement-audit screen had been removed and its 46 strings had not.
Dynamically assembled keys were kept, and the deletion was verified the only
way that settles it: by building all 15 pages in all five languages, 75 page
builds, and checking that not one deleted key appears as visible text.

### Added

`pruefstaende/probe_leichen.py` — the general form of the two defects that
0.99.3 fixed: Python checks an import inside a function only when the
function is called, and an `except` clause above it turns the failure into a
log line nobody reads. Every project-internal import must now resolve, module
and name, wherever it sits: **312 imports across 69 files**. Plus: no
module-level function without a caller (519 checked), no orphaned import, no
template without a caller, no second spelling for the AI block, and a
counter-test that the sieve really does catch an invented class.

Three of the sieves written during this review reported false positives
first — 78, then 21, then twelve. A sieve nobody counter-tests produces a
list that looks like work and is not, and it teaches you to ignore real
findings. Every sieve in the rig now has its counter-test.

### Still open, deliberately

The statement reader understands one bank's layout. A statement from a
different bank produces no bookings at all — that is now visible on the
document page instead of silent, but reading it needs a real file in that
layout to build against. Seventeen such statements are waiting; their text is
still stored, so they can be caught up without a new scan.


## [0.99.3] - 2026-10-07

### Fixed

🔴 **"Retry classification" erased the document's stored text.** Reported as
a button that does nothing. It was three separate defects, and none of them
was a dead button.

The filing step stored the text as `text[: settings.claude.max_text_chars]`.
`settings.claude` is only another name for the same AI block, and since
`max_text_chars = 0` came to mean *as much as the model can take*, that
expression was `text[:0]` — **empty**. Every click therefore deleted the
document's stored text. Measured on a live archive: the document that had
just been retried held 0 characters.

The main pipeline keeps up to 200 000 characters at the very same point, with
a comment explaining why: second-pass extractors for statements and receipts
need the booking table that sits past the model's input limit. Two code paths
doing one job, answering differently — and the one that ran less often was
the one that threw data away. It now stores what the pipeline stores. How much
the model *sees* is decided when the prompt is built, which is where it
belongs; the database has no business truncating.

🔴 **There was no button for 711 of 862 documents.** The card was shown only
for `status in ('review', 'failed')` — fourteen documents. For everything
already filed there was no way to ask for a second opinion, although "filed
under the wrong category" is the most common reason to want one. It is now
offered for filed, review and failed documents alike.

🔴 **The result did not survive the page reload.** It was displayed for
1.5 seconds, then the page rebuilt itself and took the message with it. When
the model returned the same answer as before, nothing was left to see —
indistinguishable from a button that does nothing. The outcome now survives
the reload, and it names what changed: the previous classification, the new
one, and the resulting state. When the answer is unchanged, it says so
instead of staying silent.

### Removed

🔴 **82 lines in `retry.py` that could never run.** When a reclassified
document turned out to be a bank statement, the code called a helper whose
first act was `from .finance import StatementExtractor` — a class that does
not exist anywhere in the project. Every call therefore died on the import,
and an `except Exception` above it turned that into a log line. What went down
with it were the only places that enforced `finance.local_only` and
`finance.review_before_send`: two privacy switches guarding a path that was
not there.

The main pipeline reads statements with `finance.pdf_statement`, a text parser
that checks the bookings against the statement's own opening and closing
balance and sends nothing to any model. That same reader is now used here,
with the same follow-up work. **One reader, two callers** — the same file
produces the same bookings whichever way it arrived.

🔴 **The alias `settings.claude`.** It pointed at `settings.ai`, and it had
exactly one reader left in the whole project — the line above that erased
document text. That `claude.max_text_chars` and `ai.max_text_chars` are one
and the same field was invisible at the call site. A second spelling for one
field costs nothing and hides everything. A legacy `claude:` section in an
existing config.yaml is still read, in one place, where it belongs.

### Fixed

🔴 **Receipt extraction was out of service on every cloud provider, in
silence.** With a cloud provider and the default `finance.pseudonymize = true`,
`receipts.py` imported `finance.pseudonymizer` — a module removed in v0.33.0
whose call site stayed behind. The import failed, the caller logged a warning,
and no receipt was ever extracted. It never showed here because this
installation classifies locally, where the masking step is skipped anyway.

Nothing is sent unmasked to paper over it. The masking step is genuinely
absent, so a masking requirement that cannot be met is now refused out loud,
naming both ways forward: classify locally, or switch pseudonymisation off
deliberately.

### Added

`pruefstaende/probe_leichen.py`: every project-internal import must resolve —
module and name, wherever it sits, including inside a branch that rarely runs.
Python checks an import inside a function only when the function is called,
which is why both defects above survived for so long. **312 imports across 69
files**, plus a counter-test that the sieve really does catch an invented
class, a check that no template is left without a caller, and a guard against
a second spelling for the AI block returning.

### Changed

Two interface texts claimed the document is sent to Claude. On this
installation it is classified by the machine standing next to it. Reworded in
all five languages, together with the processing label.

`pruefstaende/probe_neu_klassifizieren.py`: **50 green, 0 red**, including a
browser run that clicks the button and checks the message is still there after
the reload.


## [0.99.2] - 2026-10-07

### Fixed

🔴 **The first document after every restart was still capped at 12 000
characters.** Found by measuring 0.99.1 on a running installation rather than
trusting it: the log said `12000` where it should have said `66630`.

`max_context()` read the model name from whatever a previous `classify` call
had stored. But the caller asks for the limit **before** it classifies —
that is the whole point, since the answer decides how much text to send. On
the first document after a restart nothing was stored yet, the provider
answered "unknown", and the limit silently fell back to the old default.
From the second document onwards it worked, which is the worst kind of
fault: one that fixes itself before anyone can catch it in the act.

**The model name is now a parameter** rather than something the provider is
assumed to remember.

🔑 **The test rig had let this through, and that is the more useful lesson.**
Its fake provider set the model name by hand, so it measured a situation that
never occurs. It now asks a provider that has never classified anything, and
checks that the classifier **passes the name in** — with a counter-test that
a provider asked without a name still answers "unknown".

🔴 **And 0.99.0 had quietly introduced two deadlines of its own.** Detecting
Ollama used a 3-second limit, asking for the model's context a 10-second one —
and a failure was **remembered**. One busy moment on the machine doing the
work would therefore have switched the whole fix off for the lifetime of the
process: every further document truncated at 2048 tokens again, with nothing
ever asking a second time. Exactly the silent degradation this release series
exists to end.

Both now follow the same rule as the classification itself: `timeout_seconds =
0` means **no deadline**, and an unreachable machine still fails fast because
the connection attempt has its own limit in the operating system. A **yes** is
remembered, a **no** never is — it is asked again on the next document.

**41 green, 0 red.**


## [0.99.1] - 2026-10-07

### Changed

🔑 **`ai.max_text_chars = 0` now means: as much of the document as the model
can take.** Asked directly: why a fixed 12 000 and not everything?

Because "everything" does not exist — but the ceiling belongs to the model,
not to DocuSort, and 12 000 was far below it. Measured on
`qwen2.5:7b-instruct`:

```
model limit            32 768 tokens   ← from the weights, not a setting
system prompt           9 025 tokens   (28 339 characters of instructions)
answer + reserve        1 112 tokens
──────────────────────────────────────
left for the document  22 630 tokens ≈ 71 000 characters
```

So nearly six times the old figure fits. DocuSort now **asks the model** —
Ollama reports `<architecture>.context_length` from `/api/show` — and uses
what is left after the instructions. A hosted service that does not state a
limit keeps the old default of 12 000: there every token costs money, and
"as much as possible" would be the wrong default.

🔴 **Zero has to survive every floor.** It is checked first, before any
`max(...)`, so that "no limit" in the settings does not quietly become the
limit it replaced. Same reading as `ai.timeout_seconds`, for the same reason:
on a machine of one's own, a token costs nothing but time.

🔴 **And `num_ctx` never asks for more than the model has.** A window beyond
the architectural limit is not generosity — it is memory reserved for
something the model cannot use.

🔑 **A document that still does not fit says so.** Count of characters, how
many fit, the model's limit, and how many are left out — in the log, instead
of silence.

### Added

🧪 `probe_ollama_kontext.py` grew two sections: that the limit is read from
the model and asked only once, that `num_ctx` is capped at it, and that a
configured `0` really opens the window (66 630 characters at a 32 768-token
model, against 12 000 before) while a provider that states no limit keeps the
old default. **33 green, 0 red.**


## [0.99.0] - 2026-10-07

### Fixed

🔴 **Ollama was shown only the first 2048 tokens of every prompt — and said
nothing about it.** Reported as scanned documents no longer being recognised,
with the remark that it used to work better. It did, and the difference is
measurable.

Measured on a running installation, not guessed:

* `input_tokens` read exactly **2050** on 119 of 327 local runs — including a
  document with 200 000 characters of text.
* A freshly built prompt (28 339 characters of instructions plus the document)
  came to **10 309 tokens**, of which Ollama evaluated **2 050**. Four fifths
  were dropped.
* 🔑 It was **not** prompt caching: a unique, uncacheable prompt was cut the
  same way twice in a row.
* The OpenAI-compatible endpoint DocuSort used **discards `num_ctx`** in every
  spelling — as a top-level field, inside `options`, either way: 2050.
* Ollama's own endpoint accepts it. With `num_ctx` the same prompt went from
  2 050 to **14 741** evaluated tokens, and a health-insurance letter that had
  been filed as *document / unknown / 0.50* came back as *Versicherung / IKK
  classic / 0.98* with its payment deadline recognised.

The damage was visible in the archive as a slope, and it was **silent** —
an answer always came, it was just worse:

| document text | confidence | answers with a wrong JSON schema |
|---|---|---|
| under 2 000 characters | 0.87 | 6 |
| 2 000 – 5 000 | 0.78 | 19 |
| 5 000 – 10 000 | 0.73 | 9 |
| over 10 000 characters | **0.67** | **29** |

The instruction describing the required output format sits in the front part
of the system prompt. The longer the document, the more of the prompt fell off
the end — which is why the model started answering with an entirely different
JSON object (`keys: Kategorie`) instead of the one it was asked for.

**Fixed:** when the endpoint answers `/api/version`, DocuSort now talks to
Ollama's own `/api/chat` and sets `num_ctx` **computed from the prompt it is
actually sending** — instructions, document, room for the answer and a
reserve, rounded up to the next multiple of 2048. Nothing else changes: an
endpoint that is not Ollama keeps the OpenAI-compatible path untouched, where
`num_ctx` would have no effect anyway.

🔑 **The context is sized by the prompt, not by a figure that ought to be
enough.** On a machine of one's own, compute time is not an argument against a
correct result; a truncated prompt is an argument against every result.

🔑 **And it can no longer go wrong quietly.** If the prompt still needs more
than the context can hold, that goes in the log with both numbers and what is
being cut. The original fault was not that something was dropped — it was that
nothing said so.

### Added

🧪 **New test rig `pruefstaende/probe_ollama_kontext.py`.** It talks to a small
HTTP service of its own, not to the machine's real Ollama — otherwise it would
measure how *this* computer happens to be configured and would be green for the
wrong reason elsewhere. It checks that the context grows with the prompt and is
never smaller than the prompt needs (including the exact size that failed),
that `num_ctx` is really sent and large enough, that Ollama's own response
shape is read (`prompt_eval_count`, `eval_count`), that a non-Ollama endpoint
is left on the old path **without** `num_ctx`, and that a context too small for
the prompt is logged — with the counter-test that a sufficient one is not.
**25 green, 0 red.**


## [0.98.3] - 2026-10-07

### Fixed

🌍 **The CSV importer's failure messages existed in German only — all five of
them.** Not one was in `en.json`, so an English, French, Spanish or Italian
user got the reason in a language they may not read. 0.98.2 made the importer
explain itself; this release makes that explanation reach everyone, because a
reason nobody can read is the same silence as no reason at all.

The reasons are no longer finished sentences built inside the importer. The
report carries the **key and its parameters** (`ImportReport.error_keys`,
`invalid_example_key`), and the sentence is composed where the user's language
is known — in the web layer, from the `lang` cookie or `Accept-Language`.

🔑 **A reason can contain a reason** ("1 row read, but not one of them usable —
the amount was not readable"). The inner one travels as its own key
(`reason_key`), so an English clause can never end up in the middle of a
French sentence. Both halves are rendered in the same language.

🔑 **`report.errors` stays English** for the log and the API: finding a fault
should not depend on the browser language of whoever hit it.

Nine new keys in all five languages (`de`, `en`, `fr`, `es`, `it`):
`finance.csv.err_empty`, `err_no_header`, `err_no_own_iban`,
`err_nothing_usable`, `bad_amount`, `bad_amount_empty`, `bad_date`,
`bad_date_empty`, `bad_unknown`.

### Added

🧪 `probe_csv_upload.py` grew a language section: that every key exists in
**every** locale, that the five renderings really differ (a counter-test
against "translated" meaning copied), that the report carries the key rather
than the sentence, and — through the real upload route — that a French browser
gets a French reason and an Italian one an Italian reason with the offending
cell inside it and no English clause left in the middle. **57 green, 0 red.**


## [0.98.2] - 2026-10-07

### Fixed

🔴 **A bank CSV that failed to import told the user something that was not
true.** Reported for an ING export that "would not load".

The ING **format** was not the problem: measured, `parse_csv` reads both
common ING exports cleanly, detects the bank, picks the account's own IBAN out
of the preamble and understands the German decimal comma. The fault was in
what DocuSort *says* when an import does not work. There is exactly **one way
in** since v0.51.0 — the upload page — and three faults sat on top of each
other there:

1. **`/upload` set `hint_iban: True` for *every* CSV error.** The interface
   turns that into "which account does this file belong to?" with an account
   picker, or "names no account — ask an administrator to import it". The
   **real** reason was thrown away. Anyone uploading a file with an unknown
   header or unreadable amounts was asked an account question that fixes
   nothing about it, and never learned what was actually wrong.
2. **The failure branch used the `error` state, whose label reads "network
   error".** The importer's own sentence appeared under a heading that is
   simply false. That label lives in the translation file, not in the code, so
   no API-level test could have seen it.
3. **When not a single row could be read, nothing was reported at all.**
   Header recognised, rows counted, amount or date unreadable → `rows_invalid`
   incremented, `continue`, and the result said "0 bookings" with no
   explanation. From the outside that is exactly what "it will not load" looks
   like.

**Fixed:**

* The report now carries whether a missing own IBAN was really the cause
  (`ImportReport.needs_account_iban`). `/upload` asks for the account **only**
  then, and shows the actual reason for every other failure.
* New `csv-failed` state on the upload page. An import reason is not a network
  error and now reads as itself.
* An import that could read nothing **names the count, the reason and the cell
  from the file** (`ImportReport.invalid_example`), and asks whether the file
  really came from the account's transaction list rather than a securities or
  credit-card export. Specific enough to read out over the phone.
* The failure path returns the **whole** report instead of three fields —
  rows read, rows rejected and the detected bank used to be lost at exactly
  the moment they are needed.

🔑 **One way in, and only one.** Measured explicitly and left that way: the
finance page accepts no file of its own and only points at the upload page;
`.csv` is not in `ALLOWED_SUFFIXES`, so a CSV is never filed as a document in
the inbox; and the upload route decides by file type what belongs in the
library and what belongs in the finances. A second route through the inbox
folder was started during this work and **discarded** — it would have solved
the same job in a second place.

### Added

🧪 **New test rig `pruefstaende/probe_csv_upload.py` — the CSV import had none
at all.** Real app (`create_app`), real database, real importer, and the files
go through `POST /upload` the way a browser sends them:

* that there really is only **one** way in (finance page without a file input,
  `.csv` absent from `ALLOWED_SUFFIXES`),
* both ING exports (bank, own IBAN from the preamble, decimal comma, signs,
  ISO date, counterparty in the right field),
* the route through the page into the real table, and that a second run
  duplicates **nothing**,
* that a file without its own IBAN asks for the account — **and** that
  supplying the IBAN then really imports it, so the question is not a dead end,
* 🔴 that every **other** failure names its reason and does **not** ask for the
  account, with the counter-test (`hint_iban is False`) — that is the fault
  itself,
* 🔴 **in a real browser**, that the reason is what stands on screen and not
  "network error", plus a counter-test with a good file ("1 booking imported"),
* and that the document route beside it is unchanged (PDF → inbox, `.txt`
  rejected rather than silently swallowed).

**49 green, 0 red.**

🟠 **Open:** this does not prove it explains the reported case. The ING files
in the rig are reconstructions, not a genuine ING export — and the importer's
ING support was added from assumption too. What is missing is the first few
lines of the actual file (amounts and IBAN can be masked) and what now appears
on screen when it is uploaded. This release makes sure something usable
appears there at all.

## [0.98.1] - 2026-10-06

### Fixed

🔴 **Installing a second time from a different folder took the installation
over — and showed an empty library.** Reported after a broken release and the
update that followed it: a user could not reach his database any more and
uploaded every document again.

Reproduced on a real Docker machine, and it is exactly what happens. The
install directory follows the **working directory** (`$PWD/docusort`), and the
compose file it writes mounts **relatively** (`./data:/data`). Whoever runs the
one-liner a second time while standing somewhere else — in the downloads
folder, in the home directory, inside the installation itself — gets a second
directory. The container is called `docusort` either way, so compose rebuilds
it with the new paths. DocuSort runs, the library is empty, and the old
database sits untouched in the first folder. Measured: return code 0, not one
word about it.

**The installer no longer follows the working directory.** It asks, in order:
`DOCUSORT_DIR` if you set it · where the **running** container keeps its
documents · whether you are standing **inside** an installation (that is how
`docusort/docusort` was born) · whether one is already next to you. Only then
does it make a new one. Simulated on a real Docker machine: a second run from
another folder now says *"Using …/zuhause/docusort — this is where the running
DocuSort keeps its documents"*, the container keeps its data, and the three
documents are still there. Standing inside the installation no longer creates a
nested one.

**And it looks for an archive that nothing is using.** Two levels deep in the
places people actually stand, never the whole disk. If the folder this run
would have used is empty and exactly one such archive exists, it installs
*there* instead — without moving a single file.

### Added

🔀 **Two archives can be made into one.** Whoever ended up with two data
directories has their post in halves — and usually uploaded the second half by
hand. `--merge-from` puts them together:

```
docker run --rm -v <installation>/data:/data -v <installation>/config:/app/config \
  -v <other>/data:/fremd:ro ghcr.io/robeertm/docusort:latest \
  python -m docusort --merge-from /fremd
```

It **shows** first and writes nothing; `--merge-apply` carries it out. The
installer offers the same thing when it finds another archive, asks before
doing anything, and stops the container while it works — two writers on one
SQLite file is a way to lose both halves.

What it promises, and what is measured in `probe_zusammenfuehren.py` (39
checks, against real databases and real files):

* **Nothing is deleted.** The source is opened read-only, the target is backed
  up first.
* **Nothing is duplicated.** The same document in both archives is recognised
  by its **content** (SHA256), not by its name — a PDF uploaded twice under two
  names counts once. Bookings go by their hash, accounts by the IBAN hash.
* **The file comes with the row.** A row whose PDF is missing is *not* taken
  over, and it is named.
* **Running it twice changes nothing the second time.**
* **The other archive is untouched**, so every step is reversible.
* Learned rules and categories are merged; accounts, sessions and settings of
  the target are left exactly as they are.

### Fixed

**If this happened to you, nothing is lost.** Ask Docker where the current
installation keeps its data:

```
docker inspect docusort --format '{{range .Mounts}}{{.Source}} -> {{.Destination}}
{{end}}'
```

and look for the other one:

```
find / -name docusort.db 2>/dev/null
```

The bigger file is your archive. Stop the container, put the old `data`,
`config` and `logs` folders back beside the compose file that is in use (or run
`docker compose up -d` in the old folder instead), and everything is there
again.

🔴 **Three comment lines in the installer were executed as commands.** The
compose file is written through an unquoted heredoc, so the backticks around
`restarting` and `scanned=0` were command substitution. Users saw
`install.sh: line 100: restarting: command not found` during an otherwise
healthy install, and the words were missing from the generated file.

The hint above a relaxed search on the invoice page promised something the page
does not do. It was the library's text, reused because both use the same search
ladder — but the library sorts a relaxed answer **by relevance** and says "the
best one first". The invoice page sorts by date and groups by topic.

Measured on a real archive: `stadtwerke radeberg` found 136 of 152 invoices —
every one that names the town. That is useful behaviour, but only when the line
beside it says what it is. The page now has its own two sentences, and neither
promises an order.

## [0.98.0] - 2026-10-06

### Fixed

🔎 **The search under *Invoices* now searches where the library searches.** It
was reported that this page does not show everything, unlike the library. It
did not. The page compared what you typed against six fields as a plain
substring; the library reads the whole document, word by word. Measured on the
same documents, before the fix:

| typed | library | invoices |
|---|---|---|
| `telekom rechnung` | 3 | **0** |
| `telekom maerz` | 1 | **0** |
| `kundennummer` | 1 | **0** |
| `123456` | 1 | **0** |

Two words could only ever match if they stood next to each other **in one
field**, and the text of a scan was not read at all. The page now uses the
same three-step search ladder as the library (exact · part-of-word · the most
words, by relevance) and says when it had to relax — alongside the field
match, which stays: the amount that was read out of a document
(*"Rechnungsbetrag 54,95 €"*) is in no full-text index, and whoever types
`54,95` means the amount.

Two more things that quietly hid results:

* The counter next to *show notes* counted across the **whole** archive. With
  a search running it therefore spoke of documents nobody was looking for —
  or stayed silent about the three hits that were sitting behind it. It now
  counts your own hits, and a line says so when there are any.
* A document that matches but has **no amount read from it** can never appear
  on this page. That is now said, with the count and a link into the library,
  instead of being passed over in silence.

🧭 **One navigation instead of three.** The same areas were reached in three
different ways: from 1536 px a flat row plus a finance menu, between 768 and
1535 px a scrolling strip with all nine targets in a different order, and on
the phone a sheet with a third order — *receipts* in the middle of the finance
block, two targets sharing one icon, and on the large screen no mark at all
for the page you were on.

There is now **one list**: two groups (*Documents*, *Finance*), one order, one
icon per target, one rule for "where am I". The wide screen shows it as two
menus, the phone as a sheet with the same headings — same content, same
order, both marked. The scrolling strip is gone: two menu buttons need about
200 px instead of 879, which is what made the strip necessary in the first
place. Measured at 1536 px, every language now has over 640 px to spare.

🤖 **The local-model setup keeps Ollama alive on Linux.** Reported: the setup
file has to be fetched again and again, otherwise the AI will not start. The
reason stood in the source as an intention — without a systemd service Ollama
was started as a child of the setup script, and the comment said the next run
of the file would do it again. After a logout or a reboot it was gone, and the
way back led through a *new* launcher, because the ticket in the old one is
spent after its first success.

Three gaps closed, all on Linux:

* **A service that exists but is switched off** (the default in the Arch
  family) was only *restarted*, never *enabled* — that holds until the machine
  is switched off. It is now enabled, and the setup says so.
* **The address was right but the service was dead** — the setup reported
  success without anything listening and died three steps later with
  "Ollama is not reachable". It now asks whether the service runs, starts it,
  and only then reports success.
* **No service at all**: Ollama now gets a service of its own under your
  account (`~/.config/systemd/user/docusort-ollama.service`) that starts at
  login, restarts after a crash and, with linger, comes back after a reboot —
  the twin of the startup item on macOS, which had been there for months. If
  something else already starts Ollama, it is named and left alone.

Every one of these is guarded by a test bench that fakes the machine
(`systemctl`, `sudo`, `loginctl` and `ollama` are stand-ins): 117 checks, with
a counter-test that turns red against the old behaviour.

### Changed

The gear for *Settings* is visible from 768 px upwards. It used to appear only
from 1536 px, because the strip below the header carried it in between — and
that strip is gone.

## [0.97.1] - 2026-10-05

### Fixed
A comment in the new planner's source read like a quoted person. The rule is
that no verbatim speech belongs in a public repository; it was rewritten.
Nothing about the planner itself changed.

Worth recording why it was not caught before the first build: the check reads
the files **git knows about**, and a brand-new file is untracked until it is
committed. The first gate therefore could not see it at all.

## [0.97.0] - 2026-10-05

### Added

🎯 **A budget planner.** A new tab that asks what none of the other pages do:
*what would happen if.* Everything else looks backwards at money already
spent — this one looks forward, and it starts from measurement rather than
from guesswork.

**Income is the median of your regular earnings**, and only the regular ones.
An inheritance, a tax refund, a closed savings account are real money, but
nothing a monthly commitment may stand on; they are listed with their total
and deliberately left out of every figure. The current month is never counted
either — on its third day it would make any household look frugal. Median
rather than average throughout, because a single kitchen paid in cash would
otherwise set the level for the whole year.

**A contract is not a habit.** Both repeat, and the fixed-costs detector sees
both the rent and the weekly supermarket. But the rent is the same amount on
the same day and the supermarket is not, and only what truly repeats becomes
the **floor** of a pot: the slider will not go below it, because that part
needs a cancellation rather than a decision. Tick the contract and the floor
drops. A premium paid once a year shows nothing in eleven months and is
budgeted monthly all the same.

**The goal is three blocks that add up**, and you use only the ones you need:
simply more left over each month, a housing cost that is known to change
(moving, a mortgage instead of rent), or a sum by a date, which becomes a
monthly rate. A bar that stays in view says what is still missing.

**Then you move the sliders.** What you take from one pot lands in what is
left over, live. Beside them, where something can be found: proposals measured
straight out of your own bookings, each naming the figures it rests on — "in
the cheapest month it was 186 €, usually 540 €". Cancelling your own rent is
not offered as a saving; that is a move, and the goal already models it.

**And a local model may add ideas.** It suggests where and how, in one
concrete sentence — and every amount it names is capped at the measured room
of that pot before it is shown. DocuSort does the arithmetic, never the model.
Without a model the planner is complete; the measured half is the half that
cannot be wrong.

Nothing is written while you drag. Calculating is a question, saving is a
decision, and only the Save button writes.

## [0.96.0] - 2026-10-05

### Fixed

🔑 **Changing one booking's category changed every booking of that payer.**
Reported with a screenshot of five income bookings from the same person: one
was dragged to a different category, and all five followed.

The cause was not arithmetic, it was a default. A tick box labelled "remember
this assignment for this payee" sat *below* the spending table, pre-ticked,
and decided for every row in it. In the income block it was missing
altogether, so there was no way to say "only this one" at all. Every manual
assignment therefore wrote a rule for the whole payee and recoloured that
payee's entire history — silently, going back years.

In one real archive that had produced 305 learned rules covering 2738
bookings: a rule on a payment service held 670 of them, a rule on a public
pay office held 129 including a complete salary series, and a rule on the
owner's own name held 675.

**The decision now belongs to the row.** Behind every category dropdown — in
the spending block and in the income block — there is a small *remember* box.
It is never pre-ticked. Without it, an assignment moves exactly one booking
and writes no rule. With it, learning works as before and the message says
who was learned and how many further bookings were adjusted.

The same tick box in the booking explorer is no longer pre-ticked either.
Nothing that was already learned is touched: existing rules keep working, and
the "Rules" list in the booking explorer remains the place to remove one.

🔑 **The one-time password led into a room with no exit.** Signing in with it
worked, DocuSort then demanded a password of your own — and that form asked
for the *current* one. The current one was still the old, forgotten
password: the one-time code was never the account's password, it lived
elsewhere. Signed in and unable to do anything.

Whoever DocuSort itself forces to set a new password no longer has to know
the old one — the existing session is the proof, and the field is marked as
optional while that is the case. A rescue path that assumes you remember what
you have forgotten is not a rescue path. Everyone else still has to type
their current password, and the one-time code still works exactly once: an
earlier attempt at this fix turned the code into the account's password,
which quietly gave it an unlimited lifetime.

### Added

**Every row now says where its category came from.** The same badges as the
booking explorer — by hand, learned, recognised, transfer, bank — sit next to
each dropdown in the spending and income drill-down, with the full reason as a
tooltip. A rule that colours hundreds of bookings should be visible where the
wrong number catches your eye, not only in a separate list.

**The login page fills in your username.** Whoever has forgotten their
password often no longer knows the user name either. It was already written
in the one-time-password message and in the fallback file; now it is also
entered into the form, so there is nothing to type.

## [0.95.0] - 2026-10-05

### Fixed

🔑 **You could lock yourself out of your own archive, permanently.** A user's
password stopped being accepted and there was no way back in — no command, no
page, nothing. The documents sat untouched on the disk and were unreachable.
Signing in was the only door and it had no second lock.

There are now three ways back, and you need only one of them.

**On the login page itself**, under "Forgotten your password?", a button asks
DocuSort for a one-time password. It goes out through whatever notification
channel you already set up — Telegram, email. If you have none, DocuSort
writes it into a file in its own config folder instead, which you can open
from your NAS file manager or a network share without ever touching a
console. A console is a thing many owners do not have; a reset that assumes
one is not a reset.

Pressing that button changes **nothing**. Your current password keeps working
until somebody actually uses the one-time one, so a stranger who presses it
cannot lock you out — they can only send you a message. The code is valid for
fifteen minutes, works exactly once, dies after five wrong attempts, and can
be requested at most once every two minutes. Logging in with it immediately
asks you for a password of your own.

**On the machine**, for whoever has a shell:

    docker exec -it docusort python -m docusort --reset-password

It picks the first admin, sets a new random password, prints it once and
stores it nowhere. `--list-users` shows the accounts, `--reset-password NAME`
takes one by name, and `--add-admin` creates an *additional* administrator
without touching any existing account. The password is rolled fresh every
time: a built-in emergency password would be the same on every installation
and would be sitting in public source.

🔴 This is not a weakening. Everything above requires standing at the machine
DocuSort runs on, or holding a channel that is already yours — and anyone who
can `docker exec` on that host can read the database and every filed document
anyway. A lock that only keeps the rightful owner out protects nobody.

📮 **The Postwache account can be reset the same way**, with its own command:

    docker exec -it docusort python -m docusort --reset-postwache

It needs one, because that password does not live in the database: it comes
from a pairing secret in the config folder, and DocuSort re-asserts it on
every start. Changing only the database hash would hold until the next
restart and then quietly revert. This rolls the secret itself — enter the same
word in Postwache afterwards. Resetting a machine account by name no longer
promotes it to administrator either; it keeps the narrow role it is supposed
to have.

A probe holds the whole property, and it presses the button rather than
reading the template: after a reset you can really sign in, the old password
really stops working, the one-time code really works exactly once, and the
Postwache word really survives a restart.

🔘 **And the first version of that button did nothing at all.** It was written
with Alpine directives, and the login page does not load Alpine — it is
deliberately free of frameworks. The button could be pressed, and nothing
happened: no message, no file, no error. The probe was green because it only
checked that the template *mentioned* the command. It clicks now.

## [0.94.0] - 2026-10-05

### Fixed

📉 **Every bar in "Income & spending per salary month" was the same length.**
The chart listed eighteen periods — one bar for money in, one for money out —
and all of them ran the full width of the card. It had not shown anything for
some time, and it looked entirely healthy while doing so.

The width was written by the display formatter:

    style="width: {{ x | zahl(1) }}%"   ->   width: 6,1%

That formatter writes numbers the German way, with a comma. CSS cannot read a
comma there, so the browser discards the whole declaration — no error, no
console message, nothing wrong-looking in the source — and the bar falls back
to the full width of its box. The same formatter was on the budget progress
bar, and on the starting-balance input field, where a Jinja precedence detail
(`a or 0 | zahl(2)` binds as `a or (0 | zahl(2))`) meant an existing balance
was printed raw while only the fallback was formatted.

Display formatting now stops where a machine reads: a separate `css_pct`
filter writes CSS widths, clamped to 0–100. A probe renders the page and
holds the property — every width is a number CSS can parse, and two different
values produce two different bar lengths. A bar chart where every bar is
equally long is a failure even when every value is valid.

📱 **The account filter could not be reached on a phone.** The panel is 256
pixels wide and was pinned to the right edge of its button. On a phone the
toolbar sits on the left, so the panel extended off the left side of the
screen: its left edge started at −129 px. A strip of it was visible and not a
single checkbox could be ticked. The same filter on /fixkosten had it too.

The phone probe could not have found this: the panel is `x-show` and therefore
invisible until opened, and invisible elements are (rightly) skipped. It now
opens every disclosure it can find — any button whose `relative` box contains
an `x-show` panel — and measures each one. Nothing else is clicked.

📐 **And the salary-month table ran off the left on a 360 px phone.** Four
fixed number columns plus their gaps come to 340 px; a 360 px phone leaves
about 328 px after the page margin, and because the row is right-aligned the
income column started at −17 px. The columns are narrower below `sm`. Found
by the extended probe, not reported.

### Added

🗓 **/ausgaben can cut any period, not just one month.** The salary month
stays the default and keeps its list — it is the question that matters daily.
Beside it: the last 3, 6, 12 or 24 months, a calendar year, or a free
from–to. Every one of them carries a previous period of the *same length*, so
the comparison means something: a year against the year before, a free range
against the equally long range immediately before it. An incomplete from–to
falls back to the salary month rather than showing an empty page, which would
look like "no bookings".

◆ **Special pots can be counted back in.** The figure that says how much was
left out is now the switch that puts it back — one click, and the view
recalculates with those categories included; the badge then says so. It is a
property of the *view* and lives in the address, so a link still shows what it
says. Which categories *are* special pots is a setting and is not touched.

📄 **And the period can be exported as a PDF — one sheet, A4.** Totals,
comparison with the previous period, every category with its bar and share,
income by category when it fits, fixed against variable at the foot. It is
drawn from the same calculation the page uses, never a second one beside it.

Nothing flows onto a second page: the number of rows is computed from the
space that is actually left, and whatever does not fit is summarised as
"Others (n)". A canvas never breaks a page by itself — it would simply draw
past the bottom edge and lose the content — so the probe checks the position
of every piece of text, not just the page count.

The sheet embeds Bitstream Vera (shipped with reportlab). The fourteen
built-in PDF fonts are not embedded, and the original Helvetica has no euro
sign at all; the substitute glyph is wider than the declared metric, so
"9.000,00 € aus Sondertöpfen" came out as "€aus" — the space was drawn and
then covered. That was found by looking at the sheet, not by measuring it.

This adds `reportlab` to the requirements.

## [0.93.1] - 2026-10-05

### Fixed

🏷 **Nine category names had no label in any language — and one of them was
the one the statement reader writes.** Category names are stored as they are
filed, because they are folder names on disk; the interface translates them
for display. `Kontoauszug` and its six subcategories arrived with the bank
statement reader and were never given a label, so an English, French, Spanish
or Italian interface showed a card reading "Receipts · Supermarket" directly
above one reading "Kontoauszug · Girokonto". `Bank/Vertrag`, `Bank/Sonstiges`
and `Bank/Depot` had the same gap.

It went unnoticed for months because a missing label falls back to the name
itself — a gap looks exactly like a decision. It was found in a README
screenshot.

🔤 **Two English labels named a category differently from the English category
list.** The interface said "Salary" and "Banking" while the list in the same
installation offered "Payroll" and "Bank" — the same drawer under two words.
The labels now use the word the list uses. (The booking type stays "Salary":
a salary payment and a payroll document are different things.)

A probe now holds the property: every name in the German category list has a
label in all five languages, the English label keeps its role, and the
category dropdown submits the canonical name rather than the label — which is
why a French label never needs a role at all.

🗣 **And four interface texts named a category in German.** /settings told an
English reader "Every new Kontoauszug pauses after OCR"; the receipts rescue
panel offered "Move to Kassenzettel" in French, Spanish and Italian too. These
texts now carry a placeholder which the page fills in two steps: resolve the
role to the name *this* installation uses, then label that name in the
reader's language. Only the first step would be wrong for a German library
viewed in English; only the second would be wrong where the category was
renamed. (Three further strings carried the same German word but are called
from nowhere — measured, not assumed. They were corrected along with the rest
and were never a problem anyone could see.)

📄 **The CSV import hint in French, Spanish and Italian still described one
bank.** The importer has read nine for a while, recognises the bank from the
columns and skips bookings it already has. Those three languages now say so.

## [0.93.0] - 2026-10-05

### Changed

📱 **The phone view, rebuilt rather than squeezed.** Measured at 420 × 912 —
the iPhone Air — on sixteen pages, in German, with real data shapes:

| | before | now |
|---|---|---|
| controls stretched into capsules | **62** | **0** |
| text crushed into a narrow column | 16 lines of ~16 characters | **0** |
| library, first document visible at | y ≈ 470 px | **y ≈ 403 px** |

🔴 **The cause of the oversized buttons was the house rule itself.**
`button { min-height: 44px }` pulls *every* button to thumb size — a 20 px
checkbox became a 20 × 44 capsule, and a "×" became an 8 × 44 sliver. There
was one on every library card. The new `.tap` class takes the minimum height
back and gives the thumb an **invisible** 44 × 44 field instead: the eye sees
a square, the thumb still hits. Measured — a tap 14 px beside the box lands,
30 px beside it does not.

Also: the finance intro card stacks instead of squeezing a paragraph into a
125 px column · the library shows search and one filter row with a **count of
active filters**, status as chips · cards lead with the **document name**
instead of the category chip, and the confidence/cost figures are desktop-only
· the bookings page gets a layout of its own on a phone — one headline figure,
the income/expense pair, and what is *not* counted as a quiet line, instead of
seven ragged tiles.

💶 **One way to write a number.** Fifty places in the templates formatted with
Python's `%.2f` — `1836.60` — while four pages each carried their own
JavaScript formatter. One page said `1836.60 €`, the next `48.720,00 €`. There
is now one filter pair on the server (`money`, `zahl`) and one helper in the
browser (`DS.geld`, `DS.zahl`), and they agree.

🎨 **The chart palette is one family.** Twelve colours of mixed saturation
looked like a crayon box in a small donut. They now sit on one lightness step
and walk the colour circle in order, starting at the brand green; the
catch-all is deliberately muted.

🔴 Two of my own mistakes, both caught by measuring rather than by reading:
`position: relative` in the new `.tap` rule out-ranked the `absolute`
utility, so every card's checkbox fell out of its corner in front of the
title — fixed with `:where()`, which counts as zero. And the bulk rewrite of
the number formatting produced `{{ a + b | money }}`, where the filter binds
tighter than the plus: two pages answered 500 until every such expression got
its brackets.

## [0.92.0] - 2026-10-05

### Added

🔦 **A search hit is now shown where it sits in the document.** Finding a
document by its content is only half the job — the other half was scrolling
through twenty pages of a council notice looking for the word. Open a document
from a search and the page it was found on is shown with the word **highlighted
in yellow**, plus a line saying "match 1 of 4 · page 3" and arrows to step
through the rest. Hide it and the normal preview comes back.

🔑 The coordinates come from `pdftotext -bbox-layout` — the same `poppler-utils`
that already renders the page images, so nothing new is installed. The stored
text knows *that* a word occurs; only the PDF itself knows *where*.

🔴 Boxes are stored as a fraction of the page, never in points. The page image
is rendered at a different width than the PDF measures, and the column scales
it again — a box in points would land anywhere but on the word. The probe
checks the drawn box against the reported coordinates, and that a word at the
foot of the page really lands at the foot: PDF counts its y from the bottom,
CSS from the top, and a flipped origin looks perfectly plausible in both.

The highlight uses the same matching as the search — start of a word first,
then anywhere inside one — so the list and the page can never disagree about
what was found.

## [0.91.0] - 2026-10-05

### Added

🔍 **The search finds what is written, not just what a document is called.**
It already read the full text, but it only matched whole words from their
beginning, and it demanded every word you typed. Measured on a real library of
688 documents: three words found 14 documents, four words with one of them
slightly off found **nothing at all**.

There is now a ladder of three rungs, and the page says which one answered:

1. **exact** — all your words, from the start of a word (what it did before)
2. **part of a word** — all your words, anywhere inside a word. "energie" now
   finds "SachsenEnergie", "waren" finds "Haushaltswaren". German compounds
   and company names are exactly where the old search went quiet.
3. **closest** — most of your words, best match first. One word off no longer
   costs you the whole answer.

A rung is only used when the one above found nothing, so an exact answer never
comes mixed with a vague one. Results now also show **where in the text** the
match sits — unless that place is the sender or the subject, which the card
already shows.

🔑 The part-of-a-word rung is a second index (SQLite FTS5 trigram). It is built
once, in the background, on first start after the update — 6.9 MB of text took
a moment. Where SQLite is too old for it, the other two rungs carry on and
nothing breaks.

💡 **The edit fields suggest what already exists.** Type "spark" into the sender
of a document waiting for review and you get "Sparkasse" and
"Sparkassenversicherung" with the number of documents behind each — click, or
arrow keys and Enter. Also for tags and subcategories. Without it the same
sender slowly turns into three spellings, and every figure that groups by
sender quietly falls apart. Matching is done on the server, so the German
transliteration (ä→ae) lives in one place instead of two.

### Fixed

🔴 **Category names are translated — the comparisons on them were not.** Eleven
places in the program compared a document's category against a fixed German
word. `categories.yaml` ships in two languages, so in an **English**
installation none of them matched. Two consequences, one of them bad:

* `update_metadata` deleted a receipt's line items unless the category was
  literally "Kassenzettel". In English it is "Receipts" — so **every** edit of
  a receipt's metadata silently threw its items away. Counter-tested: on the
  old code the probe reports "before 2, after 0".
* The statement tidier wrote the category "Kontoauszug" into the database —
  a name the English category list does not contain. The document landed in a
  folder the interface never offers.

Categories now carry a **role** (`kategorien.ROLLEN`): the role is what the
program compares, the name is whatever stands in your list. Recognising and
writing both go through that one table. A source-code guard fails the build if
a fixed German category name shows up in a comparison again.

## [0.90.2] - 2026-10-04

### Changed

🔴 **`:latest` no longer moves when someone pushes.** This is the change that
matters. Every installation follows `:latest` through Watchtower, and the tag
moved the moment a commit landed on main — before anything had looked at the
image that was built. The pre-release gate checks an image built *locally*;
what users receive had never been touched by it. On 04.10. a version that
could not start on an existing database went out that way.

The order is now:

1. push to main → publishes `:<version>` and `:edge` only
2. the gate runs **against that published image**, including an upgrade from
   the previous release on a database that version wrote
3. only then `promote.yml` (manual) moves `:latest`, and Watchtower
   distributes it

`promote.yml` does not rebuild: `imagetools create` attaches a second tag to
the existing manifest, so what was tested is byte for byte what users get.

### Added

🔑 **The gate now upgrades from the version users are running.** The
existing "upgrade an existing installation" scenario created its data fresh,
so the database was written by the *new* code and no migration ever ran. The
new scenario starts the newest release older than the image under test, lets
it write its database, then puts the new image on top and checks the HTTP
answer, the log, **and** that the container is not in a restart loop.

Counter-tested against the broken 0.90.0 image: HTTP 000, 7 tracebacks,
`restarts=10` — **NICHT AUSLIEFERN**. The first version of this check passed
it, because it used the same version as predecessor and successor; the
predecessor is now required to be strictly older, read from the image itself.

🔴 **Watchtower gets `--include-restarting`.** Measured on a real machine
with the very Watchtower the installer ships: a running container is
`scanned=1, updated=1`, a crash-looping one is **`scanned=0`** — not even
looked at. Without the flag a broken release stays broken on every
installation until somebody intervenes by hand. With it, the next hourly run
picks up the fix.

The release probe now asks whether `latest` points at **exactly this**
version, instead of whether a tag called `latest` exists somewhere.

## [0.90.1] - 2026-10-04

### Fixed

🔴 **0.90.0 would not start on an existing database.** The index for the new
`transactions.besonders` column was part of the schema script, which runs
*before* the migration. On a fresh database the same script creates the table
with the column, so every probe was green. On an existing one
`CREATE TABLE IF NOT EXISTS` does nothing, the column is not there yet, and
startup died in a loop:

```
sqlite3.OperationalError: no such column: besonders
    db.py  self._conn.executescript(SCHEMA)
```

The index is created in the migration now, after the column exists. No data
was touched — the crash happened before any migration ran.

🔑 **A migration can only be tested against an old database.** The probe
builds one by creating today's schema and then dropping the new column, which
is exactly yesterday's shape and stays correct when the table changes again.
Counter-tested: against the previous code it fails with the very error seen in
production. The pre-release gate installs onto a fresh database, so it could
not see this.

## [0.90.0] - 2026-10-04

### Added

**Special expenses — money that really left the account and still should not
count.** A house build, an inheritance, a one-off: 25,632 € across fifteen
bookings next to an everyday life of a few hundred euros wrecks every average.
There are now two ways to declare it, because they are two different
questions:

* **A whole pot.** On */fixkosten*, next to "What counts as a fixed cost?",
  a second field: *What comes out of a special pot?* Tick **Hausbau** and
  everything in that category drops out everywhere — including bookings
  imported tomorrow.
* **A single booking.** On */transactions*, select rows → **Mark as special**.
  This works on categories that are not in a pot at all.

**It drops out of every calculation**, and that is measured rather than
claimed: the explorer, the headline totals, the monthly series, the salary
month, the largest payees, */ausgaben* and the **fixed-cost calculator** all
stop counting it.

🔴 **But nothing disappears without trace.** The booking stays in every list
(dimmed, marked "special pot — not counted"), there is a separate *Special
expenses (not counted)* tile, and */ausgaben* shows "◆ 9,000 € from special
pots, not counted". A figure that vanishes silently looks like a bug. Filter
explicitly on the category and you get its full sum.

The rule lives in **one** place (`Database.sonder_sql`) and every reader asks
it — the eleven places that add up money had already drifted apart three
times this week.

The probe asks the property rather than auditing eleven call sites: mark a
booking, and every counted total must drop by exactly its amount while the
separately reported figure rises by exactly the same amount. A number that
stays unchanged is as much a failure as one that moves wrongly. 33 checks,
including a browser pass over all three pages with real bookings — the mobile
probe visits those pages without data, where "no errors" only means "nothing
ran".

## [0.89.6] - 2026-10-04

### Fixed

🔴 **The navigation bar wrapped: "Fixkosten" sat alone on a second line.**
Eight destinations do not fit next to the logo and the controls. Measured at
1536px — where the bar is capped, so a wider screen does not help — as what
the links need against what they have:

```
de  879 / 888  →   +9 px      fr  938 / 892  →  −46 px, TWO lines
en  791 / 922  →  +131 px     es  803 / 941  →  +138 px
                              it  792 / 902  →  +110 px
```

German fitted by **nine pixels**, which is the difference between two font
stacks, not between right and wrong — in a real browser it wrapped. French had
been wrapping since 02.10, in the probe itself, and no check ever went red:
the header probe guards the Upload button and never counted the links.

Five of the eight destinations are finance pages. They are one menu now —
Finanzen ▾ with Übersicht, Rechnungen, Ausgaben, Buchungen, Fixkosten — using
the same click-to-open pattern as the language and account menus beside it.
The bar now has 380–538px of headroom in every language. The strip between md
and 2xl and the mobile sheet still list all eight side by side; there is room
there.

🔑 The header probe now measures the property that was missing: every
top-level destination on **one** line, in all five languages, **with a
reserve** — a layout that fits by nine pixels is not a layout that fits. It
also clicks the menu open and checks that all five pages are reachable.
Counter-tested against the previous markup: four checks go red.

🔴 `rotate-180` (the chevron) was not in the built stylesheet. Rebuilt
and compared class by class: exactly one added, none lost.

## [0.89.5] - 2026-10-04

### Changed

**The category list is alphabetical now — in every dropdown.** It used to come
in the order of `categories.yaml`, with anything added by hand appended at the
end. With seventeen entries that means scanning the whole list, and a category
you created yourself sat wherever it happened to land rather than where you
would look for it.

🔑 **Sorted by the label, not by the stored name.** Those are not the same
string: the file knows `Behoerde` and `Auto`, the dropdown shows *Behörde* and
*Verträge* — and in English *Authorities* and *Vehicle*. Sorting the names
would have produced a correct-looking German list by accident and an English
one beginning Vehicle, Banking, Authorities, which is no order at all. So the
list is sorted by what is actually on screen, per language, and umlauts count
as their transliteration (*Behörde* between *Bank* and *Bildung*, not after Z).

The order is decided in one place (`kategorien.sortiert`, applied by
`kategorien.zusammen`), so the dropdown, the subcategory list, the model's
system prompt and the check on save all agree. `/api/categories` answers in
the same order as the page that calls it — otherwise the list would jump once
the moment you added a category.

Subcategories are sorted the same way. The finance categories are a separate,
deliberately grouped list and are unchanged.

## [0.89.4] - 2026-10-04

### Fixed

🔴 **The merge button did nothing, on exactly the groups a person is supposed
to decide.** A group of two names shows no chooser — the card *is* the
decision. But only what differs purely in spelling is ticked in advance. So for
a pair separated by an actual word (*notar*, *allgemeine lebensversicherung*)
nothing was selected **and there was no way to select anything**: the button
stayed disabled forever: a button that can be pressed without anything
happening.

There is one place now that answers what would be merged, and with a single
alternative it is that alternative, always.

🔴 **And a disabled button looked exactly like an enabled one.** None of the
four button classes had any `disabled` styling — in **36 places** across the
interface. That is the worst kind of feedback: none at all. Somebody who
clicks and sees nothing concludes the program is broken, and they are right —
a button that does nothing is either broken or badly labelled. They are faded
and carry a not-allowed cursor now; no `pointer-events: none`, so the tooltip
that explains why still works.

### Changed

The mobile probe **clicks** instead of reading source. Every source-level check
was green through all of this, because they compare strings and nobody pressed
anything. It now opens the dashboard, finds every merge button and asserts that
none of them is disabled with no way to change that — and the fixture contains
exactly the shape that failed: two names separated by a word.

## [0.89.3] - 2026-10-04

### Fixed

**The tick on the deadlines card was a pill, not a square** — in every row, and
different in height from the paid state right beside it, which is a `<span>`
and therefore kept its shape. The cause is the one found in 0.89.2: the house
sets a 44 px minimum on buttons for thumbs, and a 20 px box declared as a
button gets stretched to meet it. This was predicted while fixing the sender
card, then confirmed by rendering the deadlines card and looking at it.

The row now carries `data-tiny`, the house's own exemption from that rule, and
the button wears its padding on the outside (`-m-2 p-2`) with the border on a
span inside: a 36 px target, a 20 px square. The card is 33 px shorter.

🔴 **And the stylesheet needed rebuilding, which the check caught first.**
Three of the classes used for it — including the negative margin — were not in
the built sheet, so they would have done nothing at all and the square would
have been wrong in a different way. Every class on the card was checked against
the built sheet before looking at the result; after the rebuild, the two sheets
were diffed to prove nothing in use had been dropped (two classes disappeared,
both genuinely unused).

## [0.89.2] - 2026-10-04

### Fixed

🔴 **The new sender card did not fit on a phone — and then it was still ugly.**
The merge button carried the full sender name, which ran to 548 px inside a
390 px screen with the button beside it off the edge entirely. Reported with a
screenshot rather than by a check, although the same thing had been measured
once before with a throwaway script that is long gone.

Fixing the overflow was not enough, and the second screenshot said so. The card
has been rebuilt against the house's own pattern:

- The button no longer repeats the name that is on the line above it in bold.
- The native checkbox was the loudest thing on the card and read as *done*
  rather than *chosen*. It is a small square now, the same one the deadlines
  card uses.
- **A chooser only appears when there is something to choose.** With exactly
  one other spelling the card *is* the decision, and a box in front of it is a
  question already answered — which is how ten of twelve groups look.
- 🔴 The whole row is the touch target, not the square. The house sets a 44 px
  minimum on buttons for thumbs; an 18 px box declared as a button gets
  stretched to 44 px and looks like a pill. (The deadlines card has exactly
  that flaw.)
- 🔴 `h-[18px]` was **not in the built stylesheet**, so the class did nothing,
  the square had no size and collapsed onto its content: an empty box rendered
  as a dot. `h-5 w-5` is there. Every class on the card was then checked
  against the built sheet.
- An empty box draws no tick at all rather than a transparent one, the two
  actions sit side by side instead of stacking full width, and the list is
  capped with its own scroll so twelve groups do not push the rest of the
  dashboard off the screen.

🔴 **Two TypeErrors on every single page load, behind a hidden box.** `x-show`
hides an element; it does **not** stop the expressions inside it from being
evaluated. The dashboard read `sys.host` and `sys.kerne` while `sys` was still
`null` — and a third, `lokalFortschritt.prozent`, did the same. None of them
broke anything visible, which is exactly the problem: errors nobody sees bury
the ones you need to see. The getter now returns an empty object and the
*is it there* question is asked separately, which is one place instead of
optional chaining in a dozen bindings.

🔴 **Four English strings on a German page.** The *re-classify everything in
review* button, its confirmation, its progress label and its busy warning were
hardcoded. They are translated in all five languages now, and the button row
wraps instead of running off the screen.

### Added

`pruefstaende/probe_handy.py` — fifteen pages at 390 px and 360 px, with data
shaped like a real archive (the longest sender names are copied from one, and a
subject with no spaces at all, because scanners produce those). It asserts one
property: nothing visible reaches past the edge of the screen and the page
cannot be pushed sideways, except where scrolling was chosen on purpose. It
also listens for browser errors, which is how all three TypeErrors above turned
up.

## [0.89.1] - 2026-10-04

### Fixed

🔴 **0.89.0 claimed two insurance branches would stay apart. They did not.**
The probe asserting it used names typed out of a truncated console listing
rather than taken from the data. The real names are longer, and the real
distance is 0.92, not 0.80 — so *Sparkassen-Versicherung Sachsen
Lebensversicherung AG* and *… Allgemeine Versicherung AG*, two separate
companies, landed in one group. The check was green for the wrong reason: a
probe that rebuilds its subject instead of taking it measures something else.

**Raising the threshold does not help.** On the same archive, *Verti
Versicherung AG* / *Verti Versicherung* also scores 0.92 and *ERGO
Lebensversicherung* / *… AG* scores 0.94 — and those **are** the same company.
No number separates the two cases, because the difference is not in the
distance but in **what** differs: a legal form is not another sender, a
different line of business is.

So that is what gets measured now. With case, accents, punctuation, German
transliteration and the legal form taken out, anything left over is a word
somebody chose — and each group says which word it is: *ihre*, *detlev*,
*notar*, *allgemeine lebensversicherung*. Groups that differ only in spelling
are marked safe and sort to the top; the rest ask to be looked at. In the
dashboard card every spelling now has its own checkbox, ticked in advance only
where nothing but the spelling differs.

**And the comparison learned German.** Stripping accents turns *ä* into *a*,
which does not match *ae* — the most common German spelling variant there is.
Thirteen documents of the owner's own bank were being reported as a different
sender for exactly that reason. The filename slug has always transliterated;
the comparison now does too.

## [0.89.0] - 2026-10-04

### Added

**The same organisation, filed under two names.** Measured on a real archive
of 853 documents: *Ostsächsische Sparkasse Dresden* (271 documents), the same
bank written without the umlaut (10), and the same bank with a word in front
(3) stood side by side. Filtering by the common spelling hides thirteen
documents of your own bank — and nothing tells you, because a list never says
what is missing from it. Five such groups in total, nineteen documents.

The dashboard now shows those groups when there are any, and merging them onto
one spelling takes a click. It renames the **files** too: the sender is part of
the filename, and two truths about what somebody is called were the whole
point.

🔴 **Nothing is merged on its own.** Two insurance branches of the same company
sit close together and are still two senders — on the real data,
*Sparkassen-Versicherung Sachsen Lebensversicherung* and *… Allgemeine* score
0.80, and a company and its parent (*DekaBank* / *DekaBank Deutsche
Girozentrale*) score 0.42. The measurement finds candidates; a person decides.
Merging is admin-only: it rewrites many documents at once.

🔑 The merge uses a **narrower write path** than editing a document by hand.
`update_metadata` sets the status to *filed*, because a human has just looked
at the document — but nobody looked at anything when a spelling was aligned, so
a document waiting for review has to stay waiting.

The question *are these two names the same thing written differently* now has
exactly one answer (`aehnlichkeit.py`), used both for senders and for category
names. Two thresholds for one question drift apart eventually.

## [0.88.1] - 2026-10-04

### Changed

**The AI badge in the header said the same thing three times when idle.**
*AI idle*, *calls in flight: 0*, *waiting: 0* — three lines for "nothing is
happening". A counter that only means something while work is running now
appears only while work is running.

What stands there instead is the question that has mattered since documents
started being distributed across machines: **which machine is computing, and
how fast is it**. The dashboard shows that on its machine cards, but this badge
hangs in *every* page, and there it was the one place the answer was missing.
Each compute location gets a line with a status dot, its name, and either how
long it has been working on the current document or its measured seconds per
document.

The pulsing dot now also reacts to a compute location being busy, not only to
calls started by this process — with several machines in play, work happening
elsewhere is still work.

The endpoint behind the badge does **not** measure over the network for this.
It is fetched by every open page every three seconds; the figures come from
`ai_pool.stand()`, which reads only what is already in memory — measured
durations, occupied gates, work in flight. Probing the machines from here would
have been traffic with no occasion.

## [0.88.0] - 2026-10-04

### Added

**Categories can be created by hand — in every place that offers them.**
Until now they lived only in `categories.yaml`: needing a new one meant editing
a file inside the container and restarting. Now there is a *+ New category …*
entry in the document editor (for categories **and** subcategories) and in the
library's bulk re-file menu, and what you type is available immediately —
including to the model.

That last part is the whole difficulty. A category is read by five things, and
all of them have to learn about it at once:

1. the dropdowns,
2. the check when saving — otherwise the server rejects what it just offered,
3. the folder path on disk,
4. **the classifier**, which keeps the allowed names, the allowed
   subcategories and its system prompt as snapshots taken in its constructor.
   A category it does not know, it discards as unknown — so the interface
   would be offering a drawer that nothing is ever filed into,
5. and **every** compute location, not just the active one. The others sit
   built and ready to take over the moment the active one is slow or gone; with
   a stale list the same document would land somewhere else over there.

The built-in list from the file and the hand-made list from the database are
merged in exactly one function, which all five read.

**The model may propose new categories.** When nothing fits, it can name one —
and a human confirms it once. After that it is an ordinary category the model
uses on its own. It is deliberately not allowed to create them outright: a
category here is not a label but a **folder name on disk**, and a model free to
open its own drawers produces "Insurance", "Insurances" and "Policy" inside a
week. A proposal is held to exactly the same standard as typed input, so a
near-duplicate is turned down the same way.

Names are checked before anything exists: no path separators or characters that
break a filename, nothing that is a device name on Windows, nothing longer than
will fit a folder, and nothing within a measured similarity of a name already
there. Proposals appear on the dashboard only when there are any.

Removing a category is the admin's business and never touches documents — the
answer says how many still carry it. Taking a drawer out of the list and
re-filing what was in it are two different decisions.

### Privacy

🔴 **Twenty-five verbatim remarks by the owner were sitting in published
source.** The repository is public, and the rule against quoting a person in it
has been guarded for a year — but the guard only recognised a quote that
carried a label in front of it (`Word: "…"`). A quote standing bare on an
indented line matched nothing, so six modules kept them, reported green the
whole time. All of them are now reported speech: the reason a thing was built
survives, the voice does not.

The check was then widened to bare quotes — and the first counter-test for the
new rule **stayed green**, which is how a guard that guards nothing announces
itself. The word list it used to tell a sentence from a quoted term had been
written for formal prose: it knew *und*, *nicht*, *werden*, but not *ich*,
*will*, *mal*, *man*. A request is phrased in exactly those. A probe that does
not speak the language of its subject measures nothing.

Fixing that revealed the matching trap in the other direction: several of those
informal words are also ordinary English (*was*, *will*, *man*, *hat*), and
adding them made the English changelog read as German prose. There are two
questions here, so there are now two lists — *is this file German* answers only
with words English does not share, while *is this quotation a sentence* may use
all of them.

The scan also asks **git** which files are published rather than walking the
tree: `pruefstaende/` is excluded from the repository and never shipped, and a
probe that flags findings nobody can see teaches people to ignore it.

## [0.87.3] - 2026-10-04

### Fixed

**The duplicates tile counted one thing and linked to another.** Its number was
the count of rows with `status='duplicate'` — a note from the past, *you
uploaded this file again* — while the tile links to `/duplicates`, which lists
something else: files that are byte-identical **now**. The two can drift apart
without limit. On a live archive the tile said 3 and the page behind it was
empty: a dashboard advertising work that does not exist, and a dead end for
whoever clicks it.

The three rows it was counting had no file on disk and no twin at all, so they
were not duplicates of anything. The big number is now what `/duplicates`
lists, from a single `WHERE` clause shared by the count and the listing, with
the re-upload count kept as a sub-line (the library's own sidebar already calls
those *re-uploaded*, which is what they are).

**A deadlock on the dashboard, caught before it shipped.** The first version of
that shared count took `db._lock` itself. One of its two callers already sits
inside a `with db._lock:` block, and the lock is a `threading.Lock`, not an
`RLock` — taking it twice blocks forever. The dashboard polls that endpoint
every two to three seconds, so the app would have choked on stuck threads
within minutes of a page being left open.

The probe did not go red for this; it **stopped**. A check that hangs reports
nothing, so that one call is now made with a time limit and the probe says so
out loud when it is exceeded.

## [0.87.2] - 2026-10-04

### Fixed

`db.list_documents` still defaulted to `doc_date` while `/library` passed
`created_at`, so the data layer and the page it feeds held two different ideas
of "default order". The point of 0.87.0's single constant was to stop exactly
that — and the second answer caught the author within minutes: a check written
to confirm the new ordering called `list_documents` without `order_by` and read
the old one back, reporting a September document at the top of an archive whose
newest arrival was from October.

Both the parameter default and the `ORDER BY` fallback for an unknown sort key
now name `LIBRARY_SORT_DEFAULT`. The three callers that take the default
(export, empty-trash, backfill) do not depend on order at all.

## [0.87.1] - 2026-10-04

### Fixed

The ✕ on an active filter removed that filter **and** the sort and both date
ranges with it. The breadcrumb spelled its own link out by hand and so knew only
the three filters that existed when it was written — the last place in the
library still carrying its own idea of what a view is made of. It reads the
route's slice now, minus the one key it drops.

## [0.87.0] - 2026-10-04

### Fixed

🔴 **One click on one duplicate group trashed every group.** The per-group
button sent `{"keepers": {"<hash>": <id>}}` and meant *only this group*. The
all-groups button sent `{}` and meant *every group*. The endpoint read a
missing hash as "no keeper picked, so keep the oldest and trash the rest" — so
a missing key meant two opposite things and the server could not tell which.

Measured in the live database: **127 documents trashed in one minute** from a
single click. Nothing was destroyed (they were content-identical copies and the
delete is reversible), and all 127 have been restored, but it was not what the
visitor asked for.

The keys of `keepers` are now the scope. A group that is not named is not
touched, and a request that names nothing does nothing.

🔴 **The trash counted 161 and listed 33.** A `duplicate` row is a note
("you uploaded this file again"), and keeping those out of the library is
deliberate. The rule also applied to the trash — where it swallowed the 128
rows somebody would come to the trash to put back. A counter that promises more
than the list delivers is worse than a long list: it hides that the thing
exists at all. The rule now applies to the library only.

🔴 **A document whose file was already gone could never leave the trash.**
`delete_document` has a branch for that case: it flags the row and moves
nothing. `restore_document` had no matching branch — it demanded a file under
`_Trash/` and raised otherwise, so the restore button answered with an error
every time, for good. Restore now mirrors delete branch for branch.

🔴 **Leaving a document dropped you into the whole archive.** Opening one of
the 33 under review and then saving or deleting it landed you in the full
library instead of back in the list. The filter was handed to the document page
on the way in and ignored by every way out: the back link rebuilt a path from
the document's own fields, the save redirect dropped the query string, and
delete was hardcoded to `/library`.

A view is a **slice** — filter *and* order — and it is now built in one place
(`_slice_qs`) that the cards, the back link, the edit form and the trash
buttons all read. Saving a document that left the slice returns to the list,
one entry shorter, because a document that is no longer in the list has no
neighbours there.

🔴 **The browser's back button landed on a bare grid of cards.** Changing a
filter is an htmx request against `/library?partial=1&…`, which answers with
the card grid alone — and `hx-push-url="true"` put that request URL into the
browser history. Navigating back there got a fragment with no header, no filter
bar and no navigation. The URL to push now comes from the route as an
`HX-Push-Url` header and is always the address of a page.

A money formatter no longer takes a page down: `cost_usd` is NULL on rows older
than cost tracking, and both `usd` and `eur` raised on it mid-render. They
answer `—`, like every other money field. `eur` also formats German now — it
printed `0.93 €` next to `1.038,47 €` on the same line.

### Changed

- The library opens sorted by **scan date**, newest first. An archive fed by a
  scanner is navigated by when something arrived; `doc_date` is missing or
  plain wrong on many old scans, which scattered them through the grid. One
  constant (`LIBRARY_SORT_DEFAULT`) answers for the grid and for the ←/→ keys,
  so the position counter can no longer describe a different list than the one
  on screen.
- `siblings_of` takes the sort and the date ranges, not just the filters.

### Added

- `pruefstaende/probe_steuerung.py` — 49 checks over a real app run: the trash
  lists what it counts, one click clears one group, the slice survives every
  exit, the arrow keys walk the grid's order, the pushed URL is a page, and
  delete/restore are inverses. Each fix was reverted in turn and the probe
  confirmed red.

## [0.86.6] - 2026-10-04

### Fixed

🔴 **A word in one field threw away the whole classification.** The
instructions ask for `confidence` as a number between 0 and 1, but local models
happily answer `"high"`, `"low"` or `"LO"` — and `float("high")` raises. The
exception was caught upstream and the document was filed as *Sonstiges /
Unbekannt / 0.00*, **even though category, sender and date were all there**.

Measured while importing 128 documents: **11 of them**, one in twelve. A
complete classification discarded over the spelling of a single field — and the
least important one at that, since confidence only decides whether a human
takes a second look.

Words are now read as what they are: *approximately*. `high` lands **below**
the auto-file threshold, so the document still goes to review — but with its
category, its sender and its date. No number is invented that pretends the
model gave one.

Also normalised along the way: `85` and `"85"` now mean the same thing (85 %).
The percent rule previously applied only to the string path, so the value
depended on whether the model happened to put quotes around it.

## [0.86.5] - 2026-10-03

### Fixed

🔴 **`/api/status` documented a state it never returns.** The list named
`done`; the endpoint hands back the document row's own status, and there it is
`filed`. `duplicate` was listed but easy to miss among the others.

A docstring is a contract as soon as somebody builds against it. A machine
client written from this one waited for states that cannot occur and sat out
its own per-document timeout on **every** file — measured on one install: 60
minutes each, for a batch of 128 documents. The upload page had it right all
along, because it was written against the running system rather than against
that paragraph.

The list now separates "keep polling" from "stop polling", says that `unknown`
is briefly normal right after an upload and must not be treated as final on
first sight, and the probe checks the listed states against the ones the upload
page actually treats as terminal.

### Changed

**The activity card no longer jumps, and is never empty while work is
running.** Two defects at one box:

- **A gap nobody had thought about.** The work list appeared when there were
  work rows, the idle text when nothing was active. In between sits a state
  where a file is in the inbox but its row is not written yet — neither
  condition matched, so the box was blank exactly while work was happening. It
  now says what there always is in that moment: how much is waiting.
- **The idle block was three times the height of a work row**, so during a
  batch import the box — and the whole page below it — resized every few
  seconds.

One container now holds all three states and exactly one of them is ever
shown. 🔴 A *minimum* height was not enough — it stops the box shrinking, not
growing, and the pipeline runs up to four documents at once. The height is
fixed at two rows and scrolls beyond that, so an empty box and a busy one are
the same size. Two rows rather than one because the common case should not
show a scrollbar.

## [0.86.3] - 2026-10-03

### Fixed

🔴 **Receipt and bank-statement extraction bypassed the distributor entirely.**
Both were handed a provider object and called it directly, so they always used
the manually selected machine: no distribution, and — the part that matters —
**no fallback**. If that machine was down, the extraction failed outright while
the classification next to it ran fine on another machine.

It showed up as something that looked like a success: a classification took
858 s on the server while the receipt extraction right after it took 11 s. It
had landed on the laptop, not because anything decided that, but because the
laptop was the one written down.

Both now go through the same selection and the same fallback as a
classification, and the model name is taken from the machine that was chosen
rather than from the caller — otherwise a name could be sent to a machine that
does not have it.

### Changed

**A card no longer says "chosen" while another machine is computing.** While
DocuSort distributes by itself, the selected target is only the hint that
breaks ties, not an instruction. In that mode every card invites a choice, and
pressing one switches to fixed.

## [0.86.2] - 2026-10-03

### Fixed

🔴 **With nothing measured yet, the order in the configuration file decided
where a document went.** All machines start from the same neutral assumption,
so `min()` simply took the first one in the list. On an install where `nas` is
written above `mac`, the first document after a restart would go to the slow
machine — fourteen minutes instead of eleven seconds.

The order of lines in a file says nothing about speed. The machine the user
chose does. A tie is now broken in favour of the selected target, which at the
very first document is the only hint that exists at all. A real measurement
still wins over the hint — the preference decides ties, nothing more.

🔑 This is the other half of the fix in 0.86.1. Remembering the timings helps
from the *second* start onwards; on the first there is no file yet. Together
they cover both.

🔴 **The page stated two different truths about where the model runs.** The
laptop card said *chosen*, while the badge at the top of the page showed the
server's address. Both numbers were right on their own and useless together.

Since the switcher exists, `settings.ai` means only the *fallback* — the
provider, model and address from the configuration file that apply when no
named target fits. What actually computes is known only to the running
classifier. Four places had not been told:

- **The badge on the home page** read the address out of the file.
- **Four calls built `Extractor(classifier.provider, settings.ai.model)`** —
  the running provider next to the configured model name. If the active target
  points at a different machine with a different model, a name is sent to a
  machine that does not know it.
- 🔴 **Whether a bank statement may leave the house** (`finance.local_only`)
  was decided from `settings.ai.provider`. Switch to a cloud provider while the
  file still says `openai_compat`, and the statements would have gone there —
  with DocuSort reporting that it was computing locally.
- **Whether the bridge is the active provider** was answered from the file too.

One helper, `ai_targets.aktive_ai()`, now answers "which settings are actually
in use", and every place describing the running state asks it. Places that
describe the *configuration* — the setup wizard, the settings page — keep
reading the file, because that is what they edit.

## [0.86.1] - 2026-10-03

### Fixed

🔴 **What the installation had learned was thrown away on every restart.**
The per-machine timings that decide where each document goes lived only in
memory. After every update — and the updater runs hourly — the first document
fell back to a blind assumption and went to whichever machine came first in
the list. At 11 s against 835 s that is the difference between eleven seconds
and fourteen minutes, once per update, forever. The cards on the dashboard
also claimed "not measured yet" about machines that had been measured a
hundred times.

The timings are now kept in `ai_tempo.json` next to the configuration and read
back at startup, so the first document after a restart goes to the right
machine.

Written at most every 20 seconds (a document takes longer than that anyway),
and written to a temporary file that is then renamed — a crash mid-write would
otherwise leave half a file that nothing could read. A missing, empty or
malformed file is not an error: it is exactly the state of a freshly cloned
repo, and then the timings are simply measured again. Implausible values
inside the file are dropped rather than trusted.

## [0.86.0] - 2026-10-03

### Added

**DocuSort picks the machine for each document — and uses several when there
is enough work.**

What was asked for: when one machine is away, compute on the other; when it is
back, use it again; and with enough documents, use both.

🔑 **The rule is not "spread the load". It is "send each document where it
will finish soonest."** The difference is the whole point. Measured on one
install: 11 s per document on a laptop, 835 s on a NAS. When a second document
arrives while the laptop is busy, the laptop is *still* the right answer —
11 seconds of waiting plus 11 seconds of work beats 835 seconds by a mile.
A balancer that counts idle machines sends it to the NAS and makes it
76× slower. The slow machine joins in by itself only once the queue in front
of the fast one is longer than one round on the slow one.

So one rule answers all three cases in the request: a machine that stops
answering is not eligible and gets nothing; when it answers again it wins the
next document; and with enough documents several machines compute at once.

- **The estimate comes from measurement, never from hardware.** Seconds per
  document, the median of this installation's own real classifications. Not
  from cores or RAM — the NAS in the example has *more* RAM than the laptop
  and the same number of cores. A freshly cloned repo has no measurement and
  starts from a neutral assumption that corrects itself after one document.
- **Failure no longer strands a document.** If a machine refuses, the document
  goes to the next one. Only when none can does it raise the transient error
  that leaves the file in the inbox for a later retry — the behaviour that was
  already there, now reached only as a last resort.
- **One machine, one document at a time.** Two simultaneous requests to the
  same Ollama are both roughly twice as slow, and they make the timing
  unusable — which is what the whole decision rests on. The parallelism is
  *between* machines.
- **Waiting is not counted as computing.** The clock starts after the gate.
  Measuring the queue would make a popular machine look slow, so it would get
  less work, so it would look fast again — a pendulum instead of a measurement.
- **Choosing a machine by hand switches to "fixed".** Otherwise the page would
  have two hands on the same wheel: you press *MacBook Pro* and the next
  document goes elsewhere anyway. The switch next to the cards goes back to
  automatic. Even when fixed, a machine that drops out is bypassed.
- **Batches may now run in parallel.** Draining the inbox at startup and the
  periodic retry sweep were strictly sequential, which made any distribution
  pointless — a second machine only gets work if a second document is in
  flight. Text recognition keeps its own, unchanged limit (`ocr.max_parallel`);
  it is memory-bound and always runs on this machine.

Configurable as `ai.verteilen: auto | fest`. Default `auto`; with a single
machine it makes no difference.

### Changed

**The machine card says whose numbers it is showing.**

The question that prompted this: are those CPU, memory and inbox figures the
ones from the container the model runs in — and shouldn't the page always show
the history of whichever machine is currently active?

Right on both counts. The three graphs belong to the machine DocuSort runs on
— and that is correct for them, because text recognition, the inbox and filing
all happen there regardless of where the model lives. They now carry that
heading instead of leaving it to be guessed.

🔴 **What DocuSort cannot do is measure a foreign machine's CPU and RAM.**
Nothing of ours runs there, and nothing should have to be installed — that is
a promise, not convenience. An invented percentage would be worse than none.

🔑 **What it does know about that machine is the more useful half**, and each
compute location now has its own card showing it:

- **Graphics card or main processor.** Ollama reports `size_vram`; the share of
  the model held in graphics memory is exactly what separates ten seconds from
  fourteen minutes.
- **Seconds per document**, with a sparkline of the recent real
  classifications on *that* machine. An unmeasured machine says so rather than
  claiming a number.
- **What it is computing right now**, since when, and how many documents are
  waiting for it.

The separate strip of target buttons is gone — it said the same things with
less. One block now carries the state, the speed, the work and the buttons.

## [0.85.3] - 2026-10-03

### Fixed

🔴 **The network scan searched the tunnel instead of the house.** It took the
subnet of whichever address the browser arrived from. Open DocuSort over
Tailscale and that is a `100.64/10` address — so the scan swept an overlay in
which every machine stands alone, and the machines actually sitting in the
house were never found. Reported as: press *Also scan the network* and it
finds nothing.

The subnet is now taken from the first source that can actually describe a
real network, in this order:

1. the browser's address — **unless** it is a Tailnet/CGNAT or Docker address,
2. the machines already configured as targets: an entry pointing at
   `192.0.2.5` says the house is `192.0.2.x`. That is an existing fact rather
   than a guess, and it is exactly what helps when the browser arrives through
   a tunnel,
3. our own address, unless that is a Docker one.

If none of them yields a real network, nothing is scanned — better nothing
than the wrong network. This is the same mistake as the Docker one fixed in
0.84.0, in a second place: an address that works between two machines does not
describe where "here" is.

## [0.85.2] - 2026-10-03

### Fixed

🔴 **Setup wrote an address DocuSort could not reach, after downloading
4.7 GB.** The setup script asked its own routing table which of its addresses
would be used to reach DocuSort, and sent that. From that machine's point of
view the answer was correct — and still wrong: DocuSort had been opened over
Tailscale, so the routing table answered with the machine's *Tailscale*
address. DocuSort runs in a container, and a container does not reach the
host's Tailnet; only the host does.

Measured on a real run: Ollama installed, bound, 4.7 GB pulled, speed measured
at 125 tokens/s — and then `Connection timed out` at the handover, with
everything on that side working perfectly. The address was written anyway, so
the install was left pointing at something unreachable.

**Only DocuSort can answer this**, because it is the one that has to reach the
other machine. The setup now sends *every* address it has — best guess first,
Tailnet and CGNAT addresses last, since those often work between two machines
and specifically not out of a container — and DocuSort tries them in order and
keeps the one that **answers**. If none do, the error names every address that
was tried, because somebody who just waited for gigabytes should not have to
guess.

- New `local_ai.antwortet()`. 🔑 "Does it answer?" is not the same question as
  "does it have models?": `models_at()` returns an empty list for *both* a
  dead address and a freshly installed Ollama with no model yet. Using it to
  pick an address would declare a working Ollama dead.
- **An unreachable address is still saved, but never becomes the active
  target.** Somebody who just waited for gigabytes should not lose the setting
  — the machine may come back in ten minutes. But activating it is what left a
  real install unable to classify anything at all. It is written as a target,
  reported as `verified: false` with the reason, and the install keeps
  computing where it can.

## [0.85.1] - 2026-10-03

### Fixed

🔴 **Removing your second-to-last machine took the search buttons with it.**
*Find machines* and *Also scan the network* sat in the same block as the
switcher, and the switcher only appears when there is more than one machine to
switch between. Remove one and both disappeared — leaving no way back except
editing `config.yaml` by hand.

They are two different questions. Switching is only worth offering with more
than one machine; **searching is worth offering especially when there is only
one, or none.** The search now has its own condition: can this account see the
list at all.

🔴 **"This machine" was showing a container ID.** The machine card read the
hostname, and inside a container that is the container ID — twelve hex
characters that match no device anybody owns. The card said
`THIS MACHINE · 41d48087aee1`.

It now reports the host's DMI product name where there is one (`DS1621+`),
which is readable from inside a container because it comes from the kernel
rather than the filesystem — and which names the box somebody actually has on
a shelf. Failing that, the hostname, unless it looks like a container ID.
Failing that, nothing: an empty field is more honest than a number pretending
to be an answer.

## [0.85.0] - 2026-10-03

### Added

**Install it, open it, press one button — everything runs on your own
machine.** You do not need to know that a local model is possible, what Ollama
is, or what a model is called. The start page offers it, and DocuSort does the
rest.

- **The offer appears by itself** on the start page — but only when there is
  genuinely something to offer: a reachable model service *and* enough memory.
  A button that can only disappoint is worse than no button.
- **It says what your machine will do before anything is downloaded.** Memory,
  cores, GPU, the size of the one-time download, and — where it applies — that
  this box will need minutes per document rather than seconds.
- **One click fetches the model with a progress bar.** Several gigabytes
  without feedback are indistinguishable from a hung program, so the download
  streams its progress (`stream: true`), shows percent and bytes, and appears
  in the work list. You can leave the page; it keeps going. When it finishes,
  the model is written to the config and the running classifier switches to it
  — no restart.
- **The bundled `ollama` service now runs from the start.** It used to sit
  behind `profiles: ["ki"]`, which only started with
  `docker compose --profile ki up` — a command nobody installing DocuSort for
  the first time knows, and which appeared nowhere on their path. A finished
  local model was one command away and never used. 🔴 DocuSort cannot start
  that container itself: it deliberately has no access to the Docker socket,
  which would be root on the host. So the service has to be running for the
  button to have anything to talk to; what the button then does needs no
  Docker rights at all.

### Changed

🔴 **Memory and cores are not a verdict about speed.** The check called a
machine "good" whenever it had enough RAM. Measured on a Synology DS1621+:
32 GB, 8 cores — "plenty" by that rule — and 5.6 tokens/s, about 14 minutes
per document, where a Mac with Apple Silicon takes 10 seconds. The reason is
the CPU, not the memory: a power-sipping embedded part with no graphics unit.

`docusort/hardware.py` (new, shared by the app and the setup script) now reads
the host's DMI product name — **readable from inside a container**, which is
how the Synology model surfaced at all — and the CPU model, and reports the
kind of machine. A device built to sit in a cupboard around the clock is said
to be slow *before* the download, not discovered to be slow afterwards.

- The memory check now prefers a cgroup limit over `/proc/meminfo` when one is
  set: in a container, `/proc/meminfo` describes the *host*, and a model that
  fits the host but not our limit gets killed by the kernel.
- No vendor list — the property is what matters, and it is measured.

## [0.84.0] - 2026-10-03

### Changed

**One state, two views — the settings page and the start page no longer
disagree.**

There were two worlds describing the same thing. The settings page wrote the
*configured provider* and said a restart was required; the start page switched
between *named machines* and needed none. Pick a machine in one place and the
other did not know about it. Which one was right was anybody's guess.

- **Choosing a local model in Settings now also switches, immediately.** The
  machine you pick is added to your list of machines (when you keep one) and
  becomes the active one without a restart — the running classifier really
  follows, it is not just mirrored for display.
- **Saving the AI provider no longer claims a restart is needed.** It
  switches the running classifier instead. The flag is still returned
  truthfully: if the switch itself fails, or there is no classifier yet
  because nothing has been set up, the answer says a restart is required —
  because then it is.
- A machine already in your list is matched by address rather than added a
  second time, and its model name is updated if you picked a different one.

**`ai.timeout_seconds: 0` now means no time limit at all — let it compute.**
On a local model, time costs nothing but time, and aborting just before the
answer throws away the entire computation. Measured on a Synology without a
GPU: one document ran past twelve minutes, and that was not a fault, it was
the speed of that machine. Only the wait for the answer is unlimited — a
machine that is not there still fails at connection time, so nothing hangs
forever on a host that does not exist. The floor that raises short timeouts
(`max(timeout * 3, 600)`) no longer swallows the zero: a setting that looks
like it does something and does nothing is worse than no setting.

**The settings page no longer carries a second model search.** It could set
exactly one machine, overwrote the configured provider and asked for a
restart; the start page does the same thing better. What stays in Settings is
what has no second home: the provider and its API key, and the installer for a
machine with no Ollama at all. The restart button only appears when a restart
is genuinely pending.

**The first-run setup can find a machine instead of asking you to know its
address.** That page offered an empty field — on a fresh install, at the very
first step. It now has the same search, including the deliberate network scan.

🔴 **The network scan searched the wrong network.** DocuSort usually runs in a
container, whose own address is on the Docker network (somewhere in 172.16/12).
Scanning "my own /24" from there searched the Docker network and found exactly
one thing — the gateway, i.e. its own host, shown as a bare address that
matches no device anybody owns. Machines on the actual house network were
never found. The scan now uses the network of the *browser* that asked, which
is the one reliable statement about where "here" is, and each find carries a
host name where DNS knows one.

## [0.83.0] - 2026-10-03

### Added

**One button finds the machines you have, and you can get rid of them again.**

Setting up a local model used to mean knowing an address and typing it in.
Now the machine card has a *Find machines* button:

- **Without scanning**, it asks only the addresses that announce themselves
  anyway — a configured one, localhost, the sidecar container, and the machine
  this browser is on, which is the likeliest place for a local model and the
  one address a browser cannot tell you but the connection already knows. No
  foreign device is touched.
- **Scanning the subnet is a separate, deliberate press.** 254 connections
  into a network look like a port scan from the outside, and in a company
  network that is an incident. It is never done on page load, only the one
  port Ollama uses, never wider than the local /24, and it finishes in about
  a second and a half.
- Each find shows its host, its models and which one would be used. **Add**
  writes it to `ai.targets`; the `/v1` suffix is appended for you. Adding the
  same machine twice gets its own key rather than two identical buttons.
- **✕ removes a machine from the list.** Only machines you added — the derived
  fallback stays, so an install cannot end up with nothing. Removing the one
  that is *currently running* hands over to another target and the running
  classifier really follows; it does not leave the install pointing at
  something that is gone. Nothing is deleted on the machine itself, and the
  confirmation says so, because "remove" could just as easily be read as
  "delete several gigabytes".
- **Models can be deleted from a machine** (`POST /api/ai/target/model/delete`)
  — gigabytes freed without opening an SSH session. The model the active
  machine is *using* is refused with a clear reason: switch first.

### Fixed

🔴 **Model suggestion picked the smaller model when both were present.** The
match fell back to the model *family*: `want.split(":")[0]` turns
`qwen2.5:7b-instruct` into `qwen2.5`, and `qwen2.5:3b-instruct` matches that
too — so on a machine holding both, the one Ollama happened to list first won.
That is not a cosmetic issue. Measured on the same electricity bill through
the program's real path: the 3B model took 186 s and filed it under *Haus*,
the 7B took 465 s and filed it under *Rechnungen*, where it belongs. The
smaller model is not a faster version of the same work, it is worse work.
There are two passes now — exact matches across the whole wish list first,
family matches only if none hit — and `:latest` is treated as the same model
either way. The same bug was in the setup script and is fixed there too.

## [0.82.0] - 2026-10-03

### Added

**Every machine now says what it is doing, and the setup says what your
hardware can do.**

- **A traffic light per machine, measured.** The buttons used to carry a name
  and nothing else: whether anything was answering there you found out when a
  document failed on it. Each target is now probed — side by side, with a
  short deadline so a dead host cannot hold up the page — and reports one of
  four states: *ready*, *model missing*, *not answering*, or *cannot measure*
  (a cloud provider; nothing is claimed about a data centre).
- **The amber dot is gone.** The machine card had exactly two colours, amber
  and grey, and amber meant "fine" — which reads as a warning on a healthy
  install. It is a traffic light now, and every colour has its meaning written
  next to it.
- **Buttons that can actually do something.** *Wake up* appears when a machine
  is ready but the model is cold — a cold model costs about 30 seconds extra
  on the first request. *Fetch model* appears when the machine answers but does
  not have the model. Neither is shown where it would not work, and a machine
  that is not answering cannot be selected at all.
- **A model download shows up in "what's running".** It runs in the
  background and registers in the work list, so the page no longer says
  "nothing is running" while several gigabytes come down the line.
- **You get told when a machine stops answering** — a new `ai_down`
  notification, on by default. Only on a *change*, never on every check, and
  debounced: two consecutive readings must agree before anything is sent. A
  single hiccup is not an outage, and a notification about one makes the next
  one less believable. The first pass after a restart never notifies.

### Changed

**The setup script measures the machine before it downloads anything.**

- **Memory, cores and GPU are measured**, on macOS, Linux and Windows alike,
  and the result decides which model is suggested. A machine without enough
  memory for even the small model is told so *before* several gigabytes are
  fetched — along with what to do instead (point DocuSort at another machine
  on the network, or at a cloud provider). Unmeasurable is not treated as
  insufficient: when the numbers cannot be read, nothing is claimed.
- **And then it measures the real speed.** Ollama reports timings for every
  answer, so the machine is asked rather than estimated from core counts: how
  fast it reads, how fast it writes, and what that means for one document.
  "About 12 seconds here" or "about 25 minutes here" is the single most useful
  thing somebody can know before importing three hundred documents.
- 🔴 **On macOS, Ollama now actually starts by itself.** The script used to
  run `launchctl setenv OLLAMA_HOST`, which lasts until the next reboot, and
  then handed the rest to the reader: "add this to your shell profile".
  Measured on a real machine: after a restart Ollama was not running at all,
  and started by hand it listened on `127.0.0.1` only — so DocuSort on another
  host could not reach it, and all it could report was "not reachable". A
  LaunchAgent now answers both halves: it starts Ollama at login, restarts it
  if it stops, and carries `OLLAMA_HOST` with it so the setting cannot drift
  away from the process it belongs to. Nothing is written outside the user's
  own home and no administrator rights are needed.
- **On Windows, Ollama is restarted after the setting is stored.** `setx`
  writes the value for *future* processes, so the copy already running in the
  tray kept the old one — the setting looked applied and nothing changed until
  the next reboot.

## [0.81.0] - 2026-10-03

### Added

**Pick which machine does the thinking — and switch without a restart.**
An install can have more than one place to run inference: a workstation that
answers in seconds, a server that is slower but always on, a cloud model for
when neither is up. Until now the choice was a setting: you wrote it into
`config.yaml` or the settings page, and the page told you a restart was
required. The running classifier kept the old provider until somebody
restarted the process — and a restart throws away any OCR in flight. For
"just put this one document through the fast box", that was unusable.

Named machines now live in `ai.targets`, and the machine card on the start
page offers them as buttons. Choosing one takes effect on the next document;
nothing restarts.

```yaml
ai:
  provider: openai_compat          # still the fallback at start-up
  model: a-model
  base_url: http://host-a:11434/v1
  active_target: fast
  targets:
    - key: fast
      label: Workstation
      provider: openai_compat
      model: a-model
      base_url: http://host-b:11434/v1
    - key: overnight
      label: Server
      provider: openai_compat
      model: a-model
      base_url: http://host-a:11434/v1
      note: slower
```

Any provider can be a target — a cloud model and a local one side by side is a
perfectly good pair. The API key is looked up per target's provider, so
switching from a local model to a cloud one picks up the right key instead of
the one belonging to whatever is configured as the fallback.

**Nothing to configure, nothing shown.** `ai.targets` is empty on a fresh
clone, and that is the normal case: one provider means there is nothing to
choose, so no switch is rendered. A second target is also derived — but only
when it is *measurable*, never guessed: a bridge appears as a target only
while a bridge client is actually connected. A target that fails the moment
you select it is worse than no target.

What a switch deliberately does *not* change: timeout, text limit and minimum
confidence stay as configured. Those are decisions about the work, not about
the machine.

- `GET /api/ai/targets` lists them, `POST /api/ai/target` switches. Both are
  admin-only, and that needed no work: permissions are an allowlist, so a new
  route is admin-only until somebody opens it deliberately. `/api/dashboard`
  carries only *which* target is running and whether there is more than one —
  never an address, because every signed-in account sees that payload.
- A failed switch leaves the previous target running. An install that cannot
  classify anything is worse than one still using the old machine, and a typo
  in an address should not cause it.
- A malformed entry under `ai.targets` is skipped with a warning rather than
  thrown. A crooked line in a config file must not stop the program from
  starting.
- A switch is remembered in `ai.active_target` alone, so it survives a restart
  without overwriting the configured fallback. Remove the target later and the
  install falls back to what is configured, not to an address nobody has any
  more.
- `pruefstaende/probe_rechenort.py` covers it, including the case that decides
  whether this works for anyone else: a freshly cloned install with no targets
  at all. It also asserts that no address, model name or host name appears in
  the module's code, runs the routes for real against admin, user and deliver
  accounts, and carries counter-tests that must go red.

## [0.80.0] - 2026-10-03

### Changed

**The load figures now say whose load they are.** The machine card read
`/proc`, which describes the host DocuSort runs on — and then put a model name
beside it as if both lived in the same box. On this install they happen to, so
it looked right. On an install whose model sits on a Mac across the bridge, or
in a data centre, it would have been a true number about the wrong machine.

Where the model runs is something only the provider can know, so the answer
moved there: `Provider.runtime()` is part of the interface now.

- `openai_compat` asks the endpoint (`/api/ps` on Ollama) for the loaded model,
  its size, quantisation and context, and reports whether the host is this
  machine or another one on the network.
- `bridge` reports the Mac: its name, its platform, the model it loaded — and
  its **own** CPU and memory, which the Mac client now sends in its hello and
  again whenever work arrives. Nothing else could measure that machine.
- The cloud providers answer `cloud`. There is no local load to show and
  inventing one would be worse than saying so.

The card shows two named blocks — *This machine* with DocuSort and OCR, and
*The model runs on* with wherever that is.

### Fixed

**The bridge client info never showed.** `getattr(bridge, "last_client_info")`
has no such attribute; it returned `None` every time, so the AI chip could
never name a Mac. It reads `bridge.info()["client"]` now.

## [0.79.0] - 2026-10-03

### Fixed

**A reply that is not a classification is now a failure, not a result.** Three
imported documents in a row were filed as *Sonstiges / Unbekannt / Dokument*
with confidence 0.50 and an empty reasoning. Those are exactly the fallback
values in `Classification(...)`, which is what you get when the parsed JSON
carries none of the expected keys. The model had answered with perfectly valid
JSON — of its own schema:

    {"name": "...", "taxIDNumber": "...", "earnings": {...}, "childBenefits": 2728}

It had *extracted* the tax form instead of classifying it, because it never saw
the instruction to classify. Measured in Ollama's own log:

    "truncating input prompt" limit=4098 prompt=15119 keep=4 new=4098

The prompt was cut to its last 4098 tokens, and the system prompt — the
categories, the schema, the instruction — sits at the front. Every
`data.get(..., default)` then produced a confident-looking answer out of
nothing. A fault that reads as a finding is worse than a crash; it is one now:
the reply is rejected, the usual retry runs, and the document goes to review
with the model's own key list in the reason.

**The text limit works again.** The classifier floored it at 200 000
characters, reasoning that a local model costs nothing per token. That ignored
the context window: sending more than fits does not improve the answer, it
removes the question. `ai.max_text_chars` (default 12 000) is the knob again —
raise it for a large window, lower it for a small one.

## [0.78.1] - 2026-10-03

### Changed

**The status tiles moved below the machine plots.** The owner reads the page
top-down for *what is happening right now*; the counts are the slower question
and belong after it. Order only — the same markup, the same figures.

## [0.78.0] - 2026-10-03

### Changed

**The start page says who is doing what, which documents are in which state,
and who is using what.** It previously managed to claim, on one screen, that
nothing was running and that the machine had been working for twenty-one
minutes. Both statements came from honest code; they measured different
things and called both of them *running*.

- **Who is doing what.** The pipeline now registers a stage per document —
  checking, OCR, AI, filing — and the live card lists every document in flight
  with its stage, the tool working on it (ocrmypdf, or the model's own name)
  and how long it has been there. The idle state appears only when the work
  register *and* the inbox are empty, so the page can no longer contradict
  itself.
- **Which documents are in which state.** The tiles used to show a total of
  681 next to a review count of 2, and that 681 silently contained 122
  duplicates. The row now reads filed · review · failed · duplicates ·
  statements, with the total named separately, so the figures add up.
- **Who is using what.** DocuSort and OCR are processes in our own tree and
  are read from `/proc`, split apart by process name. The model runs in a
  different container and is therefore *asked* rather than estimated: Ollama's
  `/api/ps` reports the loaded model, its size and quantisation. An
  unreachable or unloaded model says so instead of showing a dash.

## [0.77.1] - 2026-10-03

### Fixed

**The sampler now says it is alive.** 0.77.0 started a background thread that
wrote nothing anywhere, so from outside there was no way to tell a running
sampler from one that froze an hour ago — the same blind spot that once hid a
stuck poller for twelve hours behind zero log lines. It now logs once at
startup and once an hour with its measurement count.

## [0.77.0] - 2026-10-03

### Added

**The start page now shows what the machine is doing.** The AI badge in the
header counts LLM calls and nothing else. While a scan goes through OCR it
therefore says "AI idle" — and the owner, watching four cores at 400 % in the
Container Manager, reasonably concluded the badge was lying. It was not; it
was answering a narrower question than the one being asked.

A *Machine* card now sits on the dashboard with three small plots over the
last 30 minutes: CPU, memory and inbox depth, with the current numbers above
them, plus load average, free disk and DocuSort's own resident size. When
something sits in the inbox the card says so with a pulsing dot and how long
it has been there — that is the honest answer to "is anything happening?",
whichever stage the pipeline is in.

- `system_stats.py` samples `/proc/stat`, `/proc/meminfo`, the load average
  and the inbox every 10 s into a 180-point ring. Inside a container these
  files report the *host*, which is deliberate: the numbers then match what
  the Container Manager shows.
- The snapshot rides along on the existing `/api/dashboard` answer, which the
  page already polls every few seconds. No second endpoint, no second timer.
- Gaps in the history are skipped when drawing, never plotted as zero — a
  missed sample must not look like a collapse to idle.
- Every measurement falls back to `null` on its own, and the sampler thread
  cannot die of an exception; a page that shows a dash is better than a page
  that does not load.

## [0.76.1] - 2026-10-02

### Fixed

**The upload button dropped into a second row, far left.** The invoice page
added an eighth entry to the navigation, and the header row was already full:
logo, links and the right-hand controls all sat as siblings in one wrapping
flex row, so the last element — the upload button — was the one pushed out.

Measured in Chromium at 1280/1366/1440/1536/1680/1920: without the eighth entry
everything fitted; with it, four of the six widths broke. A wider screen did not
help, because the bar is capped at `max-w-screen-2xl` (1536 px).

The right-hand controls are now one `shrink-0` group that cannot leave the
first row, and only the links may wrap among themselves. Below 2xl the links
ride in the horizontal strip that already existed for medium widths — eight
German labels plus the controls do not fit beside each other at 1280 px. The
settings gear moved in with the controls, where it belongs: it is a control,
not a destination.

🔴 Worth writing down: the first attempt used `xl:flex`, a class the **built**
stylesheet did not contain (only `xl:inline-flex`). `hidden` therefore stood,
the whole link box was invisible, and the measurement reported "fine" because
nothing was left that could wrap. The built sheet is the authority, not
Tailwind's vocabulary — every new class needs a run of
`scripts/build/build-css.sh`.

A new bench, `probe_kopfleiste.py`, drives a real browser at those six widths
and asserts the upload button stays top-right — and that every class in the
header exists in the built stylesheet.

## [0.76.0] - 2026-10-02

### Added

**An invoice page: what was asked of you, what is still open, and how you know
the rest was paid.** `/rechnungen` lists every document an amount was read out
of — with sums, a date range, free grouping, and three states side by side.

Nothing new had to be stored for it. Documents have carried `due_amount`,
`paid_tx_id`, `paid_at` and `deadline_done_at` for a while; what was missing was
a view that asks the right question of them.

**Three states, kept apart on purpose.** A tick by hand and a booking from a
bank statement are both "done", but they are not worth the same:

| | |
|---|---|
| **open** | nothing suggests it was paid |
| **ticked off** | somebody said it was settled — *unconfirmed*, no money moved behind it |
| **paid** | tied to a booking — *confirmed*, and the page names where the booking came from: an imported bank statement or a CSV import, with its date and amount |

They are summed separately. A figure that adds "ticked off" to "paid" answers no
question anybody actually has.

**Not every amount is a payable.** The text reader finds totals, and some of
them are not bills at all. Measured on a real archive of 118 amounts, taken word
for word from the stored evidence:

```
Gesamtes Vertragsguthaben 21.357,37 EUR    ← a contract balance
Gesamtrente 117,81 EUR                     ← income
Gesamtersparnis: 0,45 €                    ← a saving
```

Summed bluntly that is 172,895.16 € of "invoices"; honestly it is 151,178.13 €.
A table that sums wrongly is worse than no table, because people believe it. So
`finance/invoice_kind.py` sorts every amount into *payable*, *credit note* (it
belongs to an invoice and reduces the total) or *note* (it does not). Notes are
hidden by default — and **counted** next to the switch that shows them, so
hiding is never concealing. The rule errs towards *note*: an invoice missing
from a total is noticed when reading the list; a pension counted as an open
payable quietly falsifies every figure on the page.

An amount read **without tax** keeps its caveat: the row is marked `netto` and
the page says above the totals how many there are and that the sum is that much
too low.

The evidence travels with every row — the snippet the amount was read from —
because a figure without its origin cannot be checked.

Further: date range with quick picks, grouping by topic, sender, year, month,
state or kind (each group carries its own subtotals), category filter, free-text
search, tick off / undo straight from the list, CSV export, and the filter state
in the address so a view can be sent to somebody.

## [0.75.0] - 2026-10-02

Joined a tailnet is not the same as reachable, and for one user the settings card
could not tell the difference.

### Fixed

**The card showed a green "connected" and an address that led nowhere.**
Connecting runs two steps: `tailscale up` joins the tailnet, `tailscale serve`
publishes the page on 443. On one installation the first succeeded and the second
did not:

```
joined, but could not publish the page: error enabling https feature:
error 500 Internal Server Error: zero serverNoiseKey
```

The card, however, read only `BackendState == "Running"` — which the first step
alone already sets. So it showed the green badge and a clickable
`https://<name>.ts.net`, while nothing was listening behind it. On the phone that
came back as `ERR_NAME_NOT_RESOLVED`, which looks like a fault of the phone.

Both symptoms had one cause: **MagicDNS and HTTPS Certificates were switched off
in that tailnet.** Without MagicDNS the name does not exist, which is the failed
lookup; and HTTPS cannot be switched on without MagicDNS, which is the 500.

Three things changed:

* Whether the page is really being served is now **measured**, not assumed.
  `tailscale serve status --json` answers `{"TCP": {"443": {"HTTPS": true}}}` on a
  working install and leaves the block out otherwise. The status route carries
  that as `angeboten`, and the green badge and the address both hang on it.
* When `serve` fails, the message is turned into an instruction — switch MagicDNS
  and HTTPS Certificates on under DNS, then disconnect and connect again — in the
  reader's own language, with a link to that page. Tailscale's original text is
  shown underneath and never replaced: a reading can be wrong, a measurement
  should not disappear behind a friendly sentence. A failure that does *not* match
  keeps the general message instead of sending everyone to the DNS page.
* `serve` returning 0 is no longer taken as proof. It means "accepted", and the
  check afterwards costs one call.

There is no fallback to paper over here. With `--tun=userspace-networking`
inbound traffic arrives **only** through `tailscale serve` — if that fails, the
installation is not reachable over Tailscale by any route, and the card now says
so instead of offering an address.

**The dashboard badge reported on a part nobody was using.** It read "Bridge
offline", always — even on an installation with no bridge configured. On a setup
classifying happily through a local model (`provider: openai_compat`) the page
showed a red failure: a true statement about a component nothing talks to.

The badge now reports the **configured** provider and says who is on the other
end — model and host, e.g. `qwen2.5:7b-instruct · 192.0.2.5:11434`. It has three
states, not two:

| provider | reachability |
|---|---|
| `bridge` | known without the network — the connection lives in the process |
| `openai_compat` | really checked, at most once a minute, 2 s patience |
| cloud | **not checked at all**, and shown grey — neither green nor red |

Nobody pings Anthropic every two seconds just to keep a dot green, so "not
checked" is its own state rather than a guess dressed up as one. The local check
asks the bare root, not `/v1/…`: an Ollama with no model answers 200 at the front
and 404 to every question, and the badge asks whether something is alive, not
whether it can think.

## [0.74.0] - 2026-10-02

Three faults on the one path "Ollama through the settings". All three already had
their reasoning written beside them as a comment — the work was never done. That
is the worst kind: reading the code, it looks finished.

### Fixed

**`/v1` was never appended, although the comment said it was.** Above
`self.base_url = base_url.rstrip("/")` stood *"ensure /v1 if user gave the bare
host"*. Enter `http://localhost:11434` — exactly what Ollama's own documentation
shows — and the request went to `…:11434/chat/completions` and came back 404. The
settings page saved half of it: the automatic route ("find a local model")
appends `/v1`, the field filled in by hand does not. Two routes, one of them
sound. All five spellings are now normalised, and a provider with a path of its
own (Groq, Mistral, Together) is left alone.

**A local model was given the cloud's timeout.** `bridge` gets
`max(timeout * 3, 180)`, with the reason stated beside it: local inference on a
7B model takes 30–90 s for a long bank statement, and a clock check must not kill
a call that is almost done. Word for word the same applies to `openai_compat`,
which *is* the route to Ollama — and it got the cloud default of 60 s. Measured
on a four-core machine with no graphics card:

| | |
|---|---|
| `qwen2.5:3b-instruct`, bank statement 1699 tokens, raw API | 144 s |
| `qwen2.5:3b-instruct`, electricity bill, the program's real path | **186 s** |
| `qwen2.5:7b-instruct`, the same document | 465 s |

Every classification would have failed. The floor is now 600 s — and that number
came from the bench, not from me: the first attempt used the bridge's 180 s, and
the probe pointed out that the measured 186 s is six seconds past it. An explicit
`timeout_seconds` still wins; the floor only raises the bottom, and a limit that
never fires costs a cloud provider nothing.

**The search for a local model found nothing on Linux.** It asks
`host.docker.internal` whenever DocuSort runs in a container. Docker Desktop
(Mac, Windows) invents that name; Docker on a NAS, a Pi or a VPS does not, unless
the compose file maps it — and it did not. Measured inside a real container: the
host answered on its LAN address and on the gateway `172.17.0.1`, and the name
itself answered `Name or service not known`. So the card said "nothing found"
about a model running right beside it. `docker-compose.yml` now carries
`extra_hosts: host.docker.internal:host-gateway`.

### Added

**A local model in the box next door — one command.** Until now "use a local
model" assumed you already had an Ollama somewhere and knew its address:

```
docker compose --profile ki up -d
docker compose exec ollama ollama pull qwen2.5:7b-instruct
```

The search then finds it at `http://ollama:11434` by itself — a service name from
the same file, resolved inside the Docker network, asked *before* the host
because it needs no published port. It stays **off** until you name it: a
multi-gigabyte download is not something to hand somebody who only wanted their
scans sorted. The model is held in memory permanently (`OLLAMA_KEEP_ALIVE=-1`),
because reloading it costs 30 s on top of every wait, measured.

**Which model, measured rather than assumed.** The same electricity bill,
classified on the program's real path against a real category list:

| | time | category |
|---|---|---|
| `qwen2.5:3b-instruct` | 186 s | `Haus` (Home) |
| `qwen2.5:7b-instruct` | 465 s | `Rechnungen` (Invoices) |

The smaller model is two and a half times faster and files the document in the
wrong drawer. A document in the wrong drawer is a document you have to find
again, so the compose file names the larger one.

### Changed

The bundled Watchtower names the optional model in its watched list. A container
nobody names is never updated, and nothing reports that.

### Bench

`probe_lokales_modell.py` — it measures behaviour rather than reading the source,
because the source already claimed two of these three things while they were
untrue. It reads the compose file as YAML (an `extra_hosts` inside a comment maps
no name) and pulls the candidate list out of the function with `ast` (the address
also appears in an explaining comment, and a comment asks nobody).

## [0.73.1] - 2026-10-01

### Fixed

**The Tailscale instructions now say which of the two buttons to press.** The
Keys page offers *Generate auth key…* under **Auth keys** and *Generate access
token…* under **API access tokens**, and the text said only "generate a key".
The second one is a key for the Tailscale API: it cannot log a machine in, and
`tailscale up` fails with a message that never mentions the button one line
above.

Both places now name the right button, say which one is wrong, and say that the
key you want starts with `tskey-auth-` — in the settings card in all five
languages, in the installer prompt, in both compose overlays and in the README.

**And a key of the wrong kind is now recognised before anything is tried.**
Paste an API access token and DocuSort says so, names the button to use instead,
and changes nothing. Something that is not a Tailscale key at all is turned away
the same way.

## [0.73.0] - 2026-10-01

### Added

**Tailscale is now a field and a button in the settings.** It worked before, as
a second compose file and a shell script with an auth key — which assumes a
command line, an editor, and somebody who knows which directory they are
standing in. Settings → *On your phone, from anywhere* now asks for one key,
and afterwards shows the address:

```
https://docusort.<your-tailnet>.ts.net
```

Open it on the phone, add it to the home screen, and it looks like an app.
Nothing is published to the internet, no port is forwarded, and the certificate
is Tailscale's business.

This is possible because `tailscaled --tun=userspace-networking` needs neither
`NET_ADMIN` nor `/dev/net/tun` — measured in a bare container before any of this
was built. DocuSort therefore brings its own Tailscale instead of demanding a
second container.

Three details that decide whether it keeps working:

* the login is stored in the **mounted** config directory. In the image it would
  be gone at the next `docker compose pull`, leaving a dead machine in your
  tailnet.
* it resumes by itself after a restart. With hourly updates, anything else would
  mean pressing the button every hour.
* the page is published on the port DocuSort **actually** listens on, read from
  the settings — not on a default. That exact mix-up is what made an
  installation from before the move to 9876 unreachable behind Tailscale: the
  name resolves, the certificate is valid, and nobody is listening behind it.

The sidecar route still exists and is unchanged; it is the right one when
Tailscale should also carry other containers.

## [0.72.1] - 2026-10-01

### Fixed

**Tailscale on an installation from before the move to port 9876 now points at
the right door.** `tailscale/serve.json` carries a fixed number, and an older
installation goes on listening on 8080 inside its container, because its
`config.yaml` belongs to it and is never overwritten. Tailscale would then have
put 443 onto a port where nobody is listening: the name resolves, the
certificate is valid, and the page fails with an error that looks like Tailscale
and is not. The setup now asks the config that is actually on disk — the same
question the installer asks — and says so when it moves the target.

### Changed

**The installer says how to get onto the phone.** Its closing message now names
the one command that puts an existing DocuSort on your tailnet, with nothing
exposed to the internet and no port forwarding. It worked before; nobody could
find it.

## [0.72.0] - 2026-10-01

### Added

**DocuSort and the Postwache together on your tailnet, in one command.** Each of
them could already be reached over Tailscale on its own; the two of them side by
side could not. Now:

```
curl -fsSL .../deploy/install-both.sh | bash -s -- tskey-auth-xxxxxxxx
```

and afterwards, from the phone, anywhere:

```
https://docusort.<your-tailnet>.ts.net
https://postwache.<your-tailnet>.ts.net
```

Two names, no port numbers in the address, certificates Tailscale fetches and
renews itself, nothing published to the internet and no port forwarding. Open
them on the phone and add them to the home screen.

Two sidecars rather than one, deliberately: one would have meant one name for
two programs, so the second would have carried a port number in its address.
A name is something a person can say out loud.

The pairing between the two survives that: a container that rides another's
network has no name on the Docker network any more, so the Postwache now reaches
DocuSort at the sidecar's name instead. Measured on real Docker before shipping,
not reasoned about. MagicDNS is deliberately off inside those containers —
switching it on replaces the resolver and that name would stop resolving.

### Changed

**A Tailscale container that fails to start now says why.** When a sidecar does
not come up — a used-up auth key is enough — the applications cannot enter its
network and Docker says `cannot join network namespace of container: … is
restarting`, which is true and useless. The setup now prints the sidecar's own
log, says that the key is the usual cause and where to make a new one, and
reports failure. An empty log is named as empty rather than shown as a blank
line.

## [0.71.3] - 2026-10-01

### Changed

**The setup asks whether its ticket is still good before it does any work, not
after.** The ticket was used only at the very end, so somebody could install
Ollama, bind it, and wait out a model download of several gigabytes before being
told the ticket had expired — with everything done and nothing saved. The
question costs a tenth of a second and now comes first.

If the ticket is stale the setup stops immediately and says what is actually
true: a launcher carries the ticket it was downloaded with, that ticket does not
last for ever, nothing is wrong with the machine, and the file is simply too old.
Download the launcher again.

The check does not consume the ticket — a check that spent it would be the
reason the setup afterwards failed.

## [0.71.2] - 2026-10-01

### Fixed

**The setup ticket no longer expires in the middle of the setup it belongs to.**
It was good for 30 minutes and lived only in memory, and the setup script checks
it at the very end — after installing Ollama and pulling a model of several
gigabytes. Somebody watched that download finish and was then told

```
✋ DocuSort refused the setting: setup ticket invalid or expired
```

with the work done and nothing saved. Two things had changed under that design:
a model download is longer than thirty minutes on an ordinary line, and since
updates arrive hourly the service may restart right through a setup, which
emptied the ticket store.

Tickets are now good for four hours and are written down — the SHA-256 of the
token and its expiry, nothing that identifies anyone, in the config directory,
readable only by the owner, removed the moment the ticket is spent or expires.

**And if the handover fails anyway, the work is no longer lost in silence.** The
setup now says that Ollama is running and the model is on the machine, that only
the handover failed, and prints the two values needed to finish by hand: the
address and the model name.

## [0.71.1] - 2026-10-01

### Fixed

**A fresh Ollama with no model is no longer reported as unreachable.** The setup
asked Ollama for its models and treated the answer as the answer to a different
question. A newly installed Ollama has no models yet, so `/api/tags` replies —
correctly and politely — with an empty list. An empty list is false. So the setup
read "no models" as "no Ollama", announced

```
✋ Ollama is not reachable at http://192.0.2.38:11434
```

and stopped — one step before the thing that would have fixed it, which is
pulling a model. Meanwhile the owner opened that exact address in a browser and
read "Ollama is running".

"It answered" and "it has something" are two questions. The probe now returns
nothing at all when nobody answered and a list — possibly empty — when somebody
did, and the setup asks the first question for reachability and the second only
when choosing a model. An Ollama without a model is now greeted with "Ollama
answers. No model on it yet — fetching one now."

## [0.71.0] - 2026-10-01

### Changed

**Updates now arrive once an hour instead of once a night.** A fix you are
waiting for should not have to wait until the following night — especially not
one that was shipped because your installation is the one that is broken. The
shipped Watchtower schedule is now `0 0 * * * *`, measured against a real
Watchtower before shipping: it is accepted and reports its next run on the hour.

What that costs is stated where the schedule is: the restart happens on *its*
clock, not yours. `WATCHTOWER_SCHEDULE` in `.env` moves it — `0 0 4 * * *` puts
it back to a single nightly run at 04:00.

An installation that already exists keeps the schedule written into its own
compose file, because that file belongs to its owner. It switches over by adding
one line to `.env`:

```
WATCHTOWER_SCHEDULE=0 0 * * * *
```

## [0.70.1] - 2026-10-01

### Changed

**The update card now warns against the one action that silently doubles your
installation.** A user reported that after updating there were suddenly two
DocuSort containers. Measured on real Docker: the nightly Watchtower update is
not the cause — one container before, one after. A second one appears when the
container is **created anew** in a NAS interface instead of the image being
swapped. That updates nothing: it places a second DocuSort beside the first, both
wanting the same port and each keeping its own data, so the settings you save go
into one and the page you open comes from the other.

From inside its own container DocuSort cannot see that this has happened, so the
warning stands where the update path is described: run the command in the folder
that holds `docker-compose.yml`, and never create a new container instead. In all
five languages.

## [0.70.0] - 2026-10-01

### Added

**The installer refuses to put a second DocuSort on a machine that already has
one.** A user's Docker interface showed two containers from the same image: one
green, one turning in circles for ever. Neither was broken. They were simply
both there — started from different places, each with its own data and both
wanting the same port. Nothing on screen said there were two, so the setting you
saved went into one and the page you opened came from the other.

Before it writes anything, the installer now looks for other containers running
`ghcr.io/robeertm/docusort`, names them, says why two cannot work, shows the
command that reveals which ports and folders the other one uses, and stops
without having changed a thing. Its own container is not mistaken for a stranger,
so updating still works.

## [0.69.2] - 2026-10-01

### Fixed

**A failed image fetch no longer tears down an installation that already has the
image.** `docker compose pull && docker compose up -d` meant a momentary network
problem, or a registry having a bad minute, stopped the installer dead — even
when the image was sitting on the machine already. It now asks: no copy here
either, and it stops with that said; a copy here, and it says so and starts it.

## [0.69.1] - 2026-10-01

### Fixed

**The installer no longer tells you to switch on something that is already on.**
Its closing message said "or uncomment the watchtower block in
docker-compose.yml to have it done for you" — but the compose file the installer
writes carries that service **active**. Measured on a real machine: a fresh
install brings up `docusort-watchtower` and it logs `Next scheduled run:
04:00:00`. So the sentence sent the owner looking for something to enable that
was already running, and left the impression that updates do not happen by
themselves. The message now says what is true: updates arrive nightly at 04:00,
`docker logs docusort-watchtower` prints the next run, deleting that service
switches it off, and `WATCHTOWER_SCHEDULE` in `.env` moves the time.

## [0.69.0] - 2026-10-01

### Added

**DocuSort finds your Telegram Chat-ID for you.** Connecting Telegram asked you
to create a bot — fine, @BotFather walks you through that — and then to open
`api.telegram.org/bot<TOKEN>/getUpdates` and read your Chat-ID out of the raw
answer. That was the one step in the whole setup that asked somebody to decipher
a machine's reply, and it is the step people got stuck on.

Settings → Notifications → Telegram now has a **Find Chat-ID** button. Paste the
token from @BotFather, send your bot any message in Telegram, press the button:

* one chat found — it is filled in for you.
* several found (your own chat, a group, a channel) — each one is offered with
  its name, and one tap picks it.
* nothing found yet — it says so, and says what to do: message the bot first.
  That is not an error, it is the normal state of a brand-new bot.
* a wrong token, no internet, a blocked `api.telegram.org` — each is reported as
  what it is, in words you can act on, instead of a raw HTTP code.

Groups and channels are found as well, not only private chats, so a household or
an office can have the notifications land in one shared thread.

The help text in all five languages was rewritten to describe the button. Nobody
is sent to `getUpdates` any more.

## [0.68.1] - 2026-10-01

### Fixed

**The one-click Ollama launcher now says what is wrong when DocuSort has
moved.** The launcher carries the address DocuSort had when it was downloaded,
and its file name only ever carried the host — so after DocuSort changed port,
a freshly downloaded launcher had exactly the same name as the old one. Somebody
ran the old file, got `curl: (7) Failed to connect to ...:8080` and reasonably
concluded that a port had to be forwarded somewhere. Nothing of the sort: the
file was simply out of date.

* the file name now carries the port as well, so two launchers for two
  addresses are two visibly different files.
* when DocuSort does not answer, the launcher names the address it was made
  for, says in so many words that no port has to be forwarded or changed, and
  points at Settings → Local AI for a fresh download. It also states that
  nothing on the machine was touched.
* the message on the first failed attempt no longer claims a cause it cannot
  know. It used to say "Certificate not trusted" even when the real reason was
  that nothing was listening at that address at all.

**The setup wizard no longer falls back to port 8080** when it has no port to
show. That fallback was left over from before the move to 9876.

## [0.68.0] - 2026-10-01

### Changed

**The installer no longer says "starting" and walks away.** It ran
`docker compose up -d`, printed `DocuSort is starting.` and finished — and
"started" is Docker's word for "the process was launched". A container that dies
a second later and is restarted for ever says exactly the same thing. Somebody
whose installation never came up was told that it had, saw an entry in his NAS
interface that stayed orange instead of going green, and had no way of finding
out why — while the reason sat in the container's own log the whole time.

The installer now waits until the web interface **answers**, for up to a minute,
and while waiting it watches the container:

* if it answers, it says so, and the closing message reads `DocuSort is running.`
* if the container is in a restart loop — the one failure that looks like a
  success in every interface, because the container keeps being "running" — the
  installer names it, says how many restarts there have been, and explains that
  this is exactly why the Docker interface shows it as starting and never green.
* if the container has stopped, it says that instead of waiting out the minute.
* in every failing case the **last 30 log lines of the container** are printed,
  introduced as what DocuSort itself said, followed by the command for the whole
  log — and the installer exits non-zero instead of claiming success.

If the compose file publishes no port, there is no address to ask, and the
installer says that rather than pretending to have measured something.

## [0.67.4] - 2026-10-01

### Fixed

**The Ollama setup no longer reports a success that systemd outvoted.**
On a machine with a systemd Ollama service, the setup writes a drop-in file
with the address Ollama should listen on. systemd merges drop-ins in *filename*
order and the last one wins — and `docusort.conf` sorts before `override.conf`,
the name `systemctl edit` gives its file. A machine that had once been set up
by hand therefore kept its own address while the setup announced the new one.
Three changes, each one measurable:

* the effective address is now read back from systemd itself
  (`systemctl show -p Environment ollama`) after the restart. If it is not the
  one that was asked for, the setup says so, lists every drop-in that has a
  say, and reports failure instead of success.
* if another drop-in already sets the address and sorts after ours, ours is
  written as `zz-docusort.conf` so that it is the one in force. The other file
  is named on screen and left exactly as it is; deleting ours undoes
  everything.
* if the service is *already* set to the right address, nothing is written and
  nothing is restarted. The setup says that the next question is the network
  path, not the binding — which is what it actually is in that case.

## [0.67.3] - 2026-09-30

### Fixed

**A missing default config file no longer kills the container at start-up.**
`docker-entrypoint.sh` seeded `/app/config` only when `config.yaml` was absent.
That is the wrong question: the program needs three files — `config.yaml`,
`categories.yaml` and `categories.de.yaml` — and an installation whose config
directory held one but not the others was left with what it had. The container
then died with a Python traceback and never came back, which looks to its owner
exactly like "the update broke it".

`cp -n` is already per-file no-clobber, so asking first bought nothing: a config
the user owns is never overwritten either way. Every missing default file is put
there now, and nothing else is touched.

### Added

**`pruefstaende/vor_auslieferung.py` — the gate before a delivery.** It answers
three questions, and the third is the one that was missing all along:

1. Is every bench green? — read from the exit code, never through a pipe.
2. Do the version and the changelog agree, with this version at the top?
3. **Does it run?** A real installation on a machine with a real Docker daemon,
   once fresh and once as an installation that already exists, and then the web
   UI is actually fetched. Not "the file looks right" but "it answers". The
   fresh one has to publish its port and reply; the existing one has to go on
   replying on its old port with its own `config.yaml` untouched.

It refuses to run at all if the test machine carries a real installation, and it
clears up after itself. The entrypoint bug above is its first find — on its
first proper run, before anyone shipped it.

Six more checks in `pruefstaende/probe_installer.py` (41 in total) cover the
entrypoint directly: a config directory holding only `config.yaml` has to end up
with all three files, and the one that was already there has to come back
unchanged.

## [0.67.2] - 2026-09-30

### Fixed

**A kept compose file that publishes no port is now said out loud.** The
installer keeps a `docker-compose.yml` that is already there — it is the user's
file, and that is right. Keeping it *silently* is not: a compose file without a
`ports:` section starts a container that runs, listens inside, and cannot be
reached from anywhere. Nothing fails, nothing is logged, and `docker compose ps`
shows a bare `9876/tcp` instead of `0.0.0.0:9876->9876/tcp`, which nobody reads
as an error. The installer now looks, says so, and prints the
`docker-compose.override.yml` that fixes it without touching the user's file —
compose merges the two.

**An empty `DOCUSORT_PORT` can no longer publish a random port.** Measured on a
real Docker: with the variable unset, `"${DOCUSORT_PORT}:9876"` publishes on a
port the kernel picks (32768 here), so the container is running and reachable —
just not at the address the installer printed a second earlier. The generated
compose file now carries its own fallback, `"${DOCUSORT_PORT:-9876}:9876"`.

### Added

Six more checks in `pruefstaende/probe_installer.py` (36 in total), answering
"can a fresh installation end up unreachable at all?" with measurements rather
than a promise: the generated file has a `ports:` section and a fallback value,
a kept file without a port is left untouched but produces the warning and the
remedy, and a kept file that does publish a port produces no warning.

## [0.67.1] - 2026-09-30

### Fixed

**The installer no longer writes a port the installation does not have.** With
0.67.0 `deploy/install.sh` wrote `${DOCUSORT_PORT}:9876` into a compose file it
creates. That is right for a fresh install and wrong for an older one: its
`config/config.yaml` lives in a mounted directory and is never overwritten, so
the program inside the container goes on listening on 8080 — and a mapping to
9876 points at a door that is not there. The installer now reads the port out
of an existing `config/config.yaml` and uses that as the container side,
saying so as it does. Only an installation with no config of its own gets 9876.

This only ever bites when the compose file is being written again — normally it
is kept — but that is exactly the situation somebody is in when they are trying
to repair an installation.

### Added

Four more checks in `pruefstaende/probe_installer.py` (29 in total): a run
against a directory that already holds `config/config.yaml` with `port: 8080`
has to produce a compose file mapping to 8080 and nothing pointing at 9876, and
a run with no config has to stay on 9876.

## [0.67.0] - 2026-09-30

### Changed

**DocuSort no longer asks for port 8080.** A fresh installation now publishes
its web UI on **9876**: `http://<host>:9876`. 8080 is spoken for on a lot of
machines — a NAS hands it to its own web station, a development box to whatever
was started last — and an installation that collides on its first day looks
broken while the cause is invisible from the outside.

**Nothing moves for an installation that already runs.** The port lives in two
files that belong to the installation, not to the image: `docker-compose.yml`
and `.env` are both kept by `deploy/install.sh` when they already exist, and
`docker-entrypoint.sh` seeds the default `config.yaml` with `cp -n`, so a
config that is already there is never overwritten. An installation on 8080 goes
on listening on 8080 after any update. To move it anyway, set
`DOCUSORT_PORT=9876` in `.env`, change `8080` to `9876` on the right-hand side
of the `ports:` line, put `port: 9876` in `config/config.yaml`, and restart.

Changed with it, so the parts do not disagree: `docker-compose.yml`,
`docker-compose.both.yml` (including `POSTWACHE_DS_URL`), the Tailscale
overlay and its `serve.json`, both install scripts, the README, the default
`config.yaml`, the code default, `EXPOSE`, and the port hint on the settings
page in all five languages.

### Note for the Postwache

The Postwache finds DocuSort by asking a handful of addresses that follow from
how the two are installed. It now asks **both** ports, 9876 first and 8080
after it, so a pairing with an older DocuSort keeps working — and it asks them
side by side, because doubling the list would otherwise have doubled the wait
on a machine where nothing answers. That change ships in Postwache 5.10.0.

### Added

Six more checks in `pruefstaende/probe_installer.py` (25 in total). Two of them
are the promise above: a fresh install lands on 9876, and a run against an
installation that already exists leaves its `docker-compose.yml` byte for byte
as it was and keeps `DOCUSORT_PORT=8080` in its `.env`.

## [0.66.0] - 2026-09-30

### Fixed

**The one-click model setup now does the work instead of describing it.** On a
Linux machine where Ollama runs as a systemd service — which is what the
official installer sets up — the setup used to start a second `ollama serve` of
its own next to it. That second copy can only lose: the service already holds
port 11434, so it dies with `address already in use` and the user was left
looking at

    ! Ollama did not answer within 40 s.
    Ollama is not reachable at http://<this machine>:11434.

There is exactly one owner of that port on such a machine, and it is the
service. The setup now writes
`/etc/systemd/system/ollama.service.d/docusort.conf`, reloads and restarts the
service itself, and then waits for it to answer. The only thing to type is the
login password, once — and only when there is really something to do. The file
says in its own first lines how to undo it.

**It also says why, when something does go wrong.** `ollama serve` writes its
reason into `~/ollama-docusort.log`; the setup wrote it there and then reported
only that nothing had answered. The reason was on the user's own disk and
nobody showed it to them. Now the lines from *this* run are printed, the two
answers that come up again and again (an occupied port, an existing service)
are named, and an empty log is reported as an empty log rather than passed off
as silence.

**And it opens the firewall rather than asking the user to.** If Ollama answers
on the machine but not from the network, that is a firewall; firewalld and ufw
are detected and — after one plain yes/no question — opened.

### Changed

Everything above had to work on **any** Linux, not on one:

* **Becoming root** is a question, not a constant: `sudo`, `pkexec` (the
  graphical password box, and the only one that can ask at all when the script
  was double-clicked rather than started in a terminal) and `doas` are all
  used, in the order that fits the situation. With none of them available the
  setup says so plainly and prints what an administrator would have to run,
  instead of pretending the work was done.
* **Writing the file** prefers `install -D` and falls back to
  `mkdir -p` + `cp` + `chmod` where coreutils is busybox.
* **The firewall** is asked in its own words first (`firewall-cmd --state`,
  `/etc/ufw/ufw.conf`) and only then through the service manager, because not
  every system has one.
* **nftables and plain iptables are detected but never edited.** There is no
  safe, persistent, distribution-independent way to add a rule there, and a
  wrong one can cut a machine off its own network. The setup says what is in
  the way and leaves the rules to whoever wrote them.

### Added

`pruefstaende/probe_ollama_start.py` - 49 checks. It fakes a whole Linux
machine (`ollama`, `systemctl`, `sudo`, `pkexec`, `doas`, `install`,
`firewall-cmd`, `ufw`, `nft`) in a throwaway directory and reads back what was
actually produced: the contents of the unit drop-in and the commands that were
really issued. Included are the busybox path, each way of becoming root on its
own, a machine with no way at all, and a counter-test against the previous
behaviour.

## [0.65.2] – 2026-09-30

### Fixed

- 🔴 **The one-command install stopped on a Synology.** `deploy/install.sh`
  created `data/inbox`, `data/library` and `config` — and then wrote a
  compose file that also mounts `./logs`. Most Docker daemons create a
  missing bind-mount source themselves (measured: they do, owned by root),
  so the omission never showed. Synology's refuses and answers
  `Error response from daemon: Bind mount failed: '…/logs' does not exist`,
  which is exactly where a lot of people put this. The directory is created
  now, and after the compose file is in place the installer reads every
  `./…` mount back out of it and makes anything still missing — so a compose
  file that was already there, with paths of its own, is covered too, and a
  future omission costs a line of output instead of an install. A mounted
  *file* is left alone; creating it as a directory would break it for good.
  Re-running the one-liner repairs an installation that stopped this way.
- 🔴 **The installer reported failure after a successful install.**
  `hostname -I` is Linux-only, and under `set -euo pipefail` a failing one
  ended the script at the very last step — after the container was already
  running. The user saw a non-zero exit and none of the closing notes: no
  address, no paths, no next step.

### Changed

- 📄 The README's by-hand `mkdir` now names all four directories the
  repository's own `docker-compose.yml` mounts, with a word on why they have
  to exist first.

## [0.65.1] – 2026-09-29

### Changed

- 🧹 **The benches are out of the repository.** Six `probe_*.py` files sat in
  the root and were the first thing anybody saw when they opened the project —
  scaffolding in front of the house. They were never part of the product:
  nothing imports them, no `Dockerfile` copies them, nothing that installs or
  runs DocuSort has ever carried them. They now live outside the published tree,
  so the root shows what somebody installing actually needs.

  Earlier entries in this file still name them by filename. Those entries stay
  as they were written — they record what happened at the time; this is the
  note that explains where those files went.

## [0.65.0] – 2026-09-29

### Fixed

- 🧾 **Two doctor's bills stayed open although they were paid.** The amount
  matched to the cent and the date window was right; it failed on the payee.
  The bill comes from the practice, the transfer goes to its billing agency —
  not one word in common, and the match rule needs a shared word on purpose
  (a bare amount once turned an Amazon purchase into a paid phone bill).

  🔑 But the **invoice number stands on both sides**: in the document
  (`Rechnungsnummer 01-1425-137975`) and in the booking's purpose line
  (`ONLINE-UEBERWEISUNG TERM. 01-1425-137975 …`). So there is now a second way
  to establish the payee, and it is **stricter** than the name, not looser:
  at least eight digits have to be identical. Dates are excluded — `15.09.2026`
  would otherwise be an eight-digit number standing in half the archive — and
  so are short numbers that could collide. Amount, date window and the
  one-booking-settles-one-bill rule all still apply on top.

  Measured against the real archive: 58 open bills checked, **3 newly
  settled** — the two doctor's bills and a tax claim that is paid to the state
  treasury rather than to the office that issued it, matched on the reference
  number the document itself asks you to quote. Nothing was matched wrongly.

- 🧾 **A credit note stood on the card as something to pay.** A phone bill from
  November 2025 asked for **−113.05 €** — its own text says the amount is
  credited to the account and offset against the next bill. Money that comes
  back cannot be transferred away, so a negative amount no longer appears
  under "Fällig demnächst" and no longer triggers a reminder. The matcher had
  skipped credit notes since 0.49.0; the card had not.

### Changed

- 🧾 **A settled bill now leaves the card by itself after seven days.** Until
  now paid entries stayed until they were ticked off by hand, so the card
  slowly turned into a list of things already done. They stay green and
  visible for a week — long enough to see that the payment was recognised —
  and then drop out. A hand-tick still removes one immediately.

### Added

- 🧪 **`probe_fristen.py`** — 16 probes on a throwaway database of its own:
  a bill paid to a different name but with a shared invoice number must
  match; the same amount from an unrelated payee must not; a shared date is
  not evidence; a credit note is not a demand; and a paid entry is still
  there on the last day of the week and gone on the next. Counter-tested
  against the previous behaviour, where four of them turn red.

## [0.64.1] – 2026-09-29

### Fixed

- 🎨 **Pale text on paper: a hint on the settings page measured 1.06:1 —
  light on white, simply not there.** The cause was not the colour but the
  SHAPE of its name. Light mode turns the pastel tones into their darker
  sibling through a list of class names:

  ```css
  html[data-theme="light"] :is(.text-cyan-100, .text-cyan-200, …) { … }
  ```

  Tailwind writes a **different** name for every shape of the same colour, and
  two of them were outside that list:

  | in the markup            | matched by the list? |
  | ------------------------ | -------------------- |
  | `text-cyan-100`          | yes                  |
  | `text-cyan-100/90`       | **no** — opacity modifier |
  | `hover:text-emerald-300` | **no** — state prefix |

  25 places carried an opacity modifier and 55 a state prefix. All of them
  stayed pastel on paper: notes that could not be read, links that vanished
  under the pointer. The rules now match the **shape** instead of the exact
  name, so a new opacity modifier is covered the day it is written. Dark mode
  is untouched — verified by reading the computed colour back in both themes.

- 🎨 **Hint text that used `text-ink-600` moved one step darker.** The palette
  is mirrored for light mode, which makes `ink-600` a pale grey on paper —
  2.56:1, below anything readable — and it carried real text in 18 places
  (the hints under the finance fields, the amount a document was matched on,
  the separators). It cannot be healed through the colour either: for
  `ink-600` to reach 4.5:1 on white it would have to be darker than `ink-500`,
  which turns the scale of faint steps upside down. So the faintest step that
  may carry text is `ink-500` (4.76:1 on white). Borders and surfaces in
  `ink-600` are untouched.

### Added

- 🧪 **`probe_kontrast.py`** — it compares the two sides that drifted apart:
  every pastel shape the templates use against every shape the stylesheet
  covers in light mode, and it checks the **built** sheet as well, because a
  rule that only exists in the source colours nothing. It also refuses text in
  a tone that disappears on paper. Counter-tested against three deliberately
  broken states; each one turns it red.

## [0.64.0] – 2026-09-29

### Changed

- 🔒 **Tailscale: from an instruction to one command.**

  ```bash
  ./deploy/tailscale.sh tskey-auth-xxxxxxxxxxxx
  ```

  That is the whole setup. The script writes the key **and** `COMPOSE_FILE`
  into `.env`, starts everything and prints the finished address — which it
  reads out of `tailscale cert`, the same trick
  `scripts/setup-tailscale-https.sh` already used.

  🔑 `COMPOSE_FILE` is the real gain, and it was measured: with that line in
  `.env`, a plain `docker compose up -d` uses both files from then on. Nobody
  has to remember `-f … -f …` again — and nobody starts by accident with a
  published port and no tailnet because they forgot it once.

- 🔄 **Watchtower is switched on**, in the compose file **and** in the
  one-command installer. Nightly at 04:00, movable with `WATCHTOWER_SCHEDULE`.
  It is the maintained fork `ghcr.io/nicholas-fedor/watchtower` (`containrrr`
  has stood still for years), and it watches **only** the `docusort` container,
  which is why it does not fight with a Watchtower you already run.

  🔴 An existing Watchtower does **not** pick DocuSort up on its own if it
  names the containers it watches. The README says how to tell.

### Added

- 🤝 **The pairing with the Postwache makes itself.**

  * **Installed together → no clicks at all.** `deploy/install-both.sh` or
    `docker-compose.both.yml`: one secret in one `.env`, read by both sides.
    DocuSort creates the account at start-up, the Postwache writes it down.
    Proven end to end: account there, access file at 0600, a real login
    succeeds.
  * **Installed separately → one click each side.** A new *Postwache* card in
    the settings shows a **pairing line** to copy; the Postwache has a field to
    paste it into.

- 🔑 **A third role, `deliver`.** The Postwache account may do
  **`POST /upload` and `GET /api/status/<name>`** — and nothing else. It used
  to need a `user`, and a `user` may also **read** the library and the
  finances. While a person creates that account by hand, that is a decision
  somebody made; now that the pairing happens on its own, it would be one
  nobody made. Checked against a running instance: library, finances,
  analytics, document list and settings all answer **403**.

### Fixed

- 🔴 **Raw tags stood as TEXT on the page.** Nine translations deliberately
  carry markup (`<b>`, `<code>`, a link) but were rendered with `{{ t(…) }}`,
  which Jinja escapes — so `/settings` and `/upload` showed `<b>macOS:</b>`.
  Six places now render safely; measured in the browser before and after.
- 🔴 **The wall in front of the last administrator asked the wrong question.**
  It checked "does this user become `user`" instead of "does this user lose
  `admin`". With a third role the last admin could have slipped past it and
  locked everyone out.
- 🔴 **The build workflow also ran on tags and pushed `:latest` along with
  it.** Tagging an older version would have handed every customer old code as
  `latest` on their next pull. It builds from `main` only now.

### Verified

- `probe_einstellungen.py` grows **18 → 35** checks: Watchtower (image, scope,
  schedule, and in the installer too), the Postwache card, the narrow role with
  the counter-check "the library is NOT in that list", the both-file and both
  scripts. The counter-tests go red when the old Watchtower image comes back or
  `/library` is smuggled into the narrow list.
- `probe_veroeffentlichung.py` checks that tag, release and image really made it
  out — including that the workflow does not build on tags.

## [0.63.0] – 2026-09-29

### Changed

- 🔴 **The settings page had three doors to a local model.** The "AI provider"
  dropdown offered "OpenAI-compatible" (with the address `localhost:11434`)
  **and** "Local AI Bridge"; below it stood two more cards of their own, "A
  local model, in one click" and "Local AI Bridge". Use any one of them and you
  still saw the others.

  There is now **one** card with **one** question — which provider? — and below
  it only what belongs to that answer. Nothing was dropped: the search for a
  running Ollama, the installers for macOS/Windows/Linux and the bridge are all
  still there, just where you look for them. Measured on a phone: cards
  **8 → 6**, page height **6116 → 4906 px**, 0 JS errors, and switching the
  provider reveals exactly the matching block.

### Added

- 🔒 **Tailscale as the way in.** New: `docker-compose.tailscale.yml` as an
  **overlay** next to the main file — an existing install stays untouched, and
  the private way in is one extra `-f`:

  ```bash
  echo 'TS_AUTHKEY=tskey-auth-…' >> .env
  docker compose -f docker-compose.yml -f docker-compose.tailscale.yml up -d
  ```

  Then `https://docusort.<your-tailnet>.ts.net` — HTTPS with a certificate
  Tailscale fetches and renews by itself. No port open, no reverse proxy, no
  certificate to look after.

  🔴 **`ports: !reset []`, not `ports: []`.** Compose **merges** lists: with
  the empty list the published port from the main file stayed standing — and a
  container that publishes a port **and** rides another one's network is
  refused by Docker at start. Found by actually rendering the merged file:
  `docker compose config` called the broken version **valid**.

  🔴 The auth key belongs in `.env`, never in the repository, and is needed
  only once: after that the machine's identity lives in `tailscale/state/`
  (git-ignored). Counter-test: without `TS_AUTHKEY` the start aborts instead of
  quietly coming up with no network.

### Verified

- **The phone view, twelve pages measured** (home, library, finances, bookings,
  expenses, fixed costs, upload, settings, users, account, analytics,
  duplicates) in WebKit: **0 horizontal scroll, 0 overflow, 0 clipped text, 0
  iOS zoom traps**. The work from 0.58.0 holds.
- **Every setting really takes effect** — 20 checks against the configuration
  *file*, not against the return value: AI provider, model and address; web
  address and port; privacy switches; notifications; backup; language; the
  search for local AI; the bridge status. Plus counter-tests: an unknown
  provider and an impossible port are refused with **400** and the
  configuration stays untouched.
- New bench `probe_einstellungen.py` (18 checks) keeps both nailed down.

## [0.62.0] – 2026-09-26

### Fixed
- 🔴 **The Local AI Bridge card was never translated.** Switch DocuSort to
  German and the whole card stayed English — heading, description, every
  field label, both status badges, all four buttons and the first-run hints.
  The i18n pass had covered the other cards on the page and walked past this
  one. It now speaks all five languages, and so does the AI-provider card's
  bridge section.
- 🔴 **The duplicates page was English in every language.** All of it,
  including the singular/plural built into the markup as `group{s}` and
  `cop{y|ies}` — a rule that only exists in English. Each language now gets
  its own singular and plural key.
- 🔴 **Translations rendered into JavaScript string literals.** 123 places
  wrote a translated sentence straight into a JS literal
  (`x-text="'{{ t('k') }}'"`). Jinja escapes the apostrophe to `&#39;`, and
  that lands differently depending on where it sits: inside a `<script>`
  block the browser does not decode entities, so French and Italian users
  read `l&#39;IA` on screen; inside an Alpine expression attribute the
  browser *does* decode it first, so the expression breaks and the element
  goes dead. Nine places were already live in French and Italian. All 123 now
  call `tr('key')`, which reads the text at runtime instead of casting it
  into the source.
- The `| replace("'", "\\'")` guard that was meant to prevent this never
  worked — it runs before Jinja's escaping, so it produced `Aujourd\&#39;hui`
  rather than a usable apostrophe. Removed in all 37 places.
- Three more untranslated strings: the service-restart flow in Settings, the
  bridge token regeneration prompt, and the finance link on the transactions
  page.

### Added
- **`probe_sprachen.py`** — renders every page in all five languages and
  checks each `<script>` block actually parses, finds visible text that never
  passes through the translator, finds translations cast into JS literals
  (including the ones today's languages have no apostrophe for — they are
  armed, not safe), and checks key completeness and placeholder parity.
  Counter-tested against six deliberate defects: eight failures.

### Security
- The bridge card's status line and rejection notice no longer build markup
  out of the client's self-reported host name; they render as text.

## [0.61.0] – 2026-09-25

### Added
- **A local model in one click.** Press *Find a model* in Settings and DocuSort
  looks where it can actually reach one: its own machine, its container host,
  and the computer that has the settings page open — side by side, with a
  ceiling. It then offers what it found with a usable model already picked
  (embedding and vision models are skipped; they cannot classify a document).
- 🔴 **It looks from the server's side, not the browser's.** The browser runs
  on the machine where Ollama sits and would report "reachable" while DocuSort
  — on a VM, in a container — cannot get there at all. No network scan: only
  addresses that are already known get asked.
- **A setup you download and double-click** (macOS, Windows, Linux) with this
  install's address baked in. It installs Ollama (Homebrew / the official
  script / winget), makes it listen where DocuSort can reach it, pulls a
  model, writes the setting — then asks **DocuSort** whether it works, and
  offers to restart the service so the classifier picks it up.
- **`probe_local_ai.py`** — DocuSort's first probe. Runs the whole app against
  a throwaway config and database, presses no button that acts outward.

### Changed
- 🔴 **"Saved" is no longer reported as "works".** `/api/local-ai/apply` now
  puts one real, tiny question to the model and reports the answer. The check
  goes straight at the address, so it holds even before the service restarts.
- The *Local Ollama on this machine* card was **hard-coded English** while the
  rest of DocuSort speaks five languages. Rebuilt and translated.

### Security
- The setup script has no session, so it carries a **ticket**: thirty minutes,
  held in memory, good for exactly two things — writing that one setting and
  restarting the service.
- 🔴 **Three exact paths are public, never the prefix.** Opening
  `/api/local-ai/` would let anyone on the network make DocuSort probe
  addresses. The probe demonstrates it: with the prefix open,
  `/api/local-ai/probe` answers **HTTP 200 with no session at all**.

### Fixed
- A translation rendered into a **JavaScript string literal**
  (`x-text="'{{ t('…') }}'"`) tears the script apart the moment a language
  contains an apostrophe — "Pas encore d'Ollama". In that language only, in
  the browser only. Now `tr()` instead of Jinja inside the script, and the
  probe looks for it.

## [0.60.0] – 2026-09-25

### Fixed
- 🔴 **In a container, "Update now" did the wrong thing — silently.** The
  in-app updater downloads a release, swaps the code directories and
  restarts the systemd unit. That is the right move on a source install and
  the wrong one in a container, where the code is part of the image: the
  swap succeeded, there was no systemd to restart, and the next
  `docker compose up` handed the old code back without a word. You would
  see "updated", restart, and be on the old version. There was no container
  check anywhere. Now `updater.in_container()` decides (our own image sets
  `DOCUSORT_IN_DOCKER=1`; otherwise `/.dockerenv`, `/run/.containerenv`, and
  the cgroup line as a last resort), `install_latest()` refuses with a
  `ContainerUpdateError`, and `POST /api/update` answers **409** with the
  command that does work — not a 500, because this is the wrong door, not a
  failure.
- 🔴 **On a phone the deadline card was unreadable.** The overdue chip sat
  next to the subject as a `shrink-0` sibling, so at 320–430 px the title
  was cut to "Elect…" and the line below ran up to **seven** lines, one word
  each. The chip now lives inside the text column and wraps with the title,
  and the title wraps instead of truncating (in a flex row a `nowrap` child
  shrinks rather than wrapping — that was the actual trap). Measured at
  320/360/375/390/430/500/644/1277 px: nothing truncated, no sideways scroll.

### Added
- **The update banner now shows the right path for the install it is in.**
  From source: the **Update now** button as before. In a container: the
  `docker compose pull && docker compose up -d` command, with a line saying
  why. `GET /api/version` carries `container` and `update_command`.
- **Watchtower as an opt-in**, commented out in `docker-compose.yml` and in
  the installer's generated file — nightly at 04:00, not hourly. The comment
  says what it costs: Watchtower needs the Docker socket, which is
  effectively root on the host.
- **A README chapter "Updating"** that opens with the sentence people miss:
  Docker never re-pulls a running container — `:latest` is a label, not a
  subscription.

## [0.59.0] – 2026-09-25

### Added
- **"Match against the finances" on the *Due soon* card.** The pass reads
  the invoice amount out of each document's own text and looks for the
  booking that settled it. It used to run at start-up, after an import and
  every twelve hours — now it also runs on demand, right where the word
  *overdue* is printed. The result is stated ("27 bill(s) linked to a
  booking") and the card reloads. New route
  `POST /api/finance/match-deadlines`.
- **A freshly filed document asks by itself.** At the end of the pipeline
  the pass is scheduled and runs about twenty seconds later.

### Changed
- 🔴 **A bill could wait up to twelve hours for its booking.** The amount
  is not a field in the document, it is *read* out of the text — and it was
  read on a twelve-hour cycle only. Without an amount there is nothing to
  compare, so a bill dropped in at noon sat on the dashboard as **overdue**
  until midnight although the direct debit had gone out long before.
- **All four callers now use one place** (new module `deadline_match`):
  start-up, the twelve-hour reminder watchdog, the button, and the
  pipeline. 🔴 Two passes must never overlap —
  `finance_match_due_payments` reads the set of already-linked bookings
  **once** at the start, so two concurrent runs could have handed the same
  booking to two different bills. A process-wide lock rules that out.
- **A burst costs one pass.** When forty-five invoices arrive at once the
  timer is restarted per document and the pass runs **once** after the last
  one — with a two-minute ceiling, so a steady trickle (one document every
  few seconds) cannot postpone it for ever.
- The CSV import now reads the amounts before matching. A bill whose amount
  had never been read could not be linked even when the matching booking
  had just been imported.
- A user, not only an admin, may trigger the pass. They can already link a
  booking to a document by hand (`POST /api/document/<id>/paid`); the pass
  asks the same question for every open bill at once.

## [0.58.1] – 2026-09-21

### Fixed
- 🔴 **The PDF preview did not fit its window on a phone.** It was an
  embedded viewer (`iframe`), and **iOS Safari ignores the fit-to-window
  parameter there**, showing the page at its original size — you saw the
  top left corner and nothing else. On a phone the preview is now a
  **page image rendered on the server** (Poppler / `pdftoppm`) that always
  fits the width; multi-page documents unfold below each other. The
  embedded viewer stays on the desktop and steps back in whenever the
  rendering is not possible (no Poppler, an encrypted file).
- Page images live next to the database in `preview-cache/` and carry the
  source file's modification time in their name, so the preview never
  shows a stale page after a re-run of OCR. Backups skip the folder.
- `poppler-utils` is now installed explicitly in the Docker image.

## [0.58.0] – 2026-09-21

### Changed
- **The phone view was rebuilt, not patched.** Until now the desktop
  layout was simply squeezed: ten table columns on 386 pixels, controls
  26 pixels tall, nearly a thousand pieces of text below 12.5 pixels. The
  phone now has a shape of its own.
- **A tab bar at the bottom** replaces the scrolling strip under the
  header: Home, Library, Upload (in the middle, because everything comes
  in there), Finance and “More”. Behind *More* sits a sheet with every
  other section plus language, appearance and sign-out. The old strip
  showed three entries; the rest hid behind a swipe nobody expects.
- **Tables become lists.** Fixed costs, bookings, receipts, line items
  and the recent bookings on the finance page each show one row per
  entry: name and amount on top, the essentials below, the category
  across the full width. From tablet width upwards the tables are
  unchanged.
- **Everything you tap is at least 44 pixels tall** (Apple's guideline)
  and input fields carry 16-pixel text — below that iOS zooms in on
  focus, unasked.
- **Readable type**: the smallest steps are one notch larger on a phone.
  Chart axes are excluded, where the text decides the column width.
- **Filters fold away.** Bookings put a two-screen filter card in front
  of the first booking; the library put 1,300 pixels of filters in front
  of the first document. Both now open on tap.
- **Bookings load forty at a time**: the page was 16,000 pixels long, now
  8,000 with a button for more.
- The daily chart is taller on a phone and starts at the last day that
  actually happened; long introductions appear from tablet width upwards.

### Fixed
- **More than 30 German strings** still showed in every language — the
  “Explore bookings” heading, the key figures (incoming, outgoing, net,
  saved/invested), the filter labels, “Top spending”, “By category”, “Why
  this category?”, several messages after saving, and the note above the
  costs that are not counted. 0.56.0 translated the templates but not the
  strings inside the JavaScript.
- A checkbox shrank to 13 pixels wide inside a flex row.

## [0.57.0] – 2026-09-20

### Added
- **Account filter on the fixed costs page** — the same control as on the
  spending page: one tick per account, *Apply*, and every figure on the
  page covers only those accounts: contracts, the per-category averages,
  the category list and both totals. A line above the title then names the
  accounts included, so a filtered total never looks like the full one.
  All ticks set means all accounts.

### Fixed
- **The running salary month was only drawn up to today.** Since 0.52.0
  the daily chart was meant to show the whole period with the days still
  to come as faint dots; in fact it stopped at today, because that is
  where the open period ends. On the third day of a salary month you saw
  three bars instead of a month. The chart now reaches the expected end of
  the salary month — the coming days still count nowhere.
- On a phone the chart consequently scrolled into a strip of empty future
  days; it now rests on the last day that has actually happened.
- **Ten German strings on the fixed costs page** were missed in 0.56.0 and
  showed in every language, among them the table heading, the saving
  badge, “loading …”, “since …”, the monthly-average note and the contract
  counter.
- **The quick date ranges on the bookings page** (“Today”, “Yesterday”,
  “Current month”, “Last 12 months”, …) were hard-coded German as well, as
  were the two prompts for creating a category.
- A first name appeared in a visible hint (the salary-month anchor day)
  in all five languages, and in 37 source comments.

## [0.56.0] – 2026-09-20

### Added
- **Saving game** on the spending page: every day that is already over scores
  points — a day without spending scores most, a day below your own benchmark
  scores some — plus a bonus of up to 20 % when the period ends in the black.
  Streaks, badges and five ranks, and a leaderboard across every month so the
  best one is obvious. Days that have not happened yet, and days for which no
  booking has arrived, count nowhere.
- **Leaderboard** of all periods, sorted by points ratio so short and running
  periods stay comparable; collapsed to the best three plus the current one.
- **Account filter** on the spending page: take single accounts out of every
  figure on the page, or look at one account alone.
- **Receipts say how they were paid** — found as a booking on an account, paid
  in cash, or still open because the statement for that day is missing. Cash
  receipts can be filed into a category, which is the only way that money shows
  up anywhere.
- **"How much is left"** — the budget alarm's figures (remaining, per day, days
  left) are shown next to the spending, and the daily chart draws the allowed
  daily amount as a line.

### Changed
- **Upload is the one way in.** Documents, receipts, statement PDFs and bank
  CSVs all go through the same page — single files, whole folders, drag and
  drop or a phone scan. The separate import form on the finance page is gone.
- **The bookings and fixed-costs pages speak all five languages**, and numbers
  and dates now follow the interface language instead of a fixed German format.
- The daily spending chart was rebuilt: outliers run off the top instead of
  flattening every other day, every bar carries its amount, days without
  spending are shown as what they are, and the whole period is visible from the
  first day of the month.

### Fixed
- A bank CSV dropped on the upload page never reached the server: the browser's
  file filter only knew documents and images and discarded it silently.
- A receipt was almost never matched to its booking, because the matcher looked
  for an amount in the OCR text instead of using the total the receipt already
  carries.
- A statement that arrived late was counted twice: the synthetic booking that
  had bridged the gap was never removed.
- A gap in the imported data was treated as a day without spending.

## [0.1.0 – 0.55.0] – 2026-03 … 2026-09

The private development history, in brief:

- **Documents**: folder watcher, OCR for scans and photos, AI classification
  with a stated reason, year/category/subcategory filing, readable file names,
  full-text search, tags, a review queue, duplicate detection, deadline
  extraction and reminders, a trash that can be undone.
- **Receipts**: line-item extraction, item categories, shop types, monthly and
  per-category analytics, a salvage pass for receipts that were filed as
  ordinary documents.
- **Money**: CSV import for nine German banks, deterministic statement-PDF
  reader verified against the statement's own balances, account management,
  net worth, savings accounts, transfers recognised by IBAN, category learning
  per payee, AI suggestions for unknown payees, fixed-cost detection, salary
  months, budget alarm, invoice-to-booking matching.
- **Platform**: multi-user login with an allowlist permission model, five
  languages, light and dark themes, mobile layout, rclone backups, one-click
  updates, Docker image, HTTPS, configurable AI provider including local
  models.
