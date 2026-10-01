# Changelog

All notable changes to DocuSort are documented here.
Dates are ISO; versions follow `MAJOR.MINOR.PATCH`.

This file starts with the first public release. The project was developed
privately before that; the summary under *0.1.0 – 0.55.0* lists what arrived
along the way rather than every single step.

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
✋ Ollama is not reachable at http://192.168.178.38:11434
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
