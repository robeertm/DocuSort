"""Was kann dieser Rechner — gemessen, nicht geschaetzt.

🔴 WARUM DAS EIN EIGENES MODUL IST. Dieselbe Frage wird an zwei Stellen
gestellt: vom Einrichtungsskript, das auf dem Zielrechner laeuft, und von
DocuSort selbst, wenn jemand auf der Startseite „lokales Modell einrichten"
drueckt. Zwei Kopien derselben Messung driften auseinander, und dann sagt die
eine Stelle „reicht" und die andere „reicht nicht" ueber dieselbe Maschine.

🔑 EIN MODELL, DAS NICHT HINEINPASST, IST NICHT LANGSAM — ES IST KAPUTT. Ohne
genug Speicher faengt das System an auszulagern, ein einziges Dokument dauert
Stunden, oder der Server wird abgeschossen. Von aussen sieht beides aus, als
sei DocuSort defekt. Darum wird gemessen, BEVOR irgendetwas geladen wird, und
das Ergebnis wird ausgesprochen — auch wenn es „dein Rechner kann das nicht"
lautet.

🔴 UNMESSBAR IST NICHT DASSELBE WIE ZU WENIG. Wo sich die Zahlen nicht lesen
lassen, wird nichts behauptet und der Normalfall angenommen. Jemanden wegen
einer fehlgeschlagenen Messung auszusperren waere schlimmer, als ihn es
versuchen zu lassen.
"""

from __future__ import annotations

import logging
import os
import platform
import subprocess
from typing import Any

logger = logging.getLogger(__name__)

# Ein 7B-Modell in Q4 belegt rund 5 GB, dazu Platz zum Arbeiten.
MIN_GB_GROSS = 8.0
# Ein 3B-Modell in Q4 belegt rund 2 GB.
MIN_GB_KLEIN = 4.5
# Darunter wird auch das kleine Modell auf der CPU zur Geduldsprobe.
MIN_KERNE = 4

MODELL_GROSS = "qwen2.5:7b-instruct"
MODELL_KLEIN = "qwen2.5:3b-instruct"

# Ungefaehre Groesse der Modelle, damit die Oberflaeche sagen kann, was da
# geladen wird. Bytes, nicht Marketing-GB.
MODELL_BYTES = {MODELL_GROSS: 4_700_000_000, MODELL_KLEIN: 1_900_000_000}


def _ram_bytes() -> int:
    """Arbeitsspeicher in Bytes, 0 wenn nicht messbar."""
    system = platform.system()
    try:
        if system == "Darwin":
            return int(subprocess.check_output(
                ["sysctl", "-n", "hw.memsize"], text=True).strip())
        if system == "Linux":
            # 🔴 In einem Container beschreibt /proc/meminfo den WIRT. Wenn eine
            #    cgroup-Grenze gesetzt ist, ist SIE die Wahrheit fuer uns — ein
            #    Modell, das in den Wirt passt, aber nicht in unser Limit, wird
            #    vom Kernel abgeschossen.
            for pfad in ("/sys/fs/cgroup/memory.max",
                         "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
                try:
                    with open(pfad) as fh:
                        roh = fh.read().strip()
                    if roh and roh != "max":
                        grenze = int(roh)
                        # Eine „Grenze", die groesser als jeder reale Speicher
                        # ist, heisst in Wahrheit: keine Grenze.
                        if 0 < grenze < (1 << 60):
                            return grenze
                except (OSError, ValueError):
                    pass
            with open("/proc/meminfo") as fh:
                for zeile in fh:
                    if zeile.startswith("MemTotal:"):
                        return int(zeile.split()[1]) * 1024
        if system == "Windows":
            import ctypes

            class _MS(ctypes.Structure):
                _fields_ = [("dwLength", ctypes.c_ulong),
                            ("dwMemoryLoad", ctypes.c_ulong),
                            ("ullTotalPhys", ctypes.c_ulonglong),
                            ("ullAvailPhys", ctypes.c_ulonglong),
                            ("ullTotalPageFile", ctypes.c_ulonglong),
                            ("ullAvailPageFile", ctypes.c_ulonglong),
                            ("ullTotalVirtual", ctypes.c_ulonglong),
                            ("ullAvailVirtual", ctypes.c_ulonglong),
                            ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]
            st = _MS()
            st.dwLength = ctypes.sizeof(_MS)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(st))
            return int(st.ullTotalPhys)
    except Exception:  # noqa: BLE001
        pass
    return 0


def _gpu() -> str:
    """Name einer brauchbaren Grafikkarte, "" wenn keine, "?" wenn unklar."""
    system = platform.system()
    try:
        if system == "Darwin":
            # Apple Silicon teilt den Speicher mit der GPU, Ollama nutzt Metal.
            return ("Apple Silicon (Metal)"
                    if platform.machine() in ("arm64", "aarch64") else "")
        out = subprocess.run(["nvidia-smi", "--query-gpu=name",
                              "--format=csv,noheader"],
                             capture_output=True, text=True, timeout=6)
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout.strip().splitlines()[0]
        return ""
    except FileNotFoundError:
        return ""
    except Exception:  # noqa: BLE001
        return "?"


def _produktname() -> str:
    """Was für ein Gerät ist das? Aus dem DMI des Wirts.

    🔑 DAS IST IM CONTAINER LESBAR — `/sys/class/dmi/id/product_name` kommt vom
    Kernel des Wirts, nicht aus dem Dateisystem des Containers. Auf Roberts
    Synology steht dort `DS1621+`, und das beantwortet eine Frage, die sonst
    unbeantwortbar schien: worauf läuft das hier eigentlich.
    """
    for pfad in ("/sys/class/dmi/id/product_name",
                 "/sys/devices/virtual/dmi/id/product_name"):
        try:
            with open(pfad) as fh:
                name = fh.read().strip()
            if name and name.lower() not in ("none", "to be filled by o.e.m.",
                                             "system product name", "default string"):
                return name
        except OSError:
            pass
    return ""


def _cpu_name() -> str:
    try:
        with open("/proc/cpuinfo") as fh:
            for zeile in fh:
                if zeile.startswith("model name"):
                    return zeile.split(":", 1)[1].strip()
    except OSError:
        pass
    if platform.system() == "Darwin":
        try:
            return subprocess.check_output(
                ["sysctl", "-n", "machdep.cpu.brand_string"], text=True).strip()
        except Exception:  # noqa: BLE001
            return ""
    return ""


# 🔴 Keine Herstellerliste, sondern die EIGENSCHAFT: Geräte, die rund um die
#    Uhr leise im Regal stehen, haben sparsame CPUs ohne Grafikeinheit. Genau
#    das macht sie langsam beim Rechnen — und genau das sollen sie sein.
_NAS_PRAEFIXE = ("ds", "rs", "dva", "fs", "sa",      # Synology
                 "ts-", "tvs-", "tbs-")              # QNAP
_SPARSAM = ("embedded", "atom", "celeron", "pentium silver", "n3150",
            "n5105", "n100", "j3455", "j4125", "geode")


def geraeteart() -> dict[str, Any]:
    """Was für eine Maschine ist das — und ist Rechnen hier zu erwarten langsam?

    🔴 RAM UND KERNE SIND KEIN URTEIL ÜBER GESCHWINDIGKEIT. Roberts Synology
    hat 32 GB und 8 Kerne — nach dieser Rechnung „reichlich" — und schafft
    trotzdem nur 5,6 Token/s, weil die CPU eine sparsame Embedded-CPU ohne
    Grafikeinheit ist. Ein Dokument dauert dort rund 14 Minuten, auf einem Mac
    mit Apple Silicon 10 Sekunden. Wer nur Speicher zählt, verspricht etwas,
    das die Maschine nicht halten kann.
    """
    produkt = _produktname()
    cpu = _cpu_name()
    p_klein = produkt.lower()
    ist_nas = any(p_klein.startswith(v) for v in _NAS_PRAEFIXE)
    sparsam = any(w in cpu.lower() for w in _SPARSAM)
    return {
        "produkt": produkt, "cpu": cpu,
        "art": "nas" if ist_nas else ("sparsam" if sparsam else "normal"),
        # Langsam ist nicht dasselbe wie unbrauchbar: es läuft, es dauert nur.
        "langsam": bool(ist_nas or sparsam) and not _gpu(),
    }


def pruefe() -> dict[str, Any]:
    """Misst die Maschine und sagt, was sie kann.

    Rueckgabe:
      ram, kerne, gpu   die Messung (ram in Bytes, 0 = unmessbar)
      modell            welches Modell hier laufen soll ("" = keines)
      modell_bytes      ungefaehre Groesse des Downloads
      urteil            'gut' | 'klein' | 'knapp' | 'nein' | 'unbekannt'
    """
    ram = _ram_bytes()
    kerne = os.cpu_count() or 0
    gpu = _gpu()
    d: dict[str, Any] = {"ram": ram, "kerne": kerne, "gpu": gpu}

    if not ram:
        logger.info("Hardware: Speicher nicht messbar — nehme den Normalfall an")
        d.update(geraeteart())
        d.update(modell=MODELL_GROSS, modell_bytes=MODELL_BYTES[MODELL_GROSS],
                 urteil="unbekannt")
        return d

    gb = ram / (1024 ** 3)
    if gb < MIN_GB_KLEIN:
        d.update(geraeteart())
        d.update(modell="", modell_bytes=0, urteil="nein")
        return d
    if gb < MIN_GB_GROSS:
        d.update(geraeteart())
        d.update(modell=MODELL_KLEIN, modell_bytes=MODELL_BYTES[MODELL_KLEIN],
                 urteil="klein")
        return d

    # 🔴 „Genug Speicher" heisst noch lange nicht „schnell". Die Geraeteart
    #    entscheidet mit: ein NAS mit 32 GB ist immer noch ein NAS.
    art = geraeteart()
    d.update(art)
    langsam = art["langsam"] or (not gpu and kerne and kerne < MIN_KERNE)
    d.update(modell=MODELL_GROSS, modell_bytes=MODELL_BYTES[MODELL_GROSS],
             urteil="knapp" if langsam else "gut")
    return d
