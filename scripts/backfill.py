#!/usr/bin/env python3
"""Jednorazowe uzupełnienie zbioru danych wstecz (backfill) z API NBP.

Pobiera historię kursów walut (tabela A) i cen złota z zadanego okresu - domyślnie
ostatnich 6 miesięcy (wymaganie projektu: co najmniej 3) - w kawałkach po maksymalnie
93 dni (limit API NBP) i idempotentnie scala ją z plikami ``dane/*.csv``.

Przy okazji porządkuje istniejące pliki: usuwa zduplikowane daty, sortuje wiersze
i dodaje nagłówek. Skrypt można uruchamiać wielokrotnie - wynik będzie taki sam.

Przykłady::

    python scripts/backfill.py                          # ostatnie 6 miesięcy, EUR USD CHF GBP + złoto
    python scripts/backfill.py --miesiace 12
    python scripts/backfill.py --od 2026-01-01 --do 2026-06-30 --waluty eur usd
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import sys
from pathlib import Path
from typing import List, Optional

from fetch_nbp import (
    KATALOG_DANYCH,
    WALUTY_DOMYSLNE,
    ZLOTO,
    _data,
    aktualizuj,
    dzisiaj_w_warszawie,
)

MIESIACE_DOMYSLNIE = 6


def miesiace_wstecz(data: dt.date, miesiace: int) -> dt.date:
    """Ta sama data ``miesiace`` miesięcy wcześniej (np. 31.05 - 3 mies. = 28/29.02)."""
    rok, miesiac = divmod(data.year * 12 + (data.month - 1) - miesiace, 12)
    miesiac += 1
    for dzien in range(data.day, 27, -1):
        try:
            return dt.date(rok, miesiac, dzien)
        except ValueError:
            continue
    return dt.date(rok, miesiac, min(data.day, 28))


def parser_argumentow() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Uzupełnia dane/*.csv historią kursów NBP (backfill).")
    p.add_argument("--miesiace", type=int, default=MIESIACE_DOMYSLNIE,
                   help="ile miesięcy wstecz pobrać, gdy nie podano --od (domyślnie: %(default)s)")
    p.add_argument("--od", type=_data, default=None, help="data początkowa RRRR-MM-DD")
    p.add_argument("--do", type=_data, default=None, help="data końcowa RRRR-MM-DD (domyślnie: dziś w Warszawie)")
    p.add_argument("--waluty", nargs="+", default=list(WALUTY_DOMYSLNE), metavar="KOD",
                   help="kody walut z tabeli A (domyślnie: %(default)s)")
    p.add_argument("--bez-zlota", action="store_true", help="nie pobieraj cen złota")
    p.add_argument("--katalog", type=Path, default=KATALOG_DANYCH, help="katalog z plikami CSV")
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = parser_argumentow().parse_args(argv)
    dzis = dzisiaj_w_warszawie()
    do = min(args.do or dzis, dzis)
    if args.od is None and args.miesiace < 1:
        print("--miesiace musi być >= 1", file=sys.stderr)
        return 2
    od = args.od or miesiace_wstecz(do, args.miesiace)
    if od > do:
        print(f"Data początkowa {od} jest późniejsza niż końcowa {do}", file=sys.stderr)
        return 2
    nazwy = [w.lower() for w in args.waluty] + ([] if args.bez_zlota else [ZLOTO])
    zle = [n for n in nazwy if n != ZLOTO and not re.fullmatch(r"[a-z]{3}", n)]
    if zle:
        print(f"Niepoprawne kody walut: {', '.join(zle)} (oczekiwano 3 liter, np. eur)", file=sys.stderr)
        return 2
    print(f"Backfill: {(do - od).days + 1} dni kalendarzowych")
    # Backfill uruchamia się ręcznie - każdy błąd źródła ma być widoczny (kod wyjścia 1).
    return aktualizuj(dict.fromkeys(nazwy), od, do, args.katalog, scisle=True)


if __name__ == "__main__":
    sys.exit(main())
