#!/usr/bin/env python3
"""Pobieranie średnich kursów walut (tabela A) i cen złota z API NBP do plików CSV.

Krok "extract + transform + load" potoku:

* extract   - pobranie danych z https://api.nbp.pl (bez kluczy i rejestracji),
* transform - walidacja i zamiana odpowiedzi JSON na pary (data, wartość),
* load      - idempotentne scalenie z plikami CSV w katalogu ``dane/``.

Właściwości:

* **Idempotentność** - wiersze są kluczowane datą; ponowne uruchomienie nie tworzy
  duplikatów, a plik jest nadpisywany tylko wtedy, gdy jego treść faktycznie się zmienia
  (brak pustych zatwierdzeń w repozytorium).
* **Dni bez tabeli** - w weekendy i święta NBP zwraca ``404 Brak danych``; skrypt traktuje
  to jako brak nowych danych i kończy się spokojnie (kod wyjścia 0).
* **Samonaprawianie** - zamiast samego "dzisiaj" pobierane jest okno ostatnich dni
  (domyślnie 10), więc pominięte lub opóźnione uruchomienie uzupełni się przy następnym.
* **Brak śmieci** - zapisywane są tylko poprawne daty i dodatnie liczby; zapis jest
  atomowy (plik tymczasowy + ``os.replace``).

Wyłącznie biblioteka standardowa Pythona (>= 3.9) - nic nie trzeba instalować.

Przykłady::

    python scripts/fetch_nbp.py                    # EUR, USD, CHF, GBP + złoto, ostatnie 10 dni
    python scripts/fetch_nbp.py --waluty eur usd   # wybrane waluty
    python scripts/fetch_nbp.py --bez-zlota --dni 30
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import http.client
import json
import os
import re
import ssl
import sys
import tempfile
import time
import urllib.error
import urllib.request
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python < 3.9
    ZoneInfo = None  # type: ignore[assignment]

# --------------------------------------------------------------------------- konfiguracja

API = "https://api.nbp.pl/api"
WALUTY_DOMYSLNE: Tuple[str, ...] = ("eur", "usd", "chf", "gbp")
ZLOTO = "zloto"  # nazwa pliku z cenami złota: dane/zloto.csv

KATALOG_DANYCH = Path(__file__).resolve().parent.parent / "dane"

NAGLOWEK_WALUTY = ("data", "kurs")  # średni kurs NBP w PLN za 1 jednostkę waluty
NAGLOWEK_ZLOTA = ("data", "cena")   # cena 1 g złota (próby 1000) w PLN

MAKS_DNI_ZAPYTANIA = 93    # limit długości zakresu dat w jednym zapytaniu do API NBP
NAJSTARSZA_DATA_WALUT = dt.date(2002, 1, 2)
NAJSTARSZA_DATA_ZLOTA = dt.date(2013, 1, 2)

LIMIT_CZASU_S = 20
PROBY = 4                  # liczba prób przy błędach przejściowych (sieć, 5xx, 429)
ODSTEP_PROB_S = 2.0        # bazowy odstęp; rośnie wykładniczo (2, 4, 8 s)
USER_AGENT = "kursy-nbp-pipeline/1.0 (+https://api.nbp.pl)"

Wiersze = Dict[dt.date, Decimal]


class BladPobierania(RuntimeError):
    """Źródło nie odpowiedziało poprawnie mimo ponownych prób."""


# --------------------------------------------------------------------------- czas


def dzisiaj_w_warszawie() -> dt.date:
    """Bieżąca data w strefie Europe/Warsaw (NBP publikuje tabele wg czasu polskiego).

    Runnery GitHub Actions pracują w UTC, a API NBP odrzuca zakres sięgający przyszłości
    (400 "Błędny zakres dat"), więc "dzisiaj" liczymy w czasie polskim.
    """
    if ZoneInfo is not None:
        try:
            return dt.datetime.now(ZoneInfo("Europe/Warsaw")).date()
        except Exception:  # brak bazy stref czasowych w systemie
            pass
    return dt.datetime.now(dt.timezone.utc).date()


def zakresy_dat(od: dt.date, do: dt.date, maks_dni: int = MAKS_DNI_ZAPYTANIA) -> Iterator[Tuple[dt.date, dt.date]]:
    """Dzieli przedział [od, do] na kolejne kawałki po co najwyżej ``maks_dni`` dni."""
    if od > do:
        raise ValueError(f"Data początkowa {od} jest późniejsza niż końcowa {do}")
    poczatek = od
    while poczatek <= do:
        koniec = min(poczatek + dt.timedelta(days=maks_dni - 1), do)
        yield poczatek, koniec
        poczatek = koniec + dt.timedelta(days=1)


# --------------------------------------------------------------------------- extract


def url_kursow(waluta: str, od: dt.date, do: dt.date) -> str:
    return f"{API}/exchangerates/rates/a/{waluta.lower()}/{od.isoformat()}/{do.isoformat()}/?format=json"


def url_zlota(od: dt.date, do: dt.date) -> str:
    return f"{API}/cenyzlota/{od.isoformat()}/{do.isoformat()}/?format=json"


def pobierz_json(url: str, proby: int = PROBY, odstep_s: float = ODSTEP_PROB_S) -> Optional[object]:
    """Pobiera JSON spod adresu.

    Zwraca ``None``, gdy NBP odpowie ``404 ... Brak danych`` (weekend, święto, przyszła tabela).
    Błędy przejściowe (sieć, 5xx, 429, nietypowe 404 z serwera WWW) są ponawiane z wykładniczym
    odstępem; po wyczerpaniu prób zgłaszany jest ``BladPobierania``.
    Liczby są wczytywane jako ``Decimal`` - bez błędów zaokrągleń typu float.
    """
    zadanie = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": USER_AGENT})
    ostatni_blad = ""
    for proba in range(1, proby + 1):
        try:
            with urllib.request.urlopen(zadanie, timeout=LIMIT_CZASU_S) as odp:
                tresc = odp.read().decode("utf-8")
            return json.loads(tresc, parse_float=Decimal)
        except urllib.error.HTTPError as e:
            try:
                tresc = e.read().decode("utf-8", "replace")
            except Exception:
                tresc = ""
            if e.code == 404 and "Brak danych" in tresc:
                return None  # brak tabeli w tym zakresie dat - to nie jest błąd
            ostatni_blad = f"HTTP {e.code}: {tresc.strip()[:120]}"
            # 400 (np. błędny zakres dat) się nie zmieni po ponowieniu
            if e.code == 400 or (400 <= e.code < 500 and e.code not in (404, 408, 429)):
                break
        except (urllib.error.URLError, http.client.HTTPException, OSError) as e:
            ostatni_blad = f"błąd sieci: {e}"
            if isinstance(getattr(e, "reason", e), ssl.SSLCertVerificationError):
                break  # błąd certyfikatu nie zniknie po ponowieniu
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            ostatni_blad = f"niepoprawny JSON: {e}"
        if proba < proby:
            time.sleep(odstep_s * 2 ** (proba - 1))
    raise BladPobierania(f"{url} -> {ostatni_blad}")


# --------------------------------------------------------------------------- transform


def _poprawna_wartosc(wartosc: object) -> Optional[Decimal]:
    try:
        liczba = Decimal(str(wartosc))
    except (InvalidOperation, ValueError):
        return None
    if not liczba.is_finite() or liczba <= 0:
        return None
    return liczba


def przetworz_kursy(odpowiedz: object, waluta: str = "") -> Wiersze:
    """Zamienia odpowiedź ``/exchangerates/rates/a/{kod}/...`` na ``{data: kurs_sredni}``."""
    if odpowiedz is None:
        return {}
    if not isinstance(odpowiedz, dict) or not isinstance(odpowiedz.get("rates"), list):
        raise ValueError(f"Nieoczekiwany format odpowiedzi kursów {waluta.upper()}")
    kod = str(odpowiedz.get("code", "")).lower()
    if waluta and kod and kod != waluta.lower():
        raise ValueError(f"Odpowiedź dotyczy waluty {kod.upper()}, a oczekiwano {waluta.upper()}")
    wynik: Wiersze = {}
    for pozycja in odpowiedz["rates"]:
        try:
            data = dt.date.fromisoformat(str(pozycja["effectiveDate"]))
        except (KeyError, TypeError, ValueError):
            continue
        kurs = _poprawna_wartosc(pozycja.get("mid"))
        if kurs is not None:
            wynik[data] = kurs
    return wynik


def przetworz_zloto(odpowiedz: object) -> Wiersze:
    """Zamienia odpowiedź ``/cenyzlota/...`` na ``{data: cena_1g}``."""
    if odpowiedz is None:
        return {}
    if not isinstance(odpowiedz, list):
        raise ValueError("Nieoczekiwany format odpowiedzi cen złota")
    wynik: Wiersze = {}
    for pozycja in odpowiedz:
        try:
            data = dt.date.fromisoformat(str(pozycja["data"]))
        except (KeyError, TypeError, ValueError):
            continue
        cena = _poprawna_wartosc(pozycja.get("cena"))
        if cena is not None:
            wynik[data] = cena
    return wynik


def pobierz_zakres(nazwa: str, od: dt.date, do: dt.date) -> Wiersze:
    """Pobiera kursy waluty (lub złota, gdy ``nazwa == "zloto"``) z przedziału [od, do]."""
    najstarsza = NAJSTARSZA_DATA_ZLOTA if nazwa == ZLOTO else NAJSTARSZA_DATA_WALUT
    od = max(od, najstarsza)
    if od > do:
        return {}
    wynik: Wiersze = {}
    for poczatek, koniec in zakresy_dat(od, do):
        if nazwa == ZLOTO:
            wynik.update(przetworz_zloto(pobierz_json(url_zlota(poczatek, koniec))))
        else:
            wynik.update(przetworz_kursy(pobierz_json(url_kursow(nazwa, poczatek, koniec)), nazwa))
    return wynik


# --------------------------------------------------------------------------- load


def sciezka_pliku(nazwa: str, katalog: Path = KATALOG_DANYCH) -> Path:
    return Path(katalog) / f"{nazwa.lower()}.csv"


def naglowek_dla(nazwa: str) -> Tuple[str, str]:
    return NAGLOWEK_ZLOTA if nazwa.lower() == ZLOTO else NAGLOWEK_WALUTY


def wczytaj_csv(sciezka: Path) -> Wiersze:
    """Wczytuje plik ``data,wartość`` (z nagłówkiem lub bez). Duplikaty dat są scalane,
    a puste lub uszkodzone wiersze pomijane. Brak pliku = pusty zbiór."""
    wiersze: Wiersze = {}
    if not Path(sciezka).exists():
        return wiersze
    with open(sciezka, newline="", encoding="utf-8") as f:
        for numer, rekord in enumerate(csv.reader(f), start=1):
            if not rekord or not "".join(rekord).strip():
                continue
            try:
                data = dt.date.fromisoformat(rekord[0].strip())
            except ValueError:
                if numer != 1:  # pierwszy wiersz to zwykle nagłówek
                    print(f"  ostrzeżenie: {sciezka}:{numer} pominięto wiersz {rekord!r}", file=sys.stderr)
                continue
            wartosc = _poprawna_wartosc(rekord[1].strip()) if len(rekord) > 1 else None
            if wartosc is None:
                print(f"  ostrzeżenie: {sciezka}:{numer} pominięto wiersz {rekord!r}", file=sys.stderr)
                continue
            wiersze[data] = wartosc
    return wiersze


def do_tekstu_csv(wiersze: Wiersze, naglowek: Iterable[str]) -> str:
    """Kanoniczna postać pliku: nagłówek + wiersze posortowane rosnąco po dacie, ``\\n``."""
    linie = [",".join(naglowek)]
    linie += [f"{data.isoformat()},{format(wartosc, 'f')}" for data, wartosc in sorted(wiersze.items())]
    return "\n".join(linie) + "\n"


def zapisz_atomowo(sciezka: Path, tresc: str) -> None:
    sciezka = Path(sciezka)
    sciezka.parent.mkdir(parents=True, exist_ok=True)
    fd, tymczasowy = tempfile.mkstemp(prefix=f".{sciezka.name}.", suffix=".tmp", dir=sciezka.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            f.write(tresc)
        os.replace(tymczasowy, sciezka)
    except BaseException:
        if os.path.exists(tymczasowy):
            os.unlink(tymczasowy)
        raise


def scal_i_zapisz(sciezka: Path, nowe: Wiersze, naglowek: Iterable[str]) -> Tuple[int, int, bool]:
    """Idempotentnie scala nowe wiersze z plikiem CSV.

    Zwraca ``(dodane, poprawione, zmieniono_plik)``. Plik jest zapisywany tylko wtedy,
    gdy nowa treść różni się od obecnej (także gdy trzeba usunąć duplikaty lub dodać nagłówek).
    Gdy NBP skoryguje opublikowany kurs, w pliku zostaje wartość z API.
    """
    sciezka = Path(sciezka)
    stare = wczytaj_csv(sciezka)
    dodane = sum(1 for d in nowe if d not in stare)
    poprawione = sum(1 for d, w in nowe.items() if d in stare and stare[d] != w)
    scalone = {**stare, **nowe}
    tresc = do_tekstu_csv(scalone, naglowek)
    obecna = sciezka.read_text(encoding="utf-8") if sciezka.exists() else None
    if tresc == obecna or (not scalone and obecna is None):
        return dodane, poprawione, False
    zapisz_atomowo(sciezka, tresc)
    return dodane, poprawione, True


# --------------------------------------------------------------------------- uruchomienie


def aktualizuj(nazwy: Iterable[str], od: dt.date, do: dt.date, katalog: Path = KATALOG_DANYCH,
               scisle: bool = False) -> int:
    """Pobiera dane z [od, do] dla podanych walut/złota i scala je z plikami CSV.

    Zwraca kod wyjścia: 0 - sukces lub brak nowych danych; 1 - błąd źródła w trybie ``scisle``.
    W trybie domyślnym błąd jednego źródła jest zgłaszany jako ostrzeżenie, a pozostałe
    źródła są przetwarzane dalej - niczego błędnego nie zapisujemy, a brakujące dni uzupełni
    następne uruchomienie (okno ostatnich dni).
    """
    w_actions = os.environ.get("GITHUB_ACTIONS") == "true"
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)  # logi stdout/stderr w poprawnej kolejności
    print(f"Zakres dat: {od} - {do}")
    bledy: List[str] = []
    zmienione: List[str] = []
    for nazwa in nazwy:
        nazwa = nazwa.lower()
        sciezka = sciezka_pliku(nazwa, katalog)
        try:
            nowe = pobierz_zakres(nazwa, od, do)
        except (BladPobierania, ValueError) as e:
            bledy.append(nazwa)
            komunikat = f"{nazwa.upper()}: nie udało się pobrać danych ({e})"
            print(f"::warning::{komunikat}" if w_actions else f"  ostrzeżenie: {komunikat}", file=sys.stderr)
            continue
        dodane, poprawione, zmieniono = scal_i_zapisz(sciezka, nowe, naglowek_dla(nazwa))
        if zmieniono:
            zmienione.append(sciezka.name)
        if not nowe:
            stan = "brak tabeli w tym zakresie (weekend/święto) - nic do zapisania"
        else:
            ostatnia = max(nowe)
            stan = f"{len(nowe)} notowań, ostatnie {ostatnia}: {nowe[ostatnia]}"
        szczegoly = f"+{dodane} nowych" + (f", {poprawione} poprawionych" if poprawione else "")
        print(f"  {nazwa.upper():<6} {stan}; {szczegoly}; plik {'zaktualizowany' if zmieniono else 'bez zmian'}")

    print("Zmienione pliki: " + (", ".join(zmienione) if zmienione else "brak"))
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as f:
            f.write(f"zmiany={'true' if zmienione else 'false'}\n")
            f.write(f"bledy={','.join(bledy)}\n")
    return 1 if (bledy and scisle) else 0


def _data(tekst: str) -> dt.date:
    try:
        return dt.date.fromisoformat(tekst)
    except ValueError:
        raise argparse.ArgumentTypeError(f"oczekiwano daty RRRR-MM-DD, otrzymano {tekst!r}")


def parser_argumentow() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Pobiera średnie kursy walut NBP (tabela A) i ceny złota do dane/*.csv.")
    p.add_argument("--waluty", nargs="+", default=list(WALUTY_DOMYSLNE), metavar="KOD",
                   help="kody walut z tabeli A (domyślnie: %(default)s)")
    p.add_argument("--bez-zlota", action="store_true", help="nie pobieraj cen złota")
    p.add_argument("--dni", type=int, default=10,
                   help="ile ostatnich dni kalendarzowych pobrać (domyślnie: %(default)s)")
    p.add_argument("--do", type=_data, default=None, help="data końcowa RRRR-MM-DD (domyślnie: dziś w Warszawie)")
    p.add_argument("--katalog", type=Path, default=KATALOG_DANYCH, help="katalog z plikami CSV")
    p.add_argument("--scisle", action="store_true",
                   help="zakończ kodem 1, gdy któreś źródło nie odpowie (domyślnie tylko ostrzeżenie)")
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = parser_argumentow().parse_args(argv)
    if args.dni < 1:
        print("--dni musi być >= 1", file=sys.stderr)
        return 2
    do = min(args.do or dzisiaj_w_warszawie(), dzisiaj_w_warszawie())
    od = do - dt.timedelta(days=args.dni - 1)
    nazwy = [w.lower() for w in args.waluty] + ([] if args.bez_zlota else [ZLOTO])
    zle = [n for n in nazwy if n != ZLOTO and not re.fullmatch(r"[a-z]{3}", n)]
    if zle:
        print(f"Niepoprawne kody walut: {', '.join(zle)} (oczekiwano 3 liter, np. eur)", file=sys.stderr)
        return 2
    return aktualizuj(dict.fromkeys(nazwy), od, do, args.katalog, args.scisle)


if __name__ == "__main__":
    sys.exit(main())
