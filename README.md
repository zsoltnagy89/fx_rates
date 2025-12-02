# dokumentacio:
- Problemafelvetes --> törtenelmi fx adatok sek-röl szinte alig elehetöek, vagy fizetösek. Tözsdei kereskedes alap valutaja. SEK, ami devizakitettseget jelent USA, EU  es egyeb Nordik piacokon kereskedeskor.
- Erre megoldas ez a pipeline:
    - fetcheli a historikus adatokat (1x kell megtenni)
    - fetcheli a napi adatokat (naponta, v ritkabban)
    - json adatokat betölti egy duckdb fileba
    - dbt transzformacio soran elöallnak a vegleges SEK-re vonatkoztatott valuta arfolyamok 1999-01-01-töl
- Lehetöve teszi h legaölabb a devizakitettseg nagysagrendjet meg tudjuk becsulni historikus adatok alapjan.

Projekt struktura:
- databases --> duckdb file
- jsons --> historikus es napi frissulö FX adatok JSON-ban tarolva
- notebooks --> playground quick-and-dirty POC tesztelese
- src --> vegleges production ready python scriptek
- .env.example --> egy demo .env file, hogy milyen key-ekre van szukseg a futatashoz
- requirements.txt --> venv blueprint