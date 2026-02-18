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

How to use?
1. Fetch FX data
- pull the repo
- create a venv to src in from requirements_src.txt
- run the src/fetch_historical_fx_data.py to get the historical json files
- run the src/fetch_new_fx_data.py to get the lates year's json file

2. Notebooks
- create a venv to notebooks in from requirements_notebooks.txt
- don't forget to register your venv kernel's and restart vs code
    - https://web.archive.org/web/20240430135149/https://anbasile.github.io/posts/2017-06-25-jupyter-venv/
- run notebooks