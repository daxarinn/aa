# AA fundaskrá scraper

Fyrsta útgáfa af scraper fyrir:

- `https://aa.is/aa-fundir/allir-fundir`
- `https://www.fjarfundir.org/`

Source-forgangur þegar sama fundi er lýst á fleiri en einum stað:

1. `al-anon.is`
2. `coda.is`
3. `fjarfundir.org`
4. `12sporahusid.is`
5. `gula.is`
6. `aa.is`

Skrifar gögn í SQLite og býður upp á:

- CSV útflutning
- `pandas.DataFrame.to_clipboard()` fyrir Excel-yfirferð
- einfalt Flask-yfirlit fyrir síma og skjáborð

## Uppsetning

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

## Sækja og vista gögn

```powershell
python main.py scrape
```

Sjálfgefin útgögn:

- SQLite: `data/meetings.sqlite`
- CSV: `exports/meetings_latest.csv`

## Afrita í klemmuspjald

```powershell
python main.py scrape --copy
```

eða ef gagnagrunnurinn er þegar til:

```powershell
python main.py preview --copy
```

## Keyra einfalt yfirferðarviðmót

```powershell
python main.py serve
```

Opnar svo `http://127.0.0.1:5000`.

`serve` býður nú upp á síur fyrir meðal annars source, vikudag, félag, fundarform (`Staðfundur` eða `Fjarfundur`), kyn, aðgang, staðsetningu, tíma og frjálsa textaleit.

Í vefviðmótinu er einnig hægt að skipta á milli:

- `Línuleg sýn`
- `Vikusýn`
- `Staðamöppun`

`Staðamöppun` gerir þér kleift að sameina mismunandi rithætti á staðsetningum undir einu canonical heiti án þess að breyta raw scrape gögnunum. Þar geturðu líka skráð gælunafn á canonical staðsetningu, til dæmis `Gula húsið`, `Holtagarðar` eða `Gúttó`.

Í línulegri sýn og vikusýn birtist einnig linkur á upprunasíðuna ásamt raw provenance-bút úr scrape-uðu línunni.
