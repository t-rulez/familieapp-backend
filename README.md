# Familie-backend

Python/FastAPI backend som henter meldinger fra Spond og serverer dem til familie-webappen.

## Filstruktur

```
familieapp-backend/
├── main.py            # FastAPI app, endepunkter, bakgrunnssynkronisering
├── spond_service.py   # Spond-integrasjon og datanormalisering
├── models.py          # Pydantic-modeller
├── requirements.txt   # Python-avhengigheter
├── Procfile           # Railway startkommando
├── railway.toml       # Railway-konfig
├── .env.example       # Mal for miljøvariabler
├── .gitignore         # Ekskluderer .env fra Git
└── README.md
```

## API-endepunkter

| Metode | URL | Beskrivelse |
|--------|-----|-------------|
| GET | `/` | Helsestatus og siste synktidspunkt |
| GET | `/messages` | Hent alle meldinger |
| PATCH | `/messages/{id}` | Oppdater status (read/skipped/unread) |
| POST | `/sync` | Tving manuell synkronisering |

Alle endepunkter krever headeren `x-api-token: <din token>` hvis `API_TOKEN` er satt.

---

## Deploy til Railway – steg for steg

### Steg 1: Lag GitHub-repo

1. Gå til github.com → "New repository"
2. Navn: `familieapp-backend`
3. Gjør den **privat**
4. Ikke legg til README
5. Klikk "Create repository"

### Steg 2: Push koden

```bash
cd familieapp-backend
git init
git add .
git commit -m "første versjon – spond integrasjon"
git branch -M main
git remote add origin https://github.com/DITT-BRUKERNAVN/familieapp-backend.git
git push -u origin main
```

### Steg 3: Opprett Railway-prosjekt

1. Gå til **railway.app** og logg inn med GitHub
2. Klikk **"New Project"**
3. Velg **"Deploy from GitHub repo"**
4. Velg `familieapp-backend`
5. Railway oppdager Python automatisk og bygger appen

### Steg 4: Legg til miljøvariabler

I Railway-dashbordet, gå til prosjektet ditt → fanen **"Variables"** → klikk **"New Variable"**:

| Variabel | Verdi |
|----------|-------|
| `SPOND_USERNAME` | Din Spond e-postadresse |
| `SPOND_PASSWORD` | Ditt Spond-passord |
| `API_TOKEN` | Et langt tilfeldig passord du lager selv (f.eks. `abc123xyz789...`) |

> Tips: Lag en sterk API_TOKEN med: `openssl rand -hex 32`

### Steg 5: Hent Railway-URLen din

1. I Railway: gå til prosjektet → **"Settings"** → **"Networking"**
2. Klikk **"Generate Domain"**
3. Du får en URL som `familieapp-backend-production-xxxx.up.railway.app`
4. Test at den fungerer: åpne `https://din-url.up.railway.app/` i nettleseren
   - Du skal se: `{"status": "ok", "last_sync": null}`

### Steg 6: Koble webappen til backenden

Åpne Safari på iPhone → gå til familie-webappen din → åpne **Innstillinger-fanen**.

Alternativt: åpne nettleserens konsoll (F12 på desktop) og kjør:

```javascript
localStorage.setItem('api_url', 'https://din-url.up.railway.app');
localStorage.setItem('api_token', 'din-api-token-her');
location.reload();
```

Appen vil nå hente ekte data fra Spond!

---

## Teste lokalt (valgfritt)

```bash
# Installer avhengigheter
pip install -r requirements.txt

# Lag .env-fil
cp .env.example .env
# Fyll inn dine verdier i .env

# Start serveren
uvicorn main:app --reload

# Test i nettleseren
open http://localhost:8000
open http://localhost:8000/messages
```

---

## Fase 3 (kommer): Skolemelding og Showbie

Neste steg er å legge til scraping av Skolemelding og Showbie i samme backend.
