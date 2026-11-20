# CLAUDE.md – familieapp-backend

Instruksjoner og kontekst for Claude Code i dette repoet. Frontend ligger i søsterrepoet
`familieapp` (se `CLAUDE.md` der for prosjektoversikt, enheter og arbeidsflyt).

> `README.md` her er **utdatert**. Den beskriver bare den første Spond-versjonen.
> Stol på koden og denne filen.

## Om prosjektet

Backend for **Brief**, en privat familieassistent for én familie (Gustav, Rasmus, Kristin og
Truls). Den henter meldinger fra flere kilder, lar Claude klassifisere og oppsummere dem, og
serverer meldingsfeed, AI-chat og en strukturert ukesoversikt til frontend. Eieren har ikke
kodebakgrunn.

## Stack og drift

- **Python / FastAPI**, `uvicorn main:app` (`Procfile`, `railway.toml`). Hostes på **Railway**:
  `https://familieapp-backend.up.railway.app`, med helsesjekk på `/`.
- **PostgreSQL** (Railway) via `asyncpg`. Tabellene opprettes automatisk ved oppstart
  (`init_db`, `init_user_tables`, `init_ai_tables`, `init_push_tables`). Det finnes ingen
  migreringsverktøy. Nye kolonner legges til med `ALTER TABLE … ADD COLUMN IF NOT EXISTS` i
  init-funksjonene.
- Ingen tester og ingen CI.

## Filer

| Fil | Hva |
|---|---|
| `main.py` | FastAPI-appen: alle endepunkter, bakgrunnsløkker (`sync_loop`, `evening_summary_loop`), synk per bruker, AI-chat, ukesoversikt, proxyer for WhatsApp og varme |
| `database.py` | Tabellene `messages`, `user_profile`, `api_usage` og `push_subscriptions`, og spørringer |
| `users.py` | Tabellene `users` og `user_settings` (kildeoppsett, familiekontekst, API-nøkkel per bruker) |
| `auth.py` | bcrypt, JWT (`JWT_SECRET`), kryptering av lagrede hemmeligheter (`ENCRYPTION_KEY`) |
| `ai_service.py` | Klassifisering og scoring av meldinger med Claude |
| `spond_service.py` | Spond-integrasjon |
| `showbie_service.py` | Showbie (reverse-engineered API + Claude Vision for ukeplaner) |
| `email_service.py` | Skoleplattform via IMAP + PDF-uttrekk |
| `caldav_service.py` | iCloud-kalender via CalDAV |
| `family_profile.py` | Kobler Spond- og kalendergrupper til riktig barn ut fra familieprofilen (lagnavn i anførselstegn under `**Navn**:`). Gjetter aldri, og ingen lagnavn er hardkodet |
| `weather_service.py` | Værvarsel fra MET Norway. Dagens vær lagres løpende og «fryses» når dagen er over |
| `push_service.py` | Web push (VAPID) |

## Viktige endepunkter

Alle unntatt `/`, `/auth/*` og `/push/vapid-key` krever `Authorization: Bearer <JWT>`.

- **Auth:** `/auth/register`, `/auth/login`, `/auth/me`, `/auth/change-password`,
  `/auth/forgot-password` (Resend).
- **Meldinger:** `GET /messages`, `PATCH /messages/{id}`, `POST /sync`, `POST /fetch-history`,
  `POST /messages/ingest` (fra WhatsApp-tjenesten, med headeren `x-api-token` = `API_TOKEN`).
- **AI:** `POST /ai/chat`, `GET /ai/week-overview?week_offset=0|1&refresh=` (ukeplanen),
  `POST /ai/learn`, `GET/POST /ai/history`.
- **Ukeplan-støtte:** `GET /weather/week`, `GET/PUT /heating/level`, `GET /heating/devices`,
  `GET /heating/timers`.
- **Innstillinger og annet:** `/settings`, `/profile`, `/stats/api`, `/push/*`, `/whatsapp/*`,
  `/showbie/test`, `/caldav/calendars`, `/admin/*` (engangsoppryddinger).

### `/ai/week-overview`
Bygger en prompt av meldinger fra de siste 14 dagene, kalenderen, familiekonteksten
(`family_context`) og lært kontekst (`ai_learned_context`). Claude svarer med strukturert JSON:
`days[].people[Navn] = {skole:[…], annet:[…]}`. Resultatet caches **i minnet** (`_week_cache`,
nøkkel `bruker:mandag`) og forsvinner ved restart. `refresh=true` tvinger ny generering.
Frontend (`uke.js`) er avhengig av dette formatet. Endres det, må frontend endres samtidig.

## Claude API

- Kalles direkte med `httpx` mot `https://api.anthropic.com/v1/messages`, ikke via SDK.
- API-nøkkelen hentes **per bruker** fra `user_settings.claude_api_key` (`_get_api_key`), med
  `ANTHROPIC_API_KEY` som reserve.
- Modeller i bruk: `claude-sonnet-5` (chat, ukesoversikt, klassifisering, Showbie Vision,
  kveldsoppsummering) og `claude-haiku-4-5-20251001` (`/ai/learn`). Pensjonerte modeller gir 404.
  Oppdater alle steder samtidig (`grep -n "claude-" *.py`).
- Forbruket logges i tabellen `api_usage` (`/stats/api`).

## Bakgrunnsjobber

- `sync_loop`: synker alle aktive brukere hvert 30. minutt på dagen og hvert 90. minutt om natten
  (23–06, Oslo-tid), og oppdaterer været. Nye meldinger → AI-behandling → push-varsel.
- `evening_summary_loop`: kveldsoppsummering av morgendagen som push, på tidspunktet brukeren
  har valgt (`evening_summary_time`, standard 20:00).

## Miljøvariabler (Railway)

Bare navnene står her, aldri verdiene. Verdiene ligger i Railway, ikke i repoet.

`DATABASE_URL`, `JWT_SECRET`, `ENCRYPTION_KEY`, `API_TOKEN`, `ANTHROPIC_API_KEY`,
`RESEND_API_KEY`, `VAPID_PUBLIC_KEY`, `VAPID_PRIVATE_KEY`, `VAPID_EMAIL`, `WHATSAPP_SERVICE_URL`,
`HEATING_API_URL`, `HEATING_API_TOKEN`, `WEATHER_LAT`, `WEATHER_LON`, `WEATHER_USER_AGENT`
(valgfri), og e-postoppsett som `EMAIL_HOST`/`EMAIL_*` eller nummerert `EMAIL_1_*`…`EMAIL_9_*`.

## Eksterne tjenester utenfor dette repoet

- **WhatsApp-tjenesten** (Node.js/Baileys, egen Railway-tjeneste og eget repo, som ikke er
  tilgjengelig her). Den sender meldinger til `/messages/ingest` og varsler frakobling til
  `/integrations/whatsapp/status`.
- **home-control** (varmestyring: Mill, MELCloud, Adax), som nås via `HEATING_API_URL`. Adax kan
  bruke opptil 10 minutter på å endre seg fysisk.

## Arbeidsflyt

Samme som i frontend: norsk overalt, utvikle på oppgitt branch, spør før PR og merge, og bruk
squash-merge mot `main`. Railway deployer trolig automatisk fra `main` (ikke verifisert). Test
lokalt med `python -m py_compile *.py` når det ikke finnes database eller nøkler. Endringer i
API-formatet må koordineres med `familieapp`.
