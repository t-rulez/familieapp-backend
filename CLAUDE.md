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
`days[].people[Navn] = {skole:[…], annet:[…]}`. Frontend (`uke.js`) er avhengig av dette
formatet. Endres det, må frontend endres samtidig.

- **Cache:** `_week_cache` ligger i minnet, med nøkkel `bruker:mandag`. Den hoppes bare over med
  `refresh=true` og nullstilles ved redeploy. Vanlig bytte mellom uker er gratis. Bare
  «Oppdater» og auto-oppdateringen i frontend koster tokens.
- **Rekkefølge innenfor meldingsbudsjettet:** ekte ukeplaner først, så øvrig skole-innhold, så
  resten. Ukeplaner gjenkjennes med heuristikken **«minst tre ukedagsnavn i teksten»**.
  Budsjettoverskridelse gir `continue`, ikke `break`.
- **`skole_funnet`:** AI-en skriver først en ren opptelling av hva ukeplanen inneholder, og
  fordeler den deretter på dagene. Feltet ignoreres av appen, men logges. Avviker opptellingen
  fra `days`, ligger feilen i fordelingen og ikke i lesingen. Det var denne strukturelle grepet
  som løste dekningsproblemet, ikke formuleringene i prompten.
- **Thinking/effort:** står på `output_config.effort = "low"`, og det **må** være satt et
  eksplisitt tak. Uten effort-innstilling brukte modellen hele `max_tokens` på tenking og
  returnerte et tomt svar.

### Ukeoversikt-prompten er i en skjør balanse: prøvd og forkastet

Ikke gjenta disse uten en god grunn:

| Forsøk | Resultat |
|---|---|
| `effort: "medium"` | 200 OK, men helt tomt skoleinnhold. Testen var **ugyldig** (se «tapt linje» under), så dette er egentlig uavklart |
| Ingen effort-innstilling | Hele `max_tokens` gikk til tenking, og svaret ble tomt |
| Sterkere og mer insisterende prompt-tekst | Svaret svingte: vektla jeg lekser, forsvant timeplan-aktivitetene, og omvendt |
| Nøkkelordsøk («ukeplan», «lekser») for å finne ukeplaner | Feiler fordi Claude Vision sine bildebeskrivelser nevner disse ordene meta-tekstlig. Bruk tre-ukedager-heuristikken |
| `break` ved budsjettoverskridelse | Én for stor melding stoppet alt bak den |
| Sortering kun etter nyhet | Ukeplanen tapte budsjettet mot ferskere støy (bilder, oppskrifter) |

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
- **home-control** (varmestyring: Mill, MELCloud, Adax), som nås via proxyen `/heating/*`. Den
  legger på `X-API-Token` på serversiden. Tokenet ligger **bare** som `HEATING_API_TOKEN` i
  Railway og skal aldri deles i chat. Blir det delt, skal det byttes. Adax tåler én skriving per
  30 sekunder og kan bruke 1–10 minutter på å endre seg fysisk.

## Vær (`weather_service.py`)

- **Bevisst holdt utenfor AI-en.** `/weather/week` er rent HTTP + database og bruker null
  tokens, slik at det ikke kan forstyrre ukeoversikt-prompten.
- Det som lagres er **siste varsel laget mens dagen pågikk**, ikke en observasjon av hva været
  faktisk ble. Bare inneværende og neste uke beholdes. Rader eldre enn mandag i inneværende uke
  slettes.
- Værhistorikk finnes bare fra 20.11.2026. Tidligere dager kan ikke hentes fra locationforecast.
  Historisk vær ville krevd MET sitt Frost-API, som har egen innlogging.
- **MET-vilkårene er bindende:** identifiserende User-Agent (ellers blokkeres trafikken),
  respekter `Expires` og `If-Modified-Since`, kreditering etter CC BY 4.0, og maks fire desimaler
  i lat/lon.

## Showbie

- Hver ny fil sendes gjennom Claude Vision (opptil 10 forhåndsvisningssider). Det skjer bare én
  gang per fil (`existing_ids`), så i stabil drift er kostnaden null. Forbruket logges i
  `api_usage` med purpose `"vision"`. Se statistikksiden før noe optimaliseres.
- Showbie-tokenet utløper etter ca. seks måneder og må fornyes manuelt.
- `ai_auto_skip` markerer ukeplaner som lavt prioritert («utenfor interesseområde»). Det påvirker
  feeden, men ikke ukeoversikten, som ignorerer status.

## Kjente fallgruver

- **Railway fletter logglinjer** fra samtidige forespørsler, så flerlinjers loggblokker blir
  stokket om. Bruk enlinjers logging, og ikke stol på strukturen i en flerlinjers blokk. Dette har
  før ført til en feil konklusjon (at en fil manglet).
- **200 OK betyr ikke at datastien virker.** Under en refaktorering forsvant
  `msg_context += …`. AI-en fikk tom kontekst og svarte 200 OK med tomt innhold, uten feilmelding.
  Sjekk at dataene faktisk kommer fram.

## Gjenstår

- Debug-loggingen `[UKESOVERSIKT-DEBUG]` (fire korte linjer) står bevisst på. Fjern den når
  ukeoversikten har vært stabil en stund.
- Test `effort: "medium"` på nytt med et rent grunnlag. Forrige test var ugyldig.
- Vurder å kutte kostnaden for Showbie Vision hvis tallene tilsier det, i denne rekkefølgen:
  1. Senk sidetaket fra 10 til 3–4.
  2. Billig klassifisering med Haiku først («skoleinfo eller elevinnlevering?»).
  3. Hopp over åpenbare mønstre som «Foto fra kameraet» og «Skjermbilde». Dette er noe
     risikabelt, siden ukeplanen kan være et bilde.
- Kosmetisk: `whatsapp_status` er definert to ganger i `main.py` (ca. linje 1225). Harmløst.

## Arbeidsflyt

Samme som i frontend (se `familieapp/CLAUDE.md`):
- norsk overalt
- **bekreft logikken før du koder** alt som ikke er trivielt
- utvikle på oppgitt branch, spør før PR og merge, og bruk squash-merge mot `main`

Railway deployer trolig automatisk fra `main` (ikke verifisert). Test lokalt med
`python -m py_compile *.py` når det ikke finnes database eller nøkler. Test prompt- og
budsjettlogikk isolert før levering. Endringer i API-formatet må koordineres med `familieapp`.
