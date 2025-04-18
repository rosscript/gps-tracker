# Tracker Targa Telematics

Un'applicazione Python per monitorare la posizione di un veicolo utilizzando l'API di Targa Telematics e inviare aggiornamenti su Telegram con mappe interattive.

## Funzionalità

- Monitoraggio in tempo reale della posizione del veicolo
- Visualizzazione delle ultime posizioni su mappe interattive (fino a 20 posizioni)
- Generazione automatica di mappe con visualizzazione del percorso
- Geocoding delle coordinate per ottenere indirizzi
- Salvataggio dei dati in formato CSV e TXT
- Invio automatico di aggiornamenti su Telegram ogni 5 minuti
- Invio automatico dei file di riepilogo ogni 60 minuti (se ci sono aggiornamenti)
- Pulsanti inline per richiedere mappe interattive HTML
- Pulsanti per visualizzare mappe con percorsi recenti (fino a 20 posizioni)
- Gestione delle variabili d'ambiente tramite file .env

## Requisiti

- Python 3.x
- Chrome/Chromium (per la generazione delle mappe)
- Ambiente virtuale (venv)

## Installazione

1. Clona il repository o scarica i file

2. Crea e attiva un ambiente virtuale:
```bash
# Windows
python -m venv venv
.\venv\Scripts\activate

# Linux/Mac
python -m venv venv
source venv/bin/activate
```

3. Installa le dipendenze:
```bash
pip install -r requirements.txt
```

4. Crea un file `.env` nella directory principale con le seguenti variabili:
```
# Configurazione Targa Telematics
UNIT_ID=your_unit_id
TARGA_TOKEN=your_targa_token
AUTH_TOKEN=your_auth_token

# Configurazione Telegram
TELEGRAM_TOKEN=your_telegram_token
TELEGRAM_CHAT_ID=your_chat_id
```

## Utilizzo

Per avviare il programma:
```bash
python tracker.py
```

Il programma:
- Si avvierà immediatamente con un primo fetch dei dati
- Eseguirà aggiornamenti automatici ogni 5 minuti
- Mostrerà fino a 20 posizioni recenti sulla mappa con zoom sulla posizione attuale
- Salverà i dati in `positions_log.csv` e `positions_log.txt`
- Invierà gli aggiornamenti sul canale Telegram configurato con mappe e pulsanti interattivi
- Invierà i file di riepilogo (CSV e TXT) ogni 12 check (circa 60 minuti)

## Funzionalità interattive su Telegram

- **Mappa HTML**: Richiede l'invio di una mappa HTML interattiva apribile nel browser
- **Mappa con percorso**: Visualizza la mappa con il percorso delle ultime posizioni (fino a 20)
- I pulsanti scompaiono dopo l'uso per mantenere l'interfaccia pulita

## Note

- Assicurati di avere tutte le credenziali necessarie configurate nel file `.env`
- Il programma utilizza Nominatim per il geocoding, rispetta i termini di utilizzo del servizio
- I token di autenticazione hanno una durata limitata, potrebbero essere necessari aggiornamenti periodici
- È necessario Chrome/Chromium per la generazione delle immagini delle mappe
- Ogni notifica di posizione include una mappa con zoom ravvicinato sulla posizione attuale 