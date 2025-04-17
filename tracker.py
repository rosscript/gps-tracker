import requests
import pandas as pd
import schedule
import time
from datetime import datetime
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import json
import os
from dotenv import load_dotenv

# Carica le variabili d'ambiente dal file .env
load_dotenv()

# Configurazione
unit_id = os.getenv("UNIT_ID")
token = os.getenv("TARGA_TOKEN")
base_url = f"https://fleet.targatelematics.com/t2/api/followUnit/recentPositions/{unit_id}/{token}/30"

# Configurazione Telegram
#BOT CROLLA
#TELEGRAM_TOKEN = "7394823627:AAH6ZJRik8ITpsvV8S81YIUtFpsFuGw7sRg"
#BOT ROSATO
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

# Token di autorizzazione
auth_token = os.getenv("AUTH_TOKEN")

headers = {
    "Authorization": auth_token,
    "Accept": "*/*",
    "User-Agent": "Mozilla/5.0",
    "X-Requested-With": "XMLHttpRequest"
}

csv_file = "positions_log.csv"
txt_file = "positions_log.txt"

def debug_telegram_channel():
    try:
        # Prova a ottenere informazioni sul canale
        response = requests.post(
            f"{TELEGRAM_API_URL}/getChat",
            json={"chat_id": CHAT_ID}
        )
        print("\nDebug informazioni canale:")
        print(f"Status code: {response.status_code}")
        print(f"Risposta API: {response.text}")
        
        if response.status_code == 200:
            chat_info = response.json()
            print("\nInformazioni canale:")
            print(f"Tipo: {chat_info.get('result', {}).get('type')}")
            print(f"Titolo: {chat_info.get('result', {}).get('title')}")
            print(f"Username: {chat_info.get('result', {}).get('username')}")
            return True
        return False
    except Exception as e:
        print(f"Errore nel debug del canale: {e}")
        return False

def test_telegram_connection():
    try:
        # Prima verifichiamo le informazioni del canale
        if not debug_telegram_channel():
            print("❌ Impossibile ottenere informazioni sul canale")
            return False

        # Prova a inviare un messaggio di test
        response = requests.post(
            f"{TELEGRAM_API_URL}/sendMessage",
            json={
                "chat_id": CHAT_ID,
                "text": "🔍 Test di connessione Telegram\nSe ricevi questo messaggio, la connessione funziona correttamente!"
            }
        )
        if response.status_code == 200:
            print("✅ Test di connessione Telegram riuscito!")
            return True
        else:
            print(f"❌ Errore nel test di connessione Telegram: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Errore nel test di connessione Telegram: {e}")
        return False

def send_telegram_files():
    try:
        # Prima verifichiamo la connessione
        if not test_telegram_connection():
            print("Impossibile inviare i file: connessione Telegram non funzionante")
            return

        # Prepara i file per l'invio
        media = []
        timestamp = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        
        # Aggiungi il file CSV (primo file con descrizione)
        with open(csv_file, 'rb') as csv:
            media.append({
                'type': 'document',
                'media': f'attach://{csv_file}',
                'caption': f"📊 Aggiornamento posizioni - {timestamp}"  # Descrizione solo nel primo file
            })
            files = {csv_file: csv}
            
            # Aggiungi il file TXT (secondo file senza descrizione)
            with open(txt_file, 'rb') as txt:
                media.append({
                    'type': 'document',
                    'media': f'attach://{txt_file}'
                    # Nessun campo caption per il secondo file
                })
                files[txt_file] = txt
                
                # Invia entrambi i file in un unico messaggio
                response = requests.post(
                    f"{TELEGRAM_API_URL}/sendMediaGroup",
                    data={
                        'chat_id': CHAT_ID,
                        'media': json.dumps(media)
                    },
                    files=files
                )
                
                if response.status_code == 200:
                    print("File inviati con successo su Telegram in un unico messaggio")
                else:
                    print(f"Errore nell'invio dei file: {response.text}")
                    
    except Exception as e:
        print(f"Errore nell'invio dei file su Telegram: {e}")

def get_address(lat, lon):
    geolocator = Nominatim(user_agent="tracker_app")
    try:
        print(f"Richiesta geocoding per coordinate: {lat}, {lon}")
        location = geolocator.reverse(f"{lat}, {lon}", language='it')
        if location:
            address = location.raw.get('address', {})
            result = {
                'via': address.get('road', ''),
                'comune': address.get('city', '') or address.get('town', '') or address.get('village', ''),
                'provincia': address.get('state', '')
            }
            print(f"Indirizzo trovato: {result}")
            return result
        else:
            print("Nessun risultato trovato per queste coordinate")
    except GeocoderTimedOut:
        print(f"Timeout nel geocoding per le coordinate {lat}, {lon}")
    except Exception as e:
        print(f"Errore nel geocoding: {e}")
    return {'via': '', 'comune': '', 'provincia': ''}

def save_to_txt(df):
    try:
        with open(txt_file, 'w', encoding='utf-8') as f:
            f.write("REGISTRO POSIZIONI VEICOLO\n")
            f.write("=" * 50 + "\n\n")
            
            for _, row in df.iterrows():
                f.write(f"Data/Ora: {row['timestamp']}\n")
                f.write(f"Coordinate: {row['lat']}, {row['lon']}\n")
                f.write(f"Velocità: {row['speed']} km/h\n")
                f.write(f"Chilometraggio: {row['mileage']} km\n")
                f.write(f"Via: {row['via']}\n")
                f.write(f"Comune: {row['comune']}\n")
                f.write(f"Provincia: {row['provincia']}\n")
                f.write(f"Batteria: {row['battery']} V\n")
                f.write(f"Descrizione: {row['description']}\n")
                f.write("-" * 50 + "\n\n")
                
        print(f"File {txt_file} aggiornato con successo")
    except Exception as e:
        print(f"Errore nel salvataggio del file .txt: {e}")

def fetch_and_save():
    try:
        # Parametri della richiesta
        params = {
            "advanced": "false",
            "_dc": int(time.time() * 1000),
            "page": 1,
            "start": 0,
            "limit": 25
        }

        # Utilizziamo una sessione per gestire meglio le richieste HTTP
        session = requests.Session()
        response = session.get(base_url, headers=headers, params=params)
        print(f"Status code: {response.status_code}")
        
        if response.status_code == 401:
            print("Errore: Token non valido o scaduto. Aggiorna il token di autorizzazione.")
            return
            
        response.raise_for_status()
        data = response.json()["data"]["positions"]
        print(f"Trovate {len(data)} posizioni")

        # Carica i dati esistenti se il file CSV esiste
        try:
            existing_df = pd.read_csv(csv_file)
            print(f"Trovate {len(existing_df)} posizioni esistenti")
        except FileNotFoundError:
            existing_df = pd.DataFrame()
            print("Nessun file CSV esistente trovato")

        # Estrazione dei dati interessanti
        positions = []
        for p in data:
            print(f"\nElaborazione posizione: {p['timestamp']}")
            
            # Verifica se la posizione esiste già
            is_existing = False
            if not existing_df.empty:
                existing_pos = existing_df[
                    (existing_df['lat'] == p['lat']) & 
                    (existing_df['lon'] == p['lon'])
                ]
                if not existing_pos.empty:
                    print("Posizione già esistente, riutilizzo i dati")
                    position = {
                        "timestamp": p["timestamp"],
                        "lat": p["lat"],
                        "lon": p["lon"],
                        "speed": p["speed"],
                        "mileage": p["mileage"],
                        "description": p["description"],
                        "battery": p["battExtVolts"],
                        "fix": p["fix"],
                        "hdop": p["hdop"],
                        "via": existing_pos.iloc[0]['via'],
                        "comune": existing_pos.iloc[0]['comune'],
                        "provincia": existing_pos.iloc[0]['provincia']
                    }
                    is_existing = True
            
            if not is_existing:
                print("Nuova posizione, richiedo geocoding")
                address_info = get_address(p["lat"], p["lon"])
                position = {
                    "timestamp": p["timestamp"],
                    "lat": p["lat"],
                    "lon": p["lon"],
                    "speed": p["speed"],
                    "mileage": p["mileage"],
                    "description": p["description"],
                    "battery": p["battExtVolts"],
                    "fix": p["fix"],
                    "hdop": p["hdop"],
                    "via": address_info['via'],
                    "comune": address_info['comune'],
                    "provincia": address_info['provincia']
                }
                time.sleep(1)  # Aggiungo un ritardo di 1 secondo tra le richieste di geocoding
            
            positions.append(position)

        new_df = pd.DataFrame(positions)
        print("\nRiepilogo dati salvati:")
        print(new_df[['timestamp', 'via', 'comune', 'provincia']].to_string())

        # Unisci i dati esistenti con i nuovi
        if not existing_df.empty:
            merged_df = pd.concat([existing_df, new_df])
            # Rimuovi duplicati basati su lat e lon
            merged_df.drop_duplicates(subset=['lat', 'lon'], keep='last', inplace=True)
        else:
            merged_df = new_df

        # Salva il CSV con l'encoding UTF-8
        merged_df.to_csv(csv_file, index=False, encoding="utf-8")
        print(f"\n[{datetime.now()}] Dati aggiornati, {len(new_df)} nuove posizioni salvate.")
        
        # Salva anche in formato .txt
        save_to_txt(merged_df)
        
        # Invia i file su Telegram
        send_telegram_files()
        
    except requests.exceptions.RequestException as e:
        print(f"Errore nella richiesta: {e}")
    except Exception as e:
        print(f"Errore imprevisto: {e}")
    finally:
        if 'session' in locals():
            session.close()

# Pianifica ogni 15 minuti
schedule.every(15).minutes.do(fetch_and_save)

# Avvio ciclo
print("Inizio monitoraggio veicolo ogni 15 minuti...")
fetch_and_save()  # Primo fetch subito
while True:
    schedule.run_pending()
    time.sleep(1)