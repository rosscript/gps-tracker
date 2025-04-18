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
import hashlib
import io
import folium
from PIL import Image
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import tempfile
import uuid

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

# Percorso dei file
csv_file = "positions_log.csv"
txt_file = "positions_log.txt"
map_file = "last_position_map.png"
route_map_file_5 = "route_map_5.png"
route_map_file_20 = "route_map_20.png"
interactive_map_file = "last_position_map.html"  # File per la mappa interattiva

# Flag per tenere traccia del primo avvio
primo_avvio = True
# Hash dell'ultimo invio
last_sent_hash = None
# Contatore per tracciare le richieste (invio ogni 12 check)
check_counter = 0
# Flag per indicare se ci sono stati aggiornamenti dall'ultimo invio
updates_since_last_send = False
# Timestamp dell'ultima generazione delle mappe di percorso
last_route_map_generation = 0

# Dizionario per tenere traccia degli ultimi callback_data generati
route_callbacks = {}
# Insieme per tenere traccia dei callback in elaborazione (evita duplicati)
processing_callbacks = set()
# Dizionario per tenere traccia dei messaggi e relativi pulsanti
message_buttons = {}  # { message_id: { callback_data: num_positions } }
# Dizionario per tenere traccia delle richieste di mappe HTML
html_map_callbacks = {}  # { callback_data: {"file": file_path, "type": "position/route", "num_positions": num} }

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
                "text": "Test di connessione Telegram\nSe ricevi questo messaggio, la connessione funziona correttamente."
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

def count_available_positions():
    """Conta quante posizioni sono disponibili nel file CSV"""
    try:
        if not os.path.exists(csv_file):
            return 0
        positions_df = pd.read_csv(csv_file)
        return len(positions_df)
    except Exception as e:
        print(f"Errore nel conteggio delle posizioni: {e}")
        return 0

def generate_route_map(num_positions, output_file=None, interactive_file=None):
    """Genera una mappa con l'itinerario delle ultime posizioni"""
    if output_file is None:
        output_file = route_map_file_5 if num_positions == 5 else route_map_file_20
        
    if interactive_file is None:
        interactive_file = f"route_map_{num_positions}.html"
        
    try:
        print(f"Generazione mappa del percorso con ultime {num_positions} posizioni...")
        
        # Carica i dati delle posizioni
        try:
            positions_df = pd.read_csv(csv_file)
            # Ordina per timestamp in ordine decrescente
            positions_df = positions_df.sort_values(by='timestamp', ascending=False)
            # Prendi solo le ultime n posizioni
            positions_df = positions_df.head(num_positions)
            # Riordina per timestamp in ordine crescente per il percorso
            positions_df = positions_df.sort_values(by='timestamp', ascending=True)
        except Exception as e:
            print(f"Errore nel caricamento delle posizioni: {e}")
            return False
            
        if len(positions_df) < 2:
            print("Non ci sono abbastanza posizioni per generare un percorso")
            return False
        
        # Estrai le coordinate per calcolare il migliore zoom
        lats = positions_df['lat'].tolist()
        lons = positions_df['lon'].tolist()
        
        # Calcola il centro della mappa
        center_lat = sum(lats) / len(lats)
        center_lon = sum(lons) / len(lons)
        
        # Calcola la distanza massima tra i punti per determinare lo zoom appropriato
        max_lat_diff = max(lats) - min(lats)
        max_lon_diff = max(lons) - min(lons)
        
        # Determina lo zoom in base alla distanza massima
        # Più piccolo è il valore, più ampio è lo zoom
        # Converti la differenza in gradi a una stima di zoom
        zoom_level = 18  # Zoom massimo di default
        if max_lat_diff > 0.05 or max_lon_diff > 0.05:
            zoom_level = 12
        elif max_lat_diff > 0.01 or max_lon_diff > 0.01:
            zoom_level = 14
        elif max_lat_diff > 0.005 or max_lon_diff > 0.005:
            zoom_level = 15
        elif max_lat_diff > 0.001 or max_lon_diff > 0.001:
            zoom_level = 16
        
        # Crea una mappa centrata sulla posizione media con zoom adattivo
        m = folium.Map(location=[center_lat, center_lon], zoom_start=zoom_level, tiles='CartoDB positron')
        
        # Crea una lista di punti per il percorso
        route_points = []
        
        # Aggiungi marker per ogni posizione
        for index, position in enumerate(positions_df.iterrows()):
            position_idx, position = position  # Decompose the tuple
            lat = position['lat']
            lon = position['lon']
            timestamp = position['timestamp']
            speed = position['speed']
            via = position['via'] if pd.notna(position['via']) else ''
            comune = position['comune'] if pd.notna(position['comune']) else ''
            
            # Formatta il timestamp per ottenere solo l'orario (HH:MM:SS)
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                formatted_time = dt.strftime('%H:%M:%S')
                formatted_full_time = dt.strftime('%d/%m/%Y %H:%M:%S')
            except:
                formatted_time = "??:??:??"
                formatted_full_time = timestamp
                
            # Popup con informazioni
            popup_text = f"Ora: {formatted_full_time}<br>Velocità: {speed} km/h<br>Via: {via}, {comune}"
            
            # Aggiungi il punto alla lista dei punti del percorso
            route_points.append([lat, lon])
            
            # Aggiungi il marker
            folium.CircleMarker(
                [lat, lon], 
                radius=6,
                color='red',
                fill=True,
                fill_color='red',
                fill_opacity=0.7,
                popup=popup_text
            ).add_to(m)
            
            # Determina se dobbiamo mostrare l'etichetta di orario per questa posizione
            # Per la mappa di 5 posizioni: mostra tutte le etichette
            # Per la mappa di 20 posizioni: mostra solo ogni 5 posizioni e la prima/ultima
            show_label = (num_positions <= 5) or (index % 5 == 0) or (index == len(positions_df) - 1) or index == 0
            
            if show_label:
                # Aggiungi label con l'orario
                folium.map.Marker(
                    [lat, lon],
                    icon=folium.DivIcon(
                        icon_size=(60, 20),
                        icon_anchor=(30, -10),  # Spostato più in basso (-10 invece di 0)
                        html=f'<div style="font-size: 10pt; color: black; background-color: white; border: 1px solid black; border-radius: 3px; padding: 1px 3px; text-align: center;">{formatted_time}</div>'
                    )
                ).add_to(m)
        
        # Aggiungi la linea del percorso
        if len(route_points) > 1:
            folium.PolyLine(
                route_points,
                color='blue',
                weight=3,
                opacity=0.8
            ).add_to(m)
        
        # Evidenzia l'ultima posizione in modo speciale
        last_pos = positions_df.iloc[-1]
        folium.CircleMarker(
            [last_pos['lat'], last_pos['lon']], 
            radius=10,
            color='green',
            fill=True,
            fill_color='green',
            fill_opacity=0.9,
            popup="Ultima posizione"
        ).add_to(m)
        
        # Salva la versione interattiva
        m.save(interactive_file)
        print(f"Mappa interattiva del percorso salvata come {interactive_file}")
        
        # Salva la mappa come HTML temporaneo per il rendering dell'immagine
        map_html = f"temp_route_map_{int(time.time())}.html"
        m.save(map_html)
        
        # Configura Chrome in modalità headless per il rendering
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--window-size=800,600")
        
        # Crea un driver browser
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(f"file://{os.path.abspath(map_html)}")
        
        # Aspetta che la mappa sia caricata completamente
        time.sleep(2)
        
        # Cattura lo screenshot
        driver.save_screenshot(output_file)
        
        # Chiudi il browser e rimuovi il file temporaneo
        driver.quit()
        os.remove(map_html)
        
        print(f"Mappa del percorso salvata come {output_file}")
        return True
    except Exception as e:
        print(f"Errore nella generazione della mappa del percorso: {e}")
        return False

def generate_route_maps_if_needed():
    """Genera preventivamente le mappe del percorso se ci sono abbastanza posizioni"""
    global last_route_map_generation
    
    # Verifica se è passato abbastanza tempo dall'ultima generazione (almeno 10 minuti)
    current_time = time.time()
    if current_time - last_route_map_generation < 600:  # 600 secondi = 10 minuti
        print("Mappe di percorso generate di recente, skippo la rigenerazione")
        return
        
    # Conta quante posizioni sono disponibili
    num_positions = count_available_positions()
    
    # Genera la mappa per 5 posizioni se disponibili
    if num_positions >= 5:
        print("Generazione preventiva della mappa per le ultime 5 posizioni")
        generate_route_map(5, route_map_file_5)
        
    # Genera la mappa per 20 posizioni se disponibili
    if num_positions >= 20:
        print("Generazione preventiva della mappa per le ultime 20 posizioni")
        generate_route_map(20, route_map_file_20)
        
    # Aggiorna il timestamp dell'ultima generazione
    last_route_map_generation = current_time

def send_route_map(num_positions):
    """Invia una mappa con l'itinerario delle ultime posizioni"""
    try:
        # Determina quale file usare
        route_file = route_map_file_5 if num_positions == 5 else route_map_file_20
        interactive_file = f"route_map_{num_positions}.html"
        
        # Se il file non esiste o è vecchio, generalo
        if not os.path.exists(route_file) or (time.time() - os.path.getmtime(route_file)) > 3600:  # 1 ora
            if not generate_route_map(num_positions, route_file, interactive_file):
                print(f"Impossibile generare la mappa del percorso per le ultime {num_positions} posizioni")
                return False
                
        # Formatta il messaggio
        message = f"```\nROUTE MAP - LAST {num_positions} POSITIONS\n\n"
        message += f"Generated: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n```"
        
        # Aggiungi pulsante per la mappa HTML
        buttons = []
        buttons_info = {}
        global html_map_callbacks, message_buttons
        
        if os.path.exists(interactive_file):
            html_callback = str(uuid.uuid4())
            html_map_callbacks[html_callback] = {
                "file": interactive_file,
                "type": "route",
                "num_positions": num_positions
            }
            buttons.append({
                "text": "🌐 Mappa HTML",
                "callback_data": html_callback
            })
            buttons_info[html_callback] = {
                "text": "🌐 Mappa HTML"
            }
            
        # Crea inline keyboard se ci sono pulsanti
        inline_keyboard = None
        if buttons:
            inline_keyboard = json.dumps({
                "inline_keyboard": [buttons]
            })
        
        # Invia foto con didascalia
        message_id = None
        with open(route_file, 'rb') as photo:
            files = {'photo': photo}
            data = {
                'chat_id': CHAT_ID,
                'caption': message,
                'parse_mode': 'Markdown'
            }
            
            # Aggiungi pulsanti solo se disponibili
            if inline_keyboard:
                data['reply_markup'] = inline_keyboard
                
            response = requests.post(
                f"{TELEGRAM_API_URL}/sendPhoto",
                data=data,
                files=files
            )
            
            # Estrai il message_id dalla risposta
            if response.status_code == 200:
                response_data = response.json()
                if response_data.get("ok", False) and "result" in response_data:
                    message_id = response_data["result"].get("message_id")
                
        if response.status_code == 200:
            print(f"Mappa del percorso per le ultime {num_positions} posizioni inviata con successo")
            
            # Se abbiamo il message_id e ci sono pulsanti, salva le informazioni
            if message_id is not None and buttons_info:
                message_buttons[message_id] = buttons_info
                print(f"Salvati pulsanti per il messaggio {message_id}: {buttons_info}")
                
            return True
        else:
            print(f"Errore nell'invio della mappa del percorso: {response.text}")
            return False
    except Exception as e:
        print(f"Errore nell'invio della mappa del percorso: {e}")
        return False

def process_callback_query(callback_data, callback_id, message_id=None, chat_id=None):
    """Processa i callback query dai pulsanti inline"""
    global processing_callbacks, message_buttons, html_map_callbacks
    
    try:
        # Controlla se è una richiesta di mappa HTML
        if callback_data in html_map_callbacks:
            # Ottieni informazioni sulla mappa
            map_info = html_map_callbacks[callback_data]
            html_file = map_info["file"]
            map_type = map_info["type"]
            
            # Messaggio di elaborazione
            requests.post(
                f"{TELEGRAM_API_URL}/answerCallbackQuery",
                json={
                    "callback_query_id": callback_id,
                    "text": f"Invio mappa HTML interattiva...",
                    "show_alert": True
                }
            )
            
            # Aggiorna il messaggio per rimuovere il pulsante
            if message_id is not None and chat_id is not None:
                # Trova tutti i pulsanti di questo messaggio
                if message_id in message_buttons:
                    buttons_info = message_buttons[message_id].copy()
                    
                    # Rimuovi il callback_data che stiamo processando
                    if callback_data in buttons_info:
                        del buttons_info[callback_data]
                    
                    # Crea nuovi pulsanti con quelli rimanenti
                    buttons = []
                    for cb_data, btn_info in buttons_info.items():
                        if isinstance(btn_info, dict) and "text" in btn_info:
                            buttons.append({
                                "text": btn_info["text"],
                                "callback_data": cb_data
                            })
                        elif isinstance(btn_info, int):  # Route map button
                            buttons.append({
                                "text": f"🗺️ Ultime {btn_info}",
                                "callback_data": cb_data
                            })
                    
                    # Se ci sono ancora pulsanti, aggiorna il messaggio
                    if buttons:
                        inline_keyboard = json.dumps({
                            "inline_keyboard": [buttons]
                        })
                        
                        try:
                            # Aggiorna il messaggio per rimuovere il pulsante
                            print(f"Aggiornamento messaggio {message_id} per rimuovere pulsante {callback_data}")
                            requests.post(
                                f"{TELEGRAM_API_URL}/editMessageReplyMarkup",
                                json={
                                    "chat_id": chat_id,
                                    "message_id": message_id,
                                    "reply_markup": inline_keyboard
                                }
                            )
                        except Exception as e:
                            print(f"Errore nell'aggiornamento del messaggio: {e}")
                    else:
                        # Non ci sono più pulsanti, rimuovi completamente la keyboard
                        try:
                            print(f"Rimozione di tutti i pulsanti dal messaggio {message_id}")
                            requests.post(
                                f"{TELEGRAM_API_URL}/editMessageReplyMarkup",
                                json={
                                    "chat_id": chat_id,
                                    "message_id": message_id,
                                    "reply_markup": json.dumps({"inline_keyboard": []})
                                }
                            )
                        except Exception as e:
                            print(f"Errore nella rimozione dei pulsanti: {e}")
                            
                    # Aggiorna il dizionario dei pulsanti di questo messaggio
                    message_buttons[message_id] = buttons_info
                    
                    # Se non ci sono più pulsanti, rimuovi l'intero messaggio dal dizionario
                    if not buttons_info:
                        del message_buttons[message_id]
            
            # Invia la mappa interattiva come documento
            if os.path.exists(html_file):
                caption = ""
                if map_type == "position":
                    caption = "🌐 Mappa interattiva della posizione attuale"
                else:  # route
                    num_positions = map_info.get("num_positions", 0)
                    caption = f"🌐 Mappa interattiva del percorso (ultime {num_positions} posizioni)"
                
                with open(html_file, 'rb') as f:
                    files = {'document': f}
                    response = requests.post(
                        f"{TELEGRAM_API_URL}/sendDocument",
                        data={
                            'chat_id': CHAT_ID,
                            'caption': caption,
                            'parse_mode': 'Markdown'
                        },
                        files=files
                    )
                    
                if response.status_code != 200:
                    print(f"Errore nell'invio della mappa HTML: {response.text}")
                    return False
                
                print(f"Mappa HTML interattiva inviata con successo: {html_file}")
                # Pulizia del callback utilizzato
                del html_map_callbacks[callback_data]
                return True
                
            else:
                print(f"File mappa HTML non trovato: {html_file}")
                return False
        
        # Procedi con i callback per le mappe di percorso
        if callback_data not in route_callbacks:
            print(f"Callback data non valido: {callback_data}")
            return False
            
        # Controlla se questo callback è già in elaborazione
        if callback_data in processing_callbacks:
            print(f"Callback già in elaborazione: {callback_data}")
            return False
            
        # Aggiungi questo callback all'insieme dei callback in elaborazione
        processing_callbacks.add(callback_data)
            
        # Invia un messaggio "in elaborazione"
        num_positions = route_callbacks[callback_data]
        requests.post(
            f"{TELEGRAM_API_URL}/answerCallbackQuery",
            json={
                "callback_query_id": callback_id,
                "text": f"Invio mappa con ultime {num_positions} posizioni...",
                "show_alert": True
            }
        )
        
        print(f"Richiesta mappa del percorso per le ultime {num_positions} posizioni")
        
        # Se abbiamo message_id e chat_id, possiamo aggiornare il messaggio
        if message_id is not None and chat_id is not None:
            # Trova tutti i pulsanti di questo messaggio
            if message_id in message_buttons:
                buttons_info = message_buttons[message_id].copy()
                
                # Rimuovi il callback_data che stiamo processando
                if callback_data in buttons_info:
                    del buttons_info[callback_data]
                
                # Crea nuovi pulsanti con quelli rimanenti
                buttons = []
                for cb_data, btn_info in buttons_info.items():
                    if isinstance(btn_info, dict) and "text" in btn_info:
                        buttons.append({
                            "text": btn_info["text"],
                            "callback_data": cb_data
                        })
                    elif isinstance(btn_info, int):  # Route map button
                        buttons.append({
                            "text": f"🗺️ Ultime {btn_info}",
                            "callback_data": cb_data
                        })
                
                # Se ci sono ancora pulsanti, aggiorna il messaggio
                if buttons:
                    inline_keyboard = json.dumps({
                        "inline_keyboard": [buttons]
                    })
                    
                    try:
                        # Aggiorna il messaggio per rimuovere il pulsante
                        print(f"Aggiornamento messaggio {message_id} per rimuovere pulsante {callback_data}")
                        requests.post(
                            f"{TELEGRAM_API_URL}/editMessageReplyMarkup",
                            json={
                                "chat_id": chat_id,
                                "message_id": message_id,
                                "reply_markup": inline_keyboard
                            }
                        )
                    except Exception as e:
                        print(f"Errore nell'aggiornamento del messaggio: {e}")
                else:
                    # Non ci sono più pulsanti, rimuovi completamente la keyboard
                    try:
                        print(f"Rimozione di tutti i pulsanti dal messaggio {message_id}")
                        requests.post(
                            f"{TELEGRAM_API_URL}/editMessageReplyMarkup",
                            json={
                                "chat_id": chat_id,
                                "message_id": message_id,
                                "reply_markup": json.dumps({"inline_keyboard": []})
                            }
                        )
                    except Exception as e:
                        print(f"Errore nella rimozione dei pulsanti: {e}")
                        
                # Aggiorna il dizionario dei pulsanti di questo messaggio
                message_buttons[message_id] = buttons_info
                
                # Se non ci sono più pulsanti, rimuovi l'intero messaggio dal dizionario
                if not buttons_info:
                    del message_buttons[message_id]
        
        # Invia la mappa del percorso (già generata preventivamente)
        result = send_route_map(num_positions)
        
        # Rimuovi il callback dall'insieme dei callback in elaborazione
        processing_callbacks.remove(callback_data)
        
        return result
    except Exception as e:
        # In caso di errore, assicurati di rimuovere il callback dall'insieme
        if callback_data in processing_callbacks:
            processing_callbacks.remove(callback_data)
        print(f"Errore nel processare il callback query: {e}")
        return False

def check_and_process_updates():
    """Controlla e processa gli aggiornamenti da Telegram (callback query)"""
    try:
        # Ottieni gli aggiornamenti
        response = requests.get(
            f"{TELEGRAM_API_URL}/getUpdates",
            params={
                "offset": -1,  # Prendi solo l'ultimo aggiornamento
                "timeout": 1
            }
        )
        
        if response.status_code != 200:
            print(f"Errore nel controllo degli aggiornamenti Telegram: {response.text}")
            return
            
        data = response.json()
        
        # Controlla se ci sono callback query
        if not data.get("ok", False) or "result" not in data or not data["result"]:
            return
            
        for update in data["result"]:
            if "callback_query" in update:
                callback_query = update["callback_query"]
                callback_id = callback_query["id"]
                callback_data = callback_query.get("data", "")
                
                # Estrai informazioni sul messaggio
                message = callback_query.get("message", {})
                message_id = message.get("message_id")
                chat_id = message.get("chat", {}).get("id")
                
                print(f"Ricevuto callback query: {callback_data} dal messaggio {message_id}")
                
                # Processa il callback con informazioni sul messaggio
                process_result = process_callback_query(callback_data, callback_id, message_id, chat_id)
                
                # Se il callback è stato elaborato con successo, 
                # conferma e segna l'aggiornamento come letto
                if process_result:
                    # Ottieni l'update_id
                    update_id = update.get("update_id", 0)
                    if update_id > 0:
                        # Segna come letto usando l'offset
                        requests.get(
                            f"{TELEGRAM_API_URL}/getUpdates",
                            params={"offset": update_id + 1}
                        )
                        print(f"Aggiornamento {update_id} segnato come letto")
    except Exception as e:
        print(f"Errore nel controllo degli aggiornamenti Telegram: {e}")

def generate_map_image(lat, lon, address):
    """Genera un'immagine della mappa con la posizione del veicolo e le posizioni precedenti"""
    try:
        print("Generazione mappa della posizione...")
        
        # Carica le posizioni precedenti (massimo 20)
        positions = []
        try:
            if os.path.exists(csv_file):
                positions_df = pd.read_csv(csv_file)
                # Ordina per timestamp in ordine decrescente per avere le più recenti prima
                positions_df = positions_df.sort_values(by='timestamp', ascending=False)
                # Prendi solo le ultime 20 posizioni
                positions_df = positions_df.head(20)
                # Riordina per timestamp in ordine crescente per il percorso
                positions_df = positions_df.sort_values(by='timestamp', ascending=True)
                
                for _, position in positions_df.iterrows():
                    positions.append({
                        'lat': position['lat'],
                        'lon': position['lon'],
                        'timestamp': position['timestamp'],
                        'via': position['via'] if pd.notna(position['via']) else '',
                        'comune': position['comune'] if pd.notna(position['comune']) else ''
                    })
        except Exception as e:
            print(f"Errore nel caricamento delle posizioni precedenti: {e}")
        
        # Se non ci sono posizioni precedenti o è la prima posizione, usa solo la posizione attuale
        if not positions or len(positions) < 2:
            # Crea una mappa centrata sulla posizione attuale con zoom 18 (molto ravvicinato)
            m = folium.Map(location=[lat, lon], zoom_start=18, tiles='CartoDB positron')
            
            # Aggiungi un marker per la posizione
            folium.CircleMarker(
                [lat, lon], 
                radius=10,
                color='red',
                fill=True,
                fill_color='red',
                fill_opacity=0.7,
                popup=address
            ).add_to(m)
            
            # Aggiungi un cerchio più ampio per indicare l'area
            folium.Circle(
                [lat, lon],
                radius=25,  # 25 metri di raggio per uno zoom più stretto
                color='blue',
                fill=True,
                fill_opacity=0.1
            ).add_to(m)
        else:
            # Usa l'ultima posizione come centro della mappa con zoom forte (17-18)
            # Questo assicura che la mappa sia centrata sull'ultima posizione con uno zoom forte
            m = folium.Map(location=[lat, lon], zoom_start=17, tiles='CartoDB positron')
            
            # Crea una lista di punti per il percorso
            route_points = []
            
            # Aggiungi marker per le posizioni precedenti
            for i, position in enumerate(positions):
                pos_lat = position['lat']
                pos_lon = position['lon']
                timestamp = position['timestamp']
                via = position['via']
                comune = position['comune']
                
                # Formatta il timestamp per ottenere solo l'orario (HH:MM:SS)
                try:
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    formatted_time = dt.strftime('%H:%M:%S')
                    formatted_full_time = dt.strftime('%d/%m/%Y %H:%M:%S')
                except:
                    formatted_time = "??:??:??"
                    formatted_full_time = timestamp
                
                # Popup con informazioni
                popup_text = f"Ora: {formatted_full_time}<br>Via: {via}, {comune}"
                
                # Aggiungi il punto alla lista dei punti del percorso
                route_points.append([pos_lat, pos_lon])
                
                # Aggiungi un marker per ogni posizione precedente (non l'ultima)
                if i < len(positions) - 1:
                    folium.CircleMarker(
                        [pos_lat, pos_lon], 
                        radius=6,
                        color='blue',
                        fill=True,
                        fill_color='blue',
                        fill_opacity=0.7,
                        popup=popup_text
                    ).add_to(m)
                    
                    # Aggiungi label con l'orario solo per alcune posizioni per non sovraffollare la mappa
                    # Per poche posizioni: mostra tutti gli orari
                    # Per molte posizioni: mostra ogni 5 posizioni e la prima/ultima
                    show_label = (len(positions) <= 5) or (i % 5 == 0) or (i == 0)
                    
                    if show_label:
                        folium.map.Marker(
                            [pos_lat, pos_lon],
                            icon=folium.DivIcon(
                                icon_size=(60, 20),
                                icon_anchor=(30, -10),
                                html=f'<div style="font-size: 10pt; color: black; background-color: white; border: 1px solid black; border-radius: 3px; padding: 1px 3px; text-align: center;">{formatted_time}</div>'
                            )
                        ).add_to(m)
            
            # Aggiungi la linea del percorso
            if len(route_points) > 1:
                folium.PolyLine(
                    route_points,
                    color='blue',
                    weight=3,
                    opacity=0.8
                ).add_to(m)
            
            # Aggiungi un marker più grande per l'ultima posizione (posizione attuale)
            folium.CircleMarker(
                [lat, lon], 
                radius=10,
                color='red',
                fill=True,
                fill_color='red',
                fill_opacity=0.7,
                popup=address
            ).add_to(m)
        
        # Salva la mappa come HTML temporaneo
        map_html = f"temp_map_{int(time.time())}.html"
        m.save(map_html)
        
        # Salva anche la versione interattiva permanente
        m.save(interactive_map_file)
        print(f"Mappa interattiva salvata come {interactive_map_file}")
        
        # Configura Chrome in modalità headless per il rendering
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--window-size=800,600")
        
        # Crea un driver browser
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(f"file://{os.path.abspath(map_html)}")
        
        # Aspetta che la mappa sia caricata completamente
        time.sleep(2)
        
        # Cattura lo screenshot
        driver.save_screenshot(map_file)
        
        # Chiudi il browser e rimuovi il file temporaneo
        driver.quit()
        os.remove(map_html)
        
        print(f"Mappa salvata come {map_file}")
        return True
    except Exception as e:
        print(f"Errore nella generazione della mappa: {e}")
        return False

def send_position_update(lat, lon, address, timestamp, speed, battery):
    """Invia un messaggio con la posizione attuale e la mappa"""
    try:
        # Genera immagine della mappa
        if not generate_map_image(lat, lon, address):
            print("Impossibile generare la mappa, invio solo il messaggio con la posizione")
        
        # Conta quante posizioni sono disponibili e usate nella mappa
        num_positions = min(count_available_positions(), 20)
        
        # Riformatta il timestamp in un formato più leggibile
        try:
            # Converti da formato ISO a datetime
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            # Formatta in un modo più leggibile
            formatted_timestamp = dt.strftime('%d/%m/%Y %H:%M:%S')
        except:
            # Fallback se il parsing fallisce
            formatted_timestamp = timestamp
        
        # Formatta il messaggio in stile cyber/tecnico con font monospazio
        message = f"```\nPOSITION UPDATE\n\n"
        message += f"📍 {address}\n\n"
        message += f"TIMESTAMP: {formatted_timestamp}\n"
        message += f"BATTERY: {battery}\n"
        message += f"SPEED: {speed} km/h\n"
        
        if num_positions > 1:
            message += f"SHOWING: Last {num_positions} positions\n"
            
        message += f"[{lat}, {lon}]\n```"
        
        # Variabili per callback_data e pulsanti
        global route_callbacks, message_buttons, html_map_callbacks
        inline_keyboard = None
        buttons = []
        buttons_info = {}  # { callback_data: info }
        
        # Aggiungi pulsante per la mappa HTML
        if os.path.exists(interactive_map_file):
            html_callback = str(uuid.uuid4())
            html_map_callbacks[html_callback] = {
                "file": interactive_map_file,
                "type": "position",
                "num_positions": num_positions
            }
            buttons.append({
                "text": "🌐 Mappa HTML",
                "callback_data": html_callback
            })
            buttons_info[html_callback] = {
                "text": "🌐 Mappa HTML"
            }
        
        # Non è necessario aggiungere pulsanti per visualizzare la mappa con più posizioni
        # poiché ora la mappa mostra già il massimo delle posizioni disponibili
        # Manteniamo solo il pulsante per le ultime 20 posizioni se abbiamo più di 20 posizioni
        if count_available_positions() > 20:
            callback_20 = str(uuid.uuid4())
            route_callbacks[callback_20] = 20
            buttons_info[callback_20] = 20
            buttons.append({
                "text": "🗺️ Ultime 20",
                "callback_data": callback_20
            })
            
        # Crea i pulsanti inline solo se c'è almeno un pulsante
        if buttons:
            # Una sola riga con tutti i pulsanti
            inline_keyboard = json.dumps({
                "inline_keyboard": [buttons]
            })
        
        # Invia foto con didascalia e pulsanti e salva il message_id
        message_id = None
        if os.path.exists(map_file):
            with open(map_file, 'rb') as photo:
                files = {'photo': photo}
                data = {
                    'chat_id': CHAT_ID,
                    'caption': message,
                    'parse_mode': 'Markdown',
                }
                
                # Aggiungi pulsanti solo se disponibili
                if inline_keyboard:
                    data['reply_markup'] = inline_keyboard
                
                response = requests.post(
                    f"{TELEGRAM_API_URL}/sendPhoto",
                    data=data,
                    files=files
                )
                
                # Estrai il message_id dalla risposta
                if response.status_code == 200:
                    response_data = response.json()
                    if response_data.get("ok", False) and "result" in response_data:
                        message_id = response_data["result"].get("message_id")
        else:
            # Fallback: invia solo messaggio testuale con pulsanti
            data = {
                'chat_id': CHAT_ID,
                'text': message,
                'parse_mode': 'Markdown',
            }
            
            # Aggiungi pulsanti solo se disponibili
            if inline_keyboard:
                data['reply_markup'] = inline_keyboard
                
            response = requests.post(
                f"{TELEGRAM_API_URL}/sendMessage",
                json=data
            )
            
            # Estrai il message_id dalla risposta
            if response.status_code == 200:
                response_data = response.json()
                if response_data.get("ok", False) and "result" in response_data:
                    message_id = response_data["result"].get("message_id")
            
        if response.status_code == 200:
            print("Aggiornamento posizione inviato con successo")
            
            # Se abbiamo il message_id e ci sono pulsanti, salva le informazioni
            if message_id is not None and buttons_info:
                message_buttons[message_id] = buttons_info
                print(f"Salvati pulsanti per il messaggio {message_id}: {buttons_info}")
                
            return True
        else:
            print(f"Errore nell'invio dell'aggiornamento: {response.text}")
            return False
    except Exception as e:
        print(f"Errore nell'invio dell'aggiornamento della posizione: {e}")
        return False

def send_telegram_files():
    try:
        global last_sent_hash, updates_since_last_send
        
        # Verifica se ci sono stati aggiornamenti dal precedente invio
        if not updates_since_last_send:
            print("Nessun aggiornamento dai file precedentemente inviati, salto l'invio")
            return False
            
        # Calcola hash dei file per verificare se sono cambiati
        current_hash = ""
        for file_path in [csv_file, txt_file]:
            if os.path.exists(file_path):
                with open(file_path, 'rb') as f:
                    current_hash += hashlib.md5(f.read()).hexdigest()
        
        # Se l'hash è identico all'ultimo invio, non invia nulla
        if current_hash == last_sent_hash:
            print("I file non sono cambiati dall'ultimo invio, salto l'invio")
            return False
            
        # Aggiorna l'hash
        last_sent_hash = current_hash
            
        # Prepara i file per l'invio
        media = []
        timestamp = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        
        # Aggiungi il file CSV (primo file con descrizione)
        with open(csv_file, 'rb') as csv:
            media.append({
                'type': 'document',
                'media': f'attach://{csv_file}',
                'caption': f"```\nDATA UPDATE - {timestamp}\n```"  # Descrizione in monospazio
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
                        'media': json.dumps(media),
                        'parse_mode': 'Markdown'
                    },
                    files=files
                )
                
                if response.status_code == 200:
                    print("File inviati con successo su Telegram in un unico messaggio")
                    updates_since_last_send = False  # Resetta il flag degli aggiornamenti
                    return True
                else:
                    print(f"Errore nell'invio dei file: {response.text}")
                    return False
                    
    except Exception as e:
        print(f"Errore nell'invio dei file su Telegram: {e}")
        return False

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

def get_formatted_address(via, comune, provincia):
    """Crea un indirizzo formattato dalle singole parti"""
    parts = []
    if via:
        parts.append(via)
    if comune:
        parts.append(comune)
    if provincia:
        parts.append(f"({provincia})")
    
    if parts:
        return ", ".join(parts)
    return "Indirizzo sconosciuto"

def save_to_txt(df):
    try:
        # Ordina il DataFrame per timestamp in ordine decrescente (più recenti prima)
        df_sorted = df.sort_values(by='timestamp', ascending=False)
        
        with open(txt_file, 'w', encoding='utf-8') as f:
            f.write("REGISTRO POSIZIONI VEICOLO\n")
            f.write("=" * 50 + "\n\n")
            
            for _, row in df_sorted.iterrows():
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
        return True
    except Exception as e:
        print(f"Errore nel salvataggio del file .txt: {e}")
        return False

def fetch_and_save():
    global primo_avvio, check_counter, updates_since_last_send
    
    try:
        # Verifica connessione Telegram solo al primo avvio
        if primo_avvio:
            print("Primo avvio - Verifico connessione Telegram...")
            test_telegram_connection()
            primo_avvio = False
            
        # Incrementa il contatore di check
        check_counter += 1
        print(f"\n[{datetime.now()}] Esecuzione check #{check_counter}")
            
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
        data = response.json()
        
        # Otteniamo l'ultima posizione invece dell'array di posizioni
        last_position = data["data"]["lastPosition"]
        print("Ottenuta l'ultima posizione del veicolo:")
        print(f"Coordinate: {last_position['lat']}, {last_position['lng']}")
        print(f"Timestamp: {last_position['timestamp']}")
        print(f"Indirizzo: {last_position.get('formatted_address', 'Non disponibile')}")
        
        # Carica i dati esistenti se il file CSV esiste
        try:
            existing_df = pd.read_csv(csv_file)
            print(f"Trovate {len(existing_df)} posizioni esistenti")
        except FileNotFoundError:
            existing_df = pd.DataFrame()
            print("Nessun file CSV esistente trovato")

        # Verifica se questa posizione con lo stesso timestamp esiste già
        is_existing = False
        if not existing_df.empty:
            existing_pos = existing_df[
                (existing_df['lat'] == last_position['lat']) & 
                (existing_df['lon'] == last_position['lng']) &
                (existing_df['timestamp'] == last_position['timestamp'])
            ]
            if not existing_pos.empty:
                print("Posizione con le stesse coordinate e timestamp già registrata, nessun aggiornamento necessario")
                
                # Inviamo i file ogni 12 check (circa 60 minuti) se ci sono stati aggiornamenti
                if check_counter % 12 == 0:
                    print("È arrivato il momento di inviare i file aggiornati (ogni 60 minuti)")
                    send_telegram_files()
                    
                # Controlla se ci sono callback queries da processare
                check_and_process_updates()
                
                return
                
        # Estrazione dei dati interessanti
        # Se l'indirizzo è già presente nel formatted_address, lo usiamo
        formatted_address = last_position.get('formatted_address', '')
        via = last_position.get('street', '')
        comune = last_position.get('city', '')
        provincia = last_position.get('prov', '')
        
        # Se non ci sono abbastanza informazioni, usiamo il geocoding
        if not (via and comune and provincia):
            print("Informazioni sull'indirizzo incomplete, richiedo geocoding")
            address_info = get_address(last_position["lat"], last_position["lng"])
            via = via or address_info['via']
            comune = comune or address_info['comune']
            provincia = provincia or address_info['provincia']
        
        # Creiamo una nuova posizione da salvare
        # Estraiamo informazioni aggiuntive dal veicolo
        vehicle_info = data["data"]
        battery = vehicle_info.get("batteryLevel", "N/A")
        mileage = vehicle_info.get("mileage", 0)
        
        # Creiamo la riga da aggiungere
        new_position = {
            "timestamp": last_position["timestamp"],
            "lat": last_position["lat"],
            "lon": last_position["lng"],
            "speed": last_position["speed"],
            "mileage": mileage,
            "description": last_position.get("type", "N/A"),
            "battery": battery,
            "fix": last_position.get("type", "N/A"),
            "hdop": last_position.get("accuracy", 0),
            "via": via,
            "comune": comune,
            "provincia": provincia
        }
        
        # Creiamo un DataFrame con la nuova posizione
        new_df = pd.DataFrame([new_position])
        
        # Unisci i dati esistenti con i nuovi
        if not existing_df.empty:
            merged_df = pd.concat([existing_df, new_df])
            # Rimuovi duplicati basati su lat, lon E timestamp
            merged_df.drop_duplicates(subset=['lat', 'lon', 'timestamp'], keep='last', inplace=True)
        else:
            merged_df = new_df

        # Ordina il DataFrame per timestamp in ordine decrescente (più recenti prima)
        merged_df = merged_df.sort_values(by='timestamp', ascending=False)
            
        # Salva il CSV con l'encoding UTF-8
        merged_df.to_csv(csv_file, index=False, encoding="utf-8")
        print(f"\n[{datetime.now()}] Dati aggiornati, nuova posizione salvata.")
        
        # Salva anche in formato .txt
        if save_to_txt(merged_df):
            # Segnala che ci sono stati aggiornamenti
            updates_since_last_send = True
            
        # Genera preventivamente le mappe di percorso se ci sono abbastanza posizioni
        generate_route_maps_if_needed()
        
        # Crea l'indirizzo formattato per il messaggio
        address_for_message = get_formatted_address(via, comune, provincia)
        
        # Invia un messaggio con la posizione attuale e la mappa
        send_position_update(
            last_position["lat"], 
            last_position["lng"], 
            address_for_message,
            last_position["timestamp"], 
            last_position["speed"],
            battery
        )
        
        # Inviamo i file ogni 12 check (circa 60 minuti) se ci sono stati aggiornamenti
        if check_counter % 12 == 0:
            print("È arrivato il momento di inviare i file aggiornati (ogni 60 minuti)")
            send_telegram_files()
            
        # Controlla se ci sono callback queries da processare
        check_and_process_updates()
        
    except requests.exceptions.RequestException as e:
        print(f"Errore nella richiesta: {e}")
    except Exception as e:
        print(f"Errore imprevisto: {e}")
    finally:
        if 'session' in locals():
            session.close()

# Pianifica ogni 5 minuti
schedule.every(5).minutes.do(fetch_and_save)

# Pianifica il controllo degli aggiornamenti Telegram ogni 30 secondi
schedule.every(30).seconds.do(check_and_process_updates)

# Avvio ciclo
print("Inizio monitoraggio veicolo ogni 5 minuti...")
fetch_and_save()  # Primo fetch subito

while True:
    schedule.run_pending()
    time.sleep(1)