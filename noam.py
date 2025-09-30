import os
import io
import re
import math
import json
import base64
import requests
import streamlit as st
from typing import List, Tuple, Optional
from PIL import Image, ImageOps, ImageFile

# Pour éviter des erreurs sur images tronquées
ImageFile.LOAD_TRUNCATED_IMAGES = True

# =========================
# Config via variables d'environnement
# =========================
API_URL = os.getenv("API_URL")                     # ex: https://api.fidealis.com/xxx
API_KEY = os.getenv("API_KEY")
ACCOUNT_KEY = os.getenv("ACCOUNT_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")       # pour géocodage d'adresse (optionnel)
# Service account : au CHOIX -> 1) JSON en clair via env, 2) chemin vers fichier .json
GDRIVE_SA_JSON = os.getenv("GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON")  # contenu JSON (string)
GDRIVE_SA_FILE = os.getenv("GOOGLE_DRIVE_SERVICE_ACCOUNT_FILE")  # chemin d'un fichier .json

MAX_DIM = int(os.getenv("MAX_DIM", "1600"))        # redimensionnement max (px)
JPEG_QUALITY = int(os.getenv("JPEG_QUALITY", "80"))

# =========================
# Google APIs (Drive)
# =========================
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

def get_drive_service():
    scopes = ['https://www.googleapis.com/auth/drive.readonly']
    if GDRIVE_SA_JSON:
        info = json.loads(GDRIVE_SA_JSON)
        creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    elif GDRIVE_SA_FILE:
        creds = service_account.Credentials.from_service_account_file(GDRIVE_SA_FILE, scopes=scopes)
    else:
        raise RuntimeError("Aucune crédential Google Drive fournie. Définis GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON ou GOOGLE_DRIVE_SERVICE_ACCOUNT_FILE.")
    # cache_discovery=False évite un warning
    return build('drive', 'v3', credentials=creds, cache_discovery=False)

def extract_folder_id(maybe_url: str) -> str:
    m = re.search(r'/folders/([a-zA-Z0-9_-]+)', maybe_url)
    if m:
        return m.group(1)
    if re.fullmatch(r'[a-zA-Z0-9_-]{20,}', maybe_url):
        return maybe_url
    raise ValueError("Impossible de détecter l'ID du dossier : fournis une URL Drive ou un ID.")

def list_subfolders(drive, parent_id: str) -> List[dict]:
    """Liste les sous-dossiers directs du parent."""
    q = f"'{parent_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder'"
    subfolders = []
    page_token = None
    while True:
        resp = drive.files().list(
            q=q,
            spaces='drive',
            fields="nextPageToken, files(id,name,parents)",
            pageToken=page_token
        ).execute()
        subfolders.extend(resp.get('files', []))
        page_token = resp.get('nextPageToken')
        if not page_token:
            break
    return subfolders

def list_images_in_folder(drive, folder_id: str) -> List[dict]:
    """Liste les fichiers image (mimeType image/*) d'un dossier (pas récursif)."""
    q = f"'{folder_id}' in parents and trashed=false and mimeType contains 'image/'"
    images = []
    page_token = None
    while True:
        resp = drive.files().list(
            q=q,
            spaces='drive',
            fields="nextPageToken, files(id,name,mimeType,size,createdTime)",
            pageToken=page_token
        ).execute()
        images.extend(resp.get('files', []))
        page_token = resp.get('nextPageToken')
        if not page_token:
            break
    # Petit tri par nom pour stabilité
    images.sort(key=lambda f: f.get('name',''))
    return images

def download_image_bytes(drive, file_id: str) -> bytes:
    """Télécharge un fichier Drive en mémoire (BytesIO) par streaming."""
    request = drive.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        # on pourrait afficher une barre de progression par fichier si besoin
    return fh.getvalue()

# =========================
# Google Maps Geocoding (optionnel)
# =========================
def get_coordinates(address: str) -> Tuple[Optional[str], Optional[str]]:
    if not address or not GOOGLE_API_KEY:
        return None, None
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    try:
        r = requests.get(url, params={"address": address, "key": GOOGLE_API_KEY}, timeout=30)
        if r.status_code == 200:
            data = r.json()
            if data.get("status") == "OK" and data.get("results"):
                loc = data["results"][0]["geometry"]["location"]
                return str(loc["lat"]), str(loc["lng"])
    except Exception:
        pass
    return None, None

# =========================
# Fidealis API
# =========================
def api_login() -> Optional[str]:
    try:
        r = requests.get(
            API_URL,
            params={"key": API_KEY, "call": "loginUserFromAccountKey", "accountKey": ACCOUNT_KEY},
            timeout=30,
        )
        data = r.json()
        return data.get("PHPSESSID")
    except Exception:
        return None

def get_credit(session_id: str):
    try:
        r = requests.get(
            API_URL,
            params={"key": API_KEY, "PHPSESSID": session_id, "call": "getCredits", "product_ID": ""},
            timeout=30,
        )
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None

def get_quantity_for_product_4(credit_data):
    try:
        return credit_data["4"]["quantity"]
    except Exception:
        return "N/A"

def encode_base64_bytes(b: bytes) -> str:
    return base64.b64encode(b).decode("utf-8")

# =========================
# Préprocessing & Collage
# =========================
def load_preprocess_jpeg(img_bytes: bytes, max_dim: int = MAX_DIM, quality: int = JPEG_QUALITY) -> bytes:
    """
    Ouvre l'image, corrige orientation EXIF, convertit -> RGB, resize pour que max(w,h)<=max_dim,
    renvoie des bytes JPEG compressés sans EXIF.
    """
    with Image.open(io.BytesIO(img_bytes)) as img:
        img = ImageOps.exif_transpose(img)
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        w, h = img.size
        scale = min(1.0, max_dim / max(w, h))
        if scale < 1.0:
            img = img.resize((int(w*scale), int(h*scale)), Image.LANCZOS)
        out = io.BytesIO()
        img.save(out, format="JPEG", quality=quality, optimize=True)
        return out.getvalue()

def create_collage_from_three(jpegs: List[bytes]) -> bytes:
    """
    Crée un collage horizontal avec 1 à 3 images JPEG (déjà préprocessées).
    Hauteur = min des hauteurs, marge interne légère.
    """
    images = [Image.open(io.BytesIO(b)) for b in jpegs]
    # Harmoniser la hauteur (min)
    min_h = min(im.height for im in images)
    resized = []
    for im in images:
        scale = min_h / im.height
        new_w = int(im.width * scale)
        resized.append(im.resize((new_w, min_h), Image.LANCZOS))

    # Canvas blanc avec padding 25px et 20px entre images
    total_w = sum(im.width for im in resized) + (len(resized)-1)*20 + 50
    canvas = Image.new("RGB", (total_w, min_h + 50), (255, 255, 255))
    x = 25
    for im in resized:
        canvas.paste(im, (x, 25))
        x += im.width + 20

    out = io.BytesIO()
    canvas.save(out, format="JPEG", quality=JPEG_QUALITY, optimize=True)
    return out.getvalue()

def build_description(client_name: str, address: str, lat: Optional[str], lng: Optional[str]) -> str:
    return (
        f"SCELLÉ NUMERIQUE — Bénéficiaire: {client_name} — Adresse: {address} — "
        f"Coordonnées GPS: Latitude {lat or ''}, Longitude {lng or ''}"
    )

def upload_collages_to_fidealis(session_id: str, description: str, collages: List[Tuple[str, bytes]], progress_cb=None):
    """
    Envoie les collages par 'méga-batches' de 36 (pour RAM), et dans chaque méga-batch,
    appelle l'API Fidealis par sous-batches de 12 (limite Fidealis).
    collages: liste [(filename.jpg, bytes)]
    """
    sent = 0
    for i in range(0, len(collages), 36):
        mega = collages[i:i+36]
        # sous-batches de 12 pour setDeposit
        for j in range(0, len(mega), 12):
            batch = mega[j:j+12]
            data = {
                "key": API_KEY,
                "PHPSESSID": session_id,
                "call": "setDeposit",
                "description": description,
                "type": "deposit",
                "hidden": "0",
                "sendmail": "1",
                "background": "2",
            }
            for idx, (fname, jpeg_bytes) in enumerate(batch, start=1):
                data[f"filename{idx}"] = fname
                data[f"file{idx}"] = encode_base64_bytes(jpeg_bytes)
            # POST
            r = requests.post(API_URL, data=data, timeout=120)
            # Option: vérifier r.status_code / r.json()
            sent += len(batch)
            if progress_cb:
                progress_cb(sent, len(collages))
    return sent

# =========================
# Streamlit UI
# =========================
st.title("FIDEALIS — Drive → Collages → Upload (Batch 36 / 12)")

# Connexion Fidealis
session_id = api_login()
if not session_id:
    st.error("Échec de la connexion à Fidealis (PHPSESSID manquant). Vérifie API_URL/API_KEY/ACCOUNT_KEY.")
    st.stop()

credit_data = get_credit(session_id)
if isinstance(credit_data, dict):
    st.write(f"Crédit restant (Produit 4) : {get_quantity_for_product_4(credit_data)}")

root_input = st.text_input("URL ou ID du dossier Drive racine (contenant les sous-dossiers `Client - Adresse`)")

max_dim = st.slider("Dimension max (px)", 800, 4000, MAX_DIM, step=100)
jpeg_quality = st.slider("Qualité JPEG", 50, 95, JPEG_QUALITY, step=1)

if st.button("Lancer le traitement"):
    try:
        root_id = extract_folder_id(root_input)
    except Exception as e:
        st.error(f"ID/URL de dossier invalide : {e}")
        st.stop()

    # Instancier Drive
    try:
        drive = get_drive_service()
    except Exception as e:
        st.error(f"Impossible d'initialiser l'API Google Drive : {e}")
        st.stop()

    # Lister sous-dossiers "Client - Adresse"
    subfolders = list_subfolders(drive, root_id)
    if not subfolders:
        st.warning("Aucun sous-dossier trouvé sous ce dossier racine.")
        st.stop()

    st.info(f"{len(subfolders)} sous-dossiers détectés.")

    # Compteurs globaux
    total_images_global = 0
    total_collages_global = 0
    total_collages_sent_global = 0

    # Pour l’affichage temps réel
    overall_photos_progress = st.progress(0.0, text="Photos traitées (global)")
    overall_api_progress = st.progress(0.0, text="Collages envoyés à l'API (global)")
    overall_status = st.empty()

    # Parcours de chaque sous-dossier
    for sf_idx, folder in enumerate(subfolders, start=1):
        folder_name = folder["name"]
        folder_id = folder["id"]

        # Parse "Client - Adresse"
        m = re.match(r'^\s*(.+?)\s*-\s*(.+)\s*$', folder_name)
        client_name = m.group(1) if m else folder_name
        address = m.group(2) if m else ""

        # Géocodage (optionnel)
        lat, lng = get_coordinates(address) if address else (None, None)
        description = build_description(client_name, address, lat, lng)

        st.subheader(f"Dossier {sf_idx}/{len(subfolders)} — {folder_name}")
        images = list_images_in_folder(drive, folder_id)
        total_images = len(images)
        if total_images == 0:
            st.write("Aucune image. On passe.")
            continue

        total_images_global += total_images
        # Nombre de collages = ceil(n/3)
        collages_expected = math.ceil(total_images / 3)
        total_collages_global += collages_expected

        # UI par dossier
        photos_bar = st.progress(0.0, text=f"Photos traitées : 0 / {total_images}")
        api_bar = st.progress(0.0, text=f"Collages envoyés : 0 / {collages_expected}")
        status = st.empty()

        # Pipeline : on travaille par groupe de 3 images -> produit 1 collage
        collages_buffer: List[Tuple[str, bytes]] = []
        photos_done = 0
        collages_done = 0

        def update_global_bars():
            # met à jour barres globales
            done_photos_ratio = photos_global_done / max(total_images_global, 1)
            done_collages_ratio = (total_collages_sent_global) / max(total_collages_global, 1)
            overall_photos_progress.progress(done_photos_ratio, text=f"Photos traitées (global) : {photos_global_done} / {total_images_global}")
            overall_api_progress.progress(done_collages_ratio, text=f"Collages envoyés (global) : {total_collages_sent_global} / {total_collages_global}")

        # On calcule au fur et à mesure
        photos_global_done = 0  # pour ce dossier, on l’ajoutera au global au fil de l’eau

        # Télécharger → préprocess → composer collages → upload par 36 → soumettre par 12
        # On évite d'accumuler trop en RAM : on fait un cycle "3 img -> 1 collage" puis on flush par paquets de 36
        for i in range(0, total_images, 3):
            group = images[i:i+3]

            # Télécharge et préprocess chaque image du groupe
            group_jpegs = []
            for f in group:
                try:
                    raw = download_image_bytes(drive, f["id"])
                    jp = load_preprocess_jpeg(raw, max_dim=max_dim, quality=jpeg_quality)
                    group_jpegs.append(jp)
                except Exception as e:
                    st.warning(f"Image sautée ({f.get('name','?')}): {e}")

            # Si rien dans le groupe, continue
            if not group_jpegs:
                photos_done += len(group)  # on considère traitées (skippées)
                photos_global_done += len(group)
                photos_bar.progress(min(1.0, photos_done / total_images), text=f"Photos traitées : {photos_done} / {total_images}")
                update_global_bars()
                continue

            # Collage 1..3 images
            try:
                collage_bytes = create_collage_from_three(group_jpegs)
                collage_idx = collages_done + 1
                collage_name = f"c_{client_name}_{collage_idx:05d}.jpg"
                collages_buffer.append((collage_name, collage_bytes))
                collages_done += 1
            except Exception as e:
                st.error(f"Erreur collage groupe {i//3+1}: {e}")

            # Mise à jour des compteurs "photos"
            photos_done += len(group)
            photos_global_done += len(group)
            photos_bar.progress(min(1.0, photos_done / total_images), text=f"Photos traitées : {photos_done} / {total_images}")

            # Dès qu’on a 36 collages en tampon, on envoie ce “méga-batch”
            if len(collages_buffer) >= 36 or (i + 3) >= total_images:
                # Progress callback pour ce dossier
                sent_before = collages_done - len(collages_buffer)
                def cb(sent_now, total_in_this_flush):
                    # sent_now est cumulatif dans ce flush. On mappe sur la progression du dossier.
                    sent_total = sent_before + sent_now
                    api_bar.progress(min(1.0, sent_total / collages_expected), text=f"Collages envoyés : {min(sent_total, collages_expected)} / {collages_expected}")

                try:
                    upload_collages_to_fidealis(session_id, description, collages_buffer, progress_cb=lambda s,t: cb(s, t))
                    total_collages_sent_global += len(collages_buffer)
                except Exception as e:
                    st.error(f"Erreur d’envoi Fidealis (méga-batch) : {e}")
                    # on vide quand même pour continuer avec le reste
                finally:
                    collages_buffer.clear()
                    # Mise à jour globale
                    update_global_bars()

        # Fin du sous-dossier
        api_bar.progress(1.0, text=f"Collages envoyés : {collages_done} / {collages_expected}")
        status.success(f"✅ Dossier terminé — {collages_done} collages envoyés (à partir de {photos_done} photos).")
        overall_status.info(f"Avancement global — {total_collages_sent_global} collages envoyés / {total_collages_global} attendus.")

    st.success("🎉 Traitement terminé pour tous les dossiers détectés.")
