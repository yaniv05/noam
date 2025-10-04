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

# Géocodage Google Maps (optionnel, inchangé)
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# *** Clé Google Drive pour accès public ***
GOOGLE_DRIVE_API_KEY = os.getenv("GOOGLE_DRIVE_API_KEY")   # <-- NOUVELLE VAR D'ENV OBLIGATOIRE (public-only)

MAX_DIM = int(os.getenv("MAX_DIM", "1600"))        # redimensionnement max (px)
JPEG_QUALITY = int(os.getenv("JPEG_QUALITY", "80"))

# =========================
# Google Drive (PUBLIC-ONLY via API key)
# =========================
def extract_folder_id(maybe_url: str) -> str:
    m = re.search(r'/folders/([a-zA-Z0-9_-]+)', maybe_url)
    if m:
        return m.group(1)
    if re.fullmatch(r'[a-zA-Z0-9_-]{20,}', maybe_url):
        return maybe_url
    raise ValueError("Impossible de détecter l'ID du dossier : fournis une URL Drive ou un ID.")

def _drive_list_children_public(parent_id: str, q_extra: Optional[str] = None) -> List[dict]:
    """
    Liste les éléments d'un dossier PUBLIC (Anyone with the link) via API Key.
    q_extra: filtre additionnel, ex. "and mimeType='application/vnd.google-apps.folder'"
    """
    if not GOOGLE_DRIVE_API_KEY:
        raise RuntimeError("GOOGLE_DRIVE_API_KEY manquant dans l'environnement.")

    base_url = "https://www.googleapis.com/drive/v3/files"
    q = f"'{parent_id}' in parents and trashed=false"
    if q_extra:
        q += f" {q_extra}"

    params = {
        "q": q,
        "fields": "nextPageToken, files(id,name,mimeType,size,createdTime,parents)",
        "key": GOOGLE_DRIVE_API_KEY,
        "pageSize": 1000,
        "supportsAllDrives": "false",
        "includeItemsFromAllDrives": "false",
    }

    files = []
    while True:
        r = requests.get(base_url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        files.extend(data.get("files", []))
        token = data.get("nextPageToken")
        if not token:
            break
        params["pageToken"] = token
    return files

def list_subfolders_public(parent_id: str) -> List[dict]:
    return _drive_list_children_public(
        parent_id,
        q_extra="and mimeType='application/vnd.google-apps.folder'"
    )

def list_images_public(parent_id: str) -> List[dict]:
    files = _drive_list_children_public(
        parent_id,
        q_extra="and mimeType contains 'image/'"
    )
    # Tri par nom pour stabilité
    files.sort(key=lambda f: f.get('name', ''))
    return files

def download_file_public(file_id: str) -> bytes:
    """
    Télécharge le contenu d'un fichier PUBLIC (ou 'Anyone with link').
    """
    if not GOOGLE_DRIVE_API_KEY:
        raise RuntimeError("GOOGLE_DRIVE_API_KEY manquant dans l'environnement.")

    url = f"https://www.googleapis.com/drive/v3/files/{file_id}"
    params = {"alt": "media", "key": GOOGLE_DRIVE_API_KEY}
    r = requests.get(url, params=params, timeout=120)
    r.raise_for_status()
    return r.content

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
    min_h = min(im.height for im in images)
    resized = []
    for im in images:
        scale = min_h / im.height
        new_w = int(im.width * scale)
        resized.append(im.resize((new_w, min_h), Image.LANCZOS))

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
            r = requests.post(API_URL, data=data, timeout=120)
            sent += len(batch)
            if progress_cb:
                progress_cb(sent, len(collages))
    return sent

# =========================
# Streamlit UI
# =========================
st.title("FIDEALIS — Drive (Public) → Collages → Upload (Batch 36 / 12)")

# Vérif API key Drive
if not GOOGLE_DRIVE_API_KEY:
    st.error("GOOGLE_DRIVE_API_KEY manquant. Ajoute la clé d'API Google Drive dans l'environnement.")
    st.stop()

# Connexion Fidealis
session_id = api_login()
if not session_id:
    st.error("Échec de la connexion à Fidealis (PHPSESSID manquant). Vérifie API_URL/API_KEY/ACCOUNT_KEY.")
    st.stop()

credit_data = get_credit(session_id)
if isinstance(credit_data, dict):
    st.write(f"Crédit restant (Produit 4) : {get_quantity_for_product_4(credit_data)}")

root_input = st.text_input("URL ou ID du dossier Drive PUBLIC (contenant les sous-dossiers `Client - Adresse`)")

max_dim = st.slider("Dimension max (px)", 800, 4000, MAX_DIM, step=100)
jpeg_quality = st.slider("Qualité JPEG", 50, 95, JPEG_QUALITY, step=1)

if st.button("Lancer le traitement"):
    try:
        root_id = extract_folder_id(root_input)
    except Exception as e:
        st.error(f"ID/URL de dossier invalide : {e}")
        st.stop()

    # Lister sous-dossiers "Client - Adresse" (public only)
    try:
        subfolders = list_subfolders_public(root_id)
    except Exception as e:
        st.error(f"Erreur liste sous-dossiers (public): {e}")
        st.stop()

    if not subfolders:
        st.warning("Aucun sous-dossier trouvé sous ce dossier PUBLIC. Vérifie le partage 'Anyone with the link'.")
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

        try:
            images = list_images_public(folder_id)
        except Exception as e:
            st.warning(f"Impossible de lister les images (public) pour {folder_name} : {e}")
            continue

        total_images = len(images)
        if total_images == 0:
            st.write("Aucune image. On passe.")
            continue

        total_images_global += total_images
        collages_expected = math.ceil(total_images / 3)
        total_collages_global += collages_expected

        # UI par dossier
        photos_bar = st.progress(0.0, text=f"Photos traitées : 0 / {total_images}")
        api_bar = st.progress(0.0, text=f"Collages envoyés : 0 / {collages_expected}")
        status = st.empty()

        collages_buffer: List[Tuple[str, bytes]] = []
        photos_done = 0
        collages_done = 0

        def update_global_bars():
            done_photos_ratio = photos_global_done / max(total_images_global, 1)
            done_collages_ratio = (total_collages_sent_global) / max(total_collages_global, 1)
            overall_photos_progress.progress(done_photos_ratio, text=f"Photos traitées (global) : {photos_global_done} / {total_images_global}")
            overall_api_progress.progress(done_collages_ratio, text=f"Collages envoyés (global) : {total_collages_sent_global} / {total_collages_global}")

        photos_global_done = 0

        # Pipeline : 3 images -> 1 collage
        for i in range(0, total_images, 3):
            group = images[i:i+3]

            # Télécharge et préprocess chaque image du groupe
            group_jpegs = []
            for f in group:
                try:
                    raw = download_file_public(f["id"])
                    jp = load_preprocess_jpeg(raw, max_dim=max_dim, quality=jpeg_quality)
                    group_jpegs.append(jp)
                except Exception as e:
                    st.warning(f"Image sautée ({f.get('name','?')}): {e}")

            if not group_jpegs:
                photos_done += len(group)
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

            # Flush par méga-batch de 36 collages (ou fin)
            if len(collages_buffer) >= 36 or (i + 3) >= total_images:
                sent_before = collages_done - len(collages_buffer)
                def cb(sent_now, total_in_this_flush):
                    sent_total = sent_before + sent_now
                    api_bar.progress(min(1.0, sent_total / collages_expected), text=f"Collages envoyés : {min(sent_total, collages_expected)} / {collages_expected}")

                try:
                    upload_collages_to_fidealis(session_id, description, collages_buffer, progress_cb=lambda s,t: cb(s, t))
                    total_collages_sent_global += len(collages_buffer)
                except Exception as e:
                    st.error(f"Erreur d’envoi Fidealis (méga-batch) : {e}")
                finally:
                    collages_buffer.clear()
                    update_global_bars()

        # Fin du sous-dossier
        api_bar.progress(1.0, text=f"Collages envoyés : {collages_done} / {collages_expected}")
        status.success(f"Dossier terminé — {collages_done} collages envoyés (à partir de {photos_done} photos).")
        overall_status.info(f"Avancement global — {total_collages_sent_global} collages envoyés / {total_collages_global} attendus.")

    st.success("Traitement terminé pour tous les dossiers détectés.")
