import os
import io
import re
import math
import json
import base64
import urllib.parse
import requests
import streamlit as st
import threading
import queue
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

# Géocodage Google Maps (optionnel)
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# *** Clé Google Drive pour accès public ***
GOOGLE_DRIVE_API_KEY = os.getenv("GOOGLE_DRIVE_API_KEY")   # PUBLIC ONLY

MAX_DIM = int(os.getenv("MAX_DIM", "1600"))        # redimensionnement max (px)
JPEG_QUALITY = int(os.getenv("JPEG_QUALITY", "80"))

# =========================
# Google Drive (PUBLIC-ONLY via API key) — resourceKey aware
# =========================
def extract_folder_id_and_rk(maybe_url: str) -> Tuple[str, Optional[str]]:
    """
    Retourne (folder_id, resource_key_ou_None) à partir d'une URL Drive ou d'un ID.
    Gère les URLs contenant ?resourcekey=...
    """
    m = re.search(r'/folders/([a-zA-Z0-9_-]+)', maybe_url or "")
    if m:
        folder_id = m.group(1)
        parsed = urllib.parse.urlparse(maybe_url)
        q = urllib.parse.parse_qs(parsed.query or "")
        rk = (q.get("resourcekey") or q.get("resourceKey") or [None])[0]
        return folder_id, rk
    if maybe_url and re.fullmatch(r'[a-zA-Z0-9_-]{20,}', maybe_url):
        return maybe_url, None
    raise ValueError("Impossible de détecter l'ID du dossier : fournis une URL Drive ou un ID.")

def _drive_list_children_public(parent_id: str, parent_rk: Optional[str], q_extra: Optional[str] = None) -> List[dict]:
    """
    Liste les éléments d'un dossier PUBLIC (Anyone with link).
    Ajoute resourceKey dans les champs retournés.
    """
    if not GOOGLE_DRIVE_API_KEY:
        raise RuntimeError("GOOGLE_DRIVE_API_KEY manquant dans l'environnement.")

    base_url = "https://www.googleapis.com/drive/v3/files"
    q = f"'{parent_id}' in parents and trashed=false"
    if q_extra:
        q += f" {q_extra}"

    params = {
        "q": q,
        "fields": "nextPageToken, files(id,name,mimeType,size,createdTime,parents,resourceKey)",
        "key": GOOGLE_DRIVE_API_KEY,
        "pageSize": 1000,
        "supportsAllDrives": "true",
        "includeItemsFromAllDrives": "true",
    }
    if parent_rk:
        params["resourceKey"] = parent_rk

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

def list_subfolders_public(parent_id: str, parent_rk: Optional[str]) -> List[dict]:
    return _drive_list_children_public(
        parent_id,
        parent_rk,
        q_extra="and mimeType='application/vnd.google-apps.folder'"
    )

def list_images_public(parent_id: str, parent_rk: Optional[str]) -> List[dict]:
    files = _drive_list_children_public(
        parent_id,
        parent_rk,
        q_extra="and mimeType contains 'image/'"
    )
    files.sort(key=lambda f: f.get('name', ''))
    return files

def download_file_public(file_id: str, file_resource_key: Optional[str]) -> bytes:
    """
    Télécharge un fichier PUBLIC.
    1) Drive API alt=media (+ resourceKey + supportsAllDrives + acknowledgeAbuse)
    2) Fallback URL publique uc?export=download&id=...&resourcekey=...
    """
    if not GOOGLE_DRIVE_API_KEY:
        raise RuntimeError("GOOGLE_DRIVE_API_KEY manquant dans l'environnement.")

    # Essai 1 : API Drive alt=media
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}"
    params = {
        "alt": "media",
        "key": GOOGLE_DRIVE_API_KEY,
        "supportsAllDrives": "true",
        "acknowledgeAbuse": "true",
    }
    if file_resource_key:
        params["resourceKey"] = file_resource_key

    r = requests.get(url, params=params, timeout=120)
    if r.status_code == 200:
        return r.content

    # Essai 2 : URL publique uc
    uc_params = {"export": "download", "id": file_id}
    if file_resource_key:
        uc_params["resourcekey"] = file_resource_key

    r2 = requests.get("https://drive.google.com/uc", params=uc_params, timeout=120)
    if r2.status_code == 200 and r2.content:
        return r2.content

    msg = f"Public download forbidden for file {file_id} (HTTP {r.status_code})."
    if r.text:
        msg += f" Detail: {r.text[:200]}"
    raise requests.HTTPError(msg)

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
# Fidealis API — envois par batch de 12
# =========================
def fidealis_send_batch(session_id: str, description: str, batch: List[Tuple[str, bytes]]):
    """
    Envoie un batch (<=12) à Fidealis via setDeposit.
    batch: list[(filename.jpg, bytes)]
    """
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
        data[f"file{idx}"] = base64.b64encode(jpeg_bytes).decode("utf-8")

    r = requests.post(API_URL, data=data, timeout=120)
    r.raise_for_status()
    return r

# =========================
# Uploader parallèle par dossier (producer/consumer)
# =========================
class FidealisUploader:
    """
    - queue maxsize=36 pour limiter la RAM (≈ 36 photos = 12 collages).
    - Le worker dépile et envoie par sous-batches de 12.
    - On peut pousser les collages au fil de l'eau (par groupe de 3 images).
    """
    def __init__(self, session_id: str, description: str, on_progress=None):
        self.session_id = session_id
        self.description = description
        self.on_progress = on_progress  # callback(sent_count_increment)
        self.q: "queue.Queue[Optional[Tuple[str, bytes]]]" = queue.Queue(maxsize=36)
        self.sent = 0
        self._err: Optional[Exception] = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        buffer: List[Tuple[str, bytes]] = []
        try:
            while not self._stop.is_set():
                item = self.q.get()
                if item is None:
                    if buffer:
                        self._send_buffer(buffer)
                        buffer.clear()
                    break
                buffer.append(item)
                if len(buffer) >= 12:
                    self._send_buffer(buffer)
                    buffer.clear()
        except Exception as e:
            self._err = e

    def _send_buffer(self, buffer: List[Tuple[str, bytes]]):
        fidealis_send_batch(self.session_id, self.description, buffer)
        self.sent += len(buffer)
        if self.on_progress:
            try:
                self.on_progress(len(buffer))
            except Exception:
                pass

    def put(self, item: Tuple[str, bytes]):
        self.q.put(item)

    def close(self):
        self.q.put(None)
        self._thread.join()
        if self._err:
            raise self._err

# =========================
# Préprocessing & Collage
# =========================
def load_preprocess_jpeg(img_bytes: bytes, max_dim: int = MAX_DIM, quality: int = JPEG_QUALITY) -> bytes:
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

# =========================
# Fidealis API (login/credits)
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

# =========================
# Streamlit UI
# =========================
st.title("FIDEALIS — Drive (Public) → Collages → Upload (Parallèle 12 / buffer 36)")

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
        root_id, root_rk = extract_folder_id_and_rk(root_input)
    except Exception as e:
        st.error(f"ID/URL de dossier invalide : {e}")
        st.stop()

    # Lister sous-dossiers "Client - Adresse" (public only)
    try:
        subfolders = list_subfolders_public(root_id, root_rk)
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

    # NEW: compteurs globaux mutables (pour éviter nonlocal/global)
    photos_processed_global_box = {"value": 0}
    total_collages_sent_global_box = {"value": 0}
    total_collages_sent_lock = threading.Lock()

    # Pour l’affichage temps réel
    overall_photos_progress = st.progress(0.0, text="Photos traitées (global)")
    overall_api_progress = st.progress(0.0, text="Collages envoyés à l'API (global)")
    overall_status = st.empty()

    # Calcul total d'images pour le dénominateur global
    for f in subfolders:
        try:
            imgs = list_images_public(f["id"], f.get("resourceKey"))
            total_images_global += len(imgs)
            total_collages_global += math.ceil(len(imgs) / 3) if imgs else 0
        except Exception:
            pass

    # Parcours de chaque sous-dossier
    for sf_idx, folder in enumerate(subfolders, start=1):
        folder_name = folder["name"]
        folder_id = folder["id"]
        folder_rk  = folder.get("resourceKey")

        # Parse "Client - Adresse"
        m = re.match(r'^\s*(.+?)\s*-\s*(.+)\s*$', folder_name)
        client_name = m.group(1) if m else folder_name
        address = m.group(2) if m else ""

        # Géocodage (optionnel)
        lat, lng = get_coordinates(address) if address else (None, None)
        description = build_description(client_name, address, lat, lng)

        st.subheader(f"Dossier {sf_idx}/{len(subfolders)} — {folder_name}")

        try:
            images = list_images_public(folder_id, folder_rk)
        except Exception as e:
            st.warning(f"Impossible de lister les images (public) pour {folder_name} : {e}")
            continue

        total_images = len(images)
        if total_images == 0:
            st.write("Aucune image. On passe.")
            continue

        collages_expected = math.ceil(total_images / 3)

        # UI par dossier
        photos_bar = st.progress(0.0, text=f"Photos traitées : 0 / {total_images}")
        api_bar = st.progress(0.0, text=f"Collages envoyés : 0 / {collages_expected}")
        status = st.empty()

        photos_done = 0

        # NEW: compteur mutable pour ce dossier
        collages_done_box = {"value": 0}

        # Callback de progression pour ce dossier (appelé par le worker)
        def on_folder_progress(increment: int):
            with total_collages_sent_lock:
                collages_done_box["value"] += increment
                total_collages_sent_global_box["value"] += increment

        # Uploader parallèle pour ce dossier
        uploader = FidealisUploader(session_id, description, on_progress=on_folder_progress)

        def update_global_bars():
            # Global photos
            done_photos_ratio = photos_processed_global_box["value"] / max(total_images_global or 1, 1)
            overall_photos_progress.progress(
                min(1.0, done_photos_ratio),
                text=f"Photos traitées (global) : {photos_processed_global_box['value']} / {total_images_global}"
            )
            # Global collages
            done_collages_ratio = total_collages_sent_global_box["value"] / max(total_collages_global or 1, 1)
            overall_api_progress.progress(
                min(1.0, done_collages_ratio),
                text=f"Collages envoyés (global) : {total_collages_sent_global_box['value']} / {total_collages_global}"
            )
            # Dossier
            api_bar.progress(
                min(1.0, collages_done_box["value"] / max(collages_expected, 1)),
                text=f"Collages envoyés : {collages_done_box['value']} / {collages_expected}"
            )

        # Pipeline : 3 images -> 1 collage
        for i in range(0, total_images, 3):
            group = images[i:i+3]

            # Télécharge et préprocess chaque image du groupe
            group_jpegs = []
            for f in group:
                try:
                    raw = download_file_public(f["id"], f.get("resourceKey"))
                    jp = load_preprocess_jpeg(raw, max_dim=max_dim, quality=jpeg_quality)
                    group_jpegs.append(jp)
                except Exception as e:
                    st.warning(f"Image sautée ({f.get('name','?')}): {e}")

            # Si on n'a rien (toutes sautées), on continue
            if not group_jpegs:
                photos_done += len(group)
                photos_processed_global_box["value"] += len(group)
                photos_bar.progress(min(1.0, photos_done / total_images), text=f"Photos traitées : {photos_done} / {total_images}")
                update_global_bars()
                continue

            # Collage 1..3 images -> push vers uploader (prod/cons)
            try:
                collage_bytes = create_collage_from_three(group_jpegs)
                collage_idx = (i // 3) + 1  # ordre dans le dossier
                collage_name = f"c_{client_name}_{collage_idx:05d}.jpg"
                uploader.put((collage_name, collage_bytes))  # bloque si buffer plein (36)
            except Exception as e:
                st.error(f"Erreur collage groupe {i//3+1}: {e}")

            # Mise à jour des compteurs "photos"
            photos_done += len(group)
            photos_processed_global_box["value"] += len(group)
            photos_bar.progress(min(1.0, photos_done / total_images), text=f"Photos traitées : {photos_done} / {total_images}")

            # Rafraîchit les barres globales et d'upload du dossier
            update_global_bars()

        # Fin de production pour ce dossier -> on ferme l'uploader (flush & join)
        try:
            uploader.close()
        except Exception as e:
            st.error(f"Erreur d’envoi Fidealis dans le worker : {e}")

        # Finalise les barres pour ce dossier
        api_bar.progress(1.0, text=f"Collages envoyés : {collages_done_box['value']} / {collages_expected}")
        status.success(f"Dossier terminé — {collages_done_box['value']} collages envoyés (à partir de {photos_done} photos).")
        overall_status.info(
            f"Avancement global — {total_collages_sent_global_box['value']} collages envoyés / {total_collages_global} attendus."
        )
        update_global_bars()

    st.success("Traitement terminé pour tous les dossiers détectés.")
