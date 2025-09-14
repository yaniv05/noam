import os
import io
import time
import base64
from typing import List, Tuple, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import streamlit as st
from PIL import Image, ImageOps, ImageFile

ImageFile.LOAD_TRUNCATED_IMAGES = True

# =========================
# Config
# =========================
API_URL = os.getenv("API_URL")                     # ex: https://api.fidealis.com/basic_v3.php
API_KEY = os.getenv("API_KEY")
ACCOUNT_KEY = os.getenv("ACCOUNT_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# Performances & robustesse
MAX_FILES_PER_DEPOSIT = 12
CONCURRENCY = 6
HTTP_TIMEOUT = 60
RETRIES = 3
BACKOFF_BASE = 2

# Compression
MAX_DIM = 1600
JPEG_QUALITY = 70
BATCH_MAX_PAYLOAD_MB = 8  # borne douce ~ taille totale form-data (base64) par requête

# =========================
# HTTP session persistante
# =========================
SESSION = requests.Session()

def http_get(url, params=None, timeout=HTTP_TIMEOUT):
    return SESSION.get(url, params=params, timeout=timeout)

def http_post(url, data=None, timeout=HTTP_TIMEOUT):
    return SESSION.post(url, data=data, timeout=timeout)


# =========================
# Géocodage Google
# =========================
def get_coordinates(address: str):
    if not address:
        return None, None
    resp = http_get(
        "https://maps.googleapis.com/maps/api/geocode/json",
        params={"address": address, "key": GOOGLE_API_KEY},
    )
    if resp.status_code != 200:
        return None, None
    data = resp.json()
    if data.get("status") == "OK" and data.get("results"):
        loc = data["results"][0]["geometry"]["location"]
        return loc["lat"], loc["lng"]
    return None, None


# =========================
# API Fidealis
# =========================
def api_login() -> str | None:
    resp = http_get(
        API_URL,
        params={"key": API_KEY, "call": "loginUserFromAccountKey", "accountKey": ACCOUNT_KEY},
    )
    if resp.status_code != 200:
        return None
    data = resp.json()
    return data.get("PHPSESSID")

def get_credit(session_id: str):
    resp = http_get(
        API_URL,
        params={"key": API_KEY, "PHPSESSID": session_id, "call": "getCredits", "product_ID": ""},
    )
    if resp.status_code == 200:
        return resp.json()
    return None

def get_quantity_for_product_4(credit_data):
    try:
        return credit_data["4"]["quantity"]
    except Exception:
        return "N/A"


# =========================
# Images : compression
# =========================
def compress_bytes_to_jpeg(src_bytes: bytes, max_dim=MAX_DIM, quality=JPEG_QUALITY) -> bytes:
    with Image.open(io.BytesIO(src_bytes)) as img:
        img = ImageOps.exif_transpose(img)
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        w, h = img.size
        scale = min(1.0, max_dim / max(w, h))
        if scale < 1.0:
            img = img.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
        out = io.BytesIO()
        img.save(out, format="JPEG", quality=quality, optimize=True)
        return out.getvalue()

def normalize_and_compress_uploaded_file(uploaded) -> Tuple[str, bytes]:
    raw = uploaded.read()
    name = os.path.splitext(os.path.basename(uploaded.name))[0]
    jpeg_bytes = compress_bytes_to_jpeg(raw, MAX_DIM, JPEG_QUALITY)
    return f"{name}.jpg", jpeg_bytes

def encode_b64(content: bytes) -> str:
    return base64.b64encode(content).decode("utf-8")


# =========================
# Batch builder
# =========================
def build_adaptive_batches(files: List[Tuple[str, bytes]],
                           max_per_batch=MAX_FILES_PER_DEPOSIT,
                           max_payload_mb=BATCH_MAX_PAYLOAD_MB) -> List[List[Tuple[str, str]]]:
    batches: List[List[Tuple[str, str]]] = []
    cur: List[Tuple[str, str]] = []
    cur_bytes = 0
    max_payload_bytes = max_payload_mb * 1024 * 1024

    for fname, data in files:
        b64 = encode_b64(data)
        approx = len(b64)
        if cur and (len(cur) >= max_per_batch or cur_bytes + approx > max_payload_bytes):
            batches.append(cur)
            cur = []
            cur_bytes = 0
        cur.append((fname, b64))
        cur_bytes += approx

    if cur:
        batches.append(cur)
    return batches


# =========================
# Upload Fidealis
# =========================
def make_deposit_payload(session_id: str, description: str, items: List[Tuple[str, str]], extra: Dict | None = None):
    data: Dict[str, str] = {
        "key": API_KEY,
        "PHPSESSID": session_id,
        "call": "setDeposit",
        "description": description,
        "type": "deposit",
        "hidden": "0",
        "sendmail": "0",  # accélère : pas d'email automatique Fidealis
        "background": "2",
    }
    if extra:
        data.update({k: str(v) for k, v in extra.items()})
    for idx, (fname, b64) in enumerate(items, start=1):
        data[f"filename{idx}"] = fname
        data[f"file{idx}"] = b64
    return data

def post_with_retry(data: Dict[str, str]):
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            resp = http_post(API_URL, data=data, timeout=HTTP_TIMEOUT)
            if resp.status_code == 200:
                return resp
            last_err = RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
        except Exception as e:
            last_err = e
        time.sleep(BACKOFF_BASE ** (attempt - 1))
    raise last_err

def upload_batches(session_id: str, description: str,
                   batches: List[List[Tuple[str, str]]],
                   extra: Dict | None = None,
                   on_progress=None):
    results = []
    total = len(batches)
    done = 0

    def worker(batch_items):
        payload = make_deposit_payload(session_id, description, batch_items, extra=extra)
        return post_with_retry(payload)

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futs = [pool.submit(worker, b) for b in batches]
        for fut in as_completed(futs):
            res = fut.result()
            results.append(res)
            done += 1
            if on_progress:
                on_progress(done, total)
    return results


# =========================
# UI Streamlit
# =========================
st.title("Dépôt FIDEALIS — Optimisé gros volumes")

# Connexion
session_id = api_login()
if not session_id:
    st.error("Échec de connexion API Fidealis.")
    st.stop()

credit = get_credit(session_id)
if isinstance(credit, dict):
    st.write(f"Crédit restant (Produit 4) : {get_quantity_for_product_4(credit)}")
else:
    st.warning("Impossible de récupérer les crédits.")

client_name = st.text_input("Nom du client")
address = st.text_input("Adresse complète (ex: 123 rue Exemple, Paris, France)")

col1, col2 = st.columns(2)
with col1:
    latitude = st.text_input("Latitude", value=st.session_state.get("latitude", ""))
with col2:
    longitude = st.text_input("Longitude", value=st.session_state.get("longitude", ""))

if st.button("Générer GPS"):
    if address:
        lat, lng = get_coordinates(address)
        if lat is not None:
            st.session_state["latitude"] = str(lat)
            st.session_state["longitude"] = str(lng)
            latitude, longitude = st.session_state["latitude"], st.session_state["longitude"]
            st.success(f"Coordonnées : {latitude}, {longitude}")
        else:
            st.error("Adresse introuvable.")

uploaded_files = st.file_uploader(
    "Photos (JPEG/PNG/HEIC)", accept_multiple_files=True, type=["jpg", "jpeg", "png", "heic", "webp"]
)

if st.button("Soumettre"):
    if not (client_name and address and uploaded_files):
        st.error("Nom, adresse et fichiers requis.")
        st.stop()

    # Préparation
    st.info("Compression des images…")
    prep_bar = st.progress(0.0)
    prepared: List[Tuple[str, bytes]] = []
    for i, up in enumerate(uploaded_files, start=1):
        try:
            fname, jpeg_bytes = normalize_and_compress_uploaded_file(up)
            prepared.append((f"{client_name}_{i:05d}.jpg", jpeg_bytes))
        except Exception as e:
            st.error(f"Erreur compression {up.name}: {e}")
        prep_bar.progress(i / max(1, len(uploaded_files)))

    if not prepared:
        st.error("Aucun fichier valide.")
        st.stop()

    # Découpage
    batches = build_adaptive_batches(prepared)
    st.write(f"{len(prepared)} images → {len(batches)} lots (≤ {MAX_FILES_PER_DEPOSIT} fichiers et ~{BATCH_MAX_PAYLOAD_MB} MiB/lot).")

    # Description
    description = f"SCELLÉ NUMERIQUE — Bénéficiaire: {client_name} — Adresse: {address} — GPS: lat {latitude}, lon {longitude}"
    extras = {}

    # Upload
    st.info("Envoi vers Fidealis…")
    send_bar = st.progress(0.0)
    status = st.empty()

    def on_progress(done, total):
        send_bar.progress(done / total)
        status.write(f"Lots envoyés : {done}/{total}")

    try:
        responses = upload_batches(session_id, description, batches, extra=extras, on_progress=on_progress)
        ok = sum(1 for r in responses if r.status_code == 200)
        st.success(f"Terminé : {ok}/{len(responses)} lots OK.")
    except Exception as e:
        st.error(f"Échec d'envoi : {e}")
