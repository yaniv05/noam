import os
import time
import base64
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import streamlit as st
from PIL import Image, ImageOps

# =========================
# Configuration
# =========================
API_URL = os.getenv("API_URL")                     # ex: https://api.fidealis.com/basic_v3.php
API_KEY = os.getenv("API_KEY")
ACCOUNT_KEY = os.getenv("ACCOUNT_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# Limites / perfs
MAX_FILES_PER_DEPOSIT = 12        # Limite API Fidealis (par appel setDeposit)
CONCURRENCY = 4                   # Nb. de lots envoyés en parallèle (3-5 recommandé)
HTTP_TIMEOUT = 60                 # s
RETRIES = 3                       # tentatives par lot
BACKOFF_BASE = 2                  # 1s,2s,4s par défaut

# Compression images
MAX_DIM = 2048                    # px (max(width,height))
JPEG_QUALITY = 80                 # 1..95
MAKE_COLLAGES = False             # True = collages par 3; False = envoi image par image


# =========================
# Utilitaires
# =========================
def http_get(url, params=None, timeout=HTTP_TIMEOUT):
    return requests.get(url, params=params, timeout=timeout)

def http_post(url, data=None, timeout=HTTP_TIMEOUT):
    return requests.post(url, data=data, timeout=timeout)

def encode_file_b64(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


# =========================
# Google Geocoding
# =========================
def get_coordinates(address: str):
    if not address:
        return None, None
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    resp = http_get(url, params={"address": address, "key": GOOGLE_API_KEY})
    if resp.status_code != 200:
        return None, None
    data = resp.json()
    if data.get("status") == "OK" and data.get("results"):
        loc = data["results"][0]["geometry"]["location"]
        return loc["lat"], loc["lng"]
    return None, None


# =========================
# Fidealis API
# =========================
def api_login():
    params = {
        "key": API_KEY,
        "call": "loginUserFromAccountKey",
        "accountKey": ACCOUNT_KEY,
    }
    resp = http_get(API_URL, params=params)
    resp.raise_for_status()
    data = resp.json()
    return data.get("PHPSESSID")

def get_credit(session_id: str):
    params = {
        "key": API_KEY,
        "PHPSESSID": session_id,
        "call": "getCredits",
        "product_ID": "",  # vide => tous les produits
    }
    resp = http_get(API_URL, params=params)
    if resp.status_code == 200:
        return resp.json()
    return None

def get_quantity_for_product_4(credit_data):
    try:
        return credit_data["4"]["quantity"]
    except Exception:
        return "N/A"

def make_deposit_payload(session_id: str, description: str, paths: list, extra: dict | None = None):
    data = {
        "key": API_KEY,
        "PHPSESSID": session_id,
        "call": "setDeposit",
        "description": description,
        "type": "deposit",
        "hidden": "0",
        "sendmail": "1",
        "background": "2",  # traitement asynchrone côté Fidealis (recommandé gros volumes)
    }
    if extra:
        data.update(extra)  # GPS, legend, receiverCopy, etc.

    for idx, path in enumerate(paths, start=1):
        data[f"filename{idx}"] = os.path.basename(path)
        data[f"file{idx}"] = encode_file_b64(path)

    return data

def post_with_retry(data):
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            resp = http_post(API_URL, data=data)
            if resp.status_code == 200:
                return resp
            last_err = RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            last_err = e
        time.sleep(BACKOFF_BASE ** (attempt - 1))
    raise last_err

def upload_many_files(session_id: str, description: str, all_paths: list[str], extra: dict | None = None, progress_cb=None):
    # Découpage en lots de 12
    batches = [all_paths[i:i + MAX_FILES_PER_DEPOSIT] for i in range(0, len(all_paths), MAX_FILES_PER_DEPOSIT)]
    total = len(batches)
    done = 0

    def worker(batch_paths):
        payload = make_deposit_payload(session_id, description, batch_paths, extra=extra)
        return post_with_retry(payload)

    results = []
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futures = [pool.submit(worker, b) for b in batches]
        for fut in as_completed(futures):
            resp = fut.result()  # raise si échec après retries
            results.append(resp)
            done += 1
            if progress_cb:
                progress_cb(done, total)
    return results


# =========================
# Image processing
# =========================
def compress_image(src_path: str, dst_path: str, max_dim=MAX_DIM, quality=JPEG_QUALITY):
    img = Image.open(src_path).convert("RGB")
    w, h = img.size
    scale = min(1.0, max_dim / max(w, h))
    if scale < 1.0:
        img = img.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
    img.save(dst_path, format="JPEG", quality=quality, optimize=True)

def create_collage(images: list[Image.Image], output_path: str, margin=25, gap=20, bg=(255, 255, 255)):
    min_height = min(img.size[1] for img in images)
    resized = [ImageOps.fit(img, (int(img.size[0] * min_height / img.size[1]), min_height)) for img in images]
    total_width = sum(img.size[0] for img in resized) + (len(resized) - 1) * gap + margin * 2
    canvas = Image.new("RGB", (total_width, min_height + margin * 2), bg)
    x = margin
    for im in resized:
        canvas.paste(im, (x, margin))
        x += im.size[0] + gap
    canvas.save(output_path)

def build_collages_from_paths(paths: list[str], client_name: str):
    """Par 3 images -> 1 collage"""
    out_paths = []
    for i in range(0, len(paths), 3):
        group = paths[i:i + 3]
        imgs = [Image.open(p) for p in group]
        out_name = f"c_{client_name}_{len(out_paths) + 1}.jpg"
        create_collage(imgs, out_name)
        out_paths.append(out_name)
        # cleanup PIL
        for im in imgs:
            im.close()
    return out_paths


# =========================
# UI Streamlit
# =========================
st.title("Formulaire de dépôt FIDEALIS – GIA PARTNER (optimisé)")

# Login Fidealis
session_id = None
try:
    session_id = api_login()
except Exception as e:
    st.error(f"Échec de la connexion Fidealis : {e}")

if session_id:
    credit = get_credit(session_id)
    if isinstance(credit, dict):
        st.write(f"Crédit restant (Produit 4): {get_quantity_for_product_4(credit)}")
    else:
        st.warning("Impossible de récupérer les crédits.")

client_name = st.text_input("Nom du client")
address = st.text_input("Adresse complète (ex: 123 rue Exemple, Paris, France)")

# GPS
col1, col2 = st.columns(2)
with col1:
    latitude = st.text_input("Latitude", value=st.session_state.get("latitude", ""))
with col2:
    longitude = st.text_input("Longitude", value=st.session_state.get("longitude", ""))

if st.button("Générer les coordonnées GPS"):
    if address:
        lat, lng = get_coordinates(address)
        if lat is not None:
            st.session_state["latitude"] = str(lat)
            st.session_state["longitude"] = str(lng)
            st.success("Coordonnées récupérées.")
        else:
            st.error("Adresse introuvable.")

uploaded_files = st.file_uploader(
    "Téléchargez les photos (JPEG/PNG) — multiples autorisés",
    accept_multiple_files=True,
    type=["jpg", "jpeg", "png"],
)

make_collages = st.toggle("Créer des collages par 3 images (sinon envoi image par image)", value=MAKE_COLLAGES)

if st.button("Soumettre"):
    if not (session_id and client_name and address and st.session_state.get("latitude") and st.session_state.get("longitude") and uploaded_files):
        st.error("Veuillez remplir tous les champs et sélectionner des photos.")
        st.stop()

    st.info("Préparation des fichiers (compression)…")
    tmp_dir = tempfile.mkdtemp(prefix="fidealis_")
    prepared_paths = []

    # 1) Sauvegarde + compression
    for idx, up in enumerate(uploaded_files, start=1):
        src = os.path.join(tmp_dir, f"src_{idx}.jpg")
        with open(src, "wb") as f:
            f.write(up.read())
        dst = os.path.join(tmp_dir, f"{client_name}_img_{idx}.jpg")
        compress_image(src, dst)          # compression + resize
        prepared_paths.append(dst)
        try:
            os.remove(src)
        except Exception:
            pass

    # 2) Collages (optionnel)
    final_paths = prepared_paths
    if make_collages:
        st.info("Création des collages…")
        final_paths = build_collages_from_paths(prepared_paths, client_name)

        # Renommer le premier collage pour inclure le nom du client
        if final_paths:
            first = final_paths[0]
            renamed = os.path.join(os.path.dirname(first), f"{client_name}_1.jpg")
            try:
                os.replace(first, renamed)
                final_paths[0] = renamed
            except Exception:
                pass

        # Libère les compressées sources si tu ne veux garder que les collages
        for p in prepared_paths:
            try:
                os.remove(p)
            except Exception:
                pass

    # 3) Description + extras API
    description = (
        f"SCELLÉ NUMERIQUE Bénéficiaire: Nom: {client_name}, Adresse: {address}, "
        f"Coordonnées GPS: Latitude {st.session_state['latitude']}, Longitude {st.session_state['longitude']}"
    )
    extras = {
        # Champs optionnels acceptés par setDeposit :
        # "GPS": f"{st.session_state['latitude']},{st.session_state['longitude']}",
        # "legend": client_name,
        # "receiverCopy": "", "resellerCustomer": "", "divulgation": "", "locarno": "", "coordonnees": "",
    }

    # 4) Upload en lots parallélisés
    st.info("Envoi des données à Fidealis…")
    bar = st.progress(0.0)
    status = st.empty()

    def on_progress(done, total):
        bar.progress(done / total)
        status.write(f"Lots envoyés : {done} / {total}")

    try:
        responses = upload_many_files(session_id, description, final_paths, extra=extras, progress_cb=on_progress)
        st.success(f"Envoi terminé ({len(responses)} lots).")
    except Exception as e:
        st.error(f"Échec d'envoi : {e}")

    # 5) Nettoyage des fichiers temporaires
    for p in final_paths:
        try:
            os.remove(p)
        except Exception:
            pass
