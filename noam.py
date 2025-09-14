import os
import io
import base64
import requests
import streamlit as st
from PIL import Image, ImageOps, ImageFile

# Pour gérer certains fichiers tronqués sans crasher
ImageFile.LOAD_TRUNCATED_IMAGES = True

# =========================
# Config
# =========================
API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")
ACCOUNT_KEY = os.getenv("ACCOUNT_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# Préprocessing images
MAX_DIM = 1600          # px max (largeur/hauteur)
JPEG_QUALITY = 80       # 1..95

# =========================
# Helpers
# =========================
def get_coordinates(address: str):
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    response = requests.get(url, params={"address": address, "key": GOOGLE_API_KEY}, timeout=30)
    if response.status_code == 200:
        data = response.json()
        if data.get("status") == "OK" and data.get("results"):
            loc = data["results"][0]["geometry"]["location"]
            return loc["lat"], loc["lng"]
    return None, None

def api_login():
    r = requests.get(
        API_URL,
        params={"key": API_KEY, "call": "loginUserFromAccountKey", "accountKey": ACCOUNT_KEY},
        timeout=30,
    )
    data = r.json()
    return data.get("PHPSESSID")

def encode_base64_bytes(b: bytes) -> str:
    return base64.b64encode(b).decode("utf-8")

def preprocess_to_jpeg_bytes(uploaded_file, max_dim=MAX_DIM, quality=JPEG_QUALITY) -> tuple[str, bytes]:
    """
    Lis le fichier (jpg/png), corrige l'orientation EXIF, convertit en RGB,
    redimensionne pour que max(width,height) == max_dim, puis exporte en JPEG compressé.
    Retourne (nom_sans_ext_en_jpg, bytes_jpeg).
    """
    raw = uploaded_file.read()
    name_wo_ext = os.path.splitext(os.path.basename(uploaded_file.name))[0]

    with Image.open(io.BytesIO(raw)) as img:
        # Respecter l'orientation EXIF
        img = ImageOps.exif_transpose(img)

        # Conversion -> RGB si nécessaire
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")

        # Redimensionnement
        w, h = img.size
        scale = min(1.0, max_dim / max(w, h))
        if scale < 1.0:
            new_w, new_h = int(w * scale), int(h * scale)
            img = img.resize((new_w, new_h), Image.LANCZOS)

        # Export JPEG compressé sans EXIF
        out = io.BytesIO()
        img.save(out, format="JPEG", quality=quality, optimize=True)
        jpeg_bytes = out.getvalue()

    return f"{name_wo_ext}.jpg", jpeg_bytes

def api_upload_files(description: str, prepared_items: list[tuple[str, bytes]], session_id: str):
    """
    prepared_items: liste [(filename.jpg, jpeg_bytes)]
    Envoie par lots de 12 (contrainte API Fidealis), en base64.
    """
    for i in range(0, len(prepared_items), 12):
        batch = prepared_items[i:i + 12]
        data = {
            "key": API_KEY,
            "PHPSESSID": session_id,
            "call": "setDeposit",
            "description": description,
            "type": "deposit",
            "hidden": "0",
            "sendmail": "1",  # mets "0" si tu veux désactiver l'email auto Fidealis
            "background": "2" # traitement côté Fidealis en arrière-plan (réponse plus rapide)
        }
        for idx, (fname, jpeg_bytes) in enumerate(batch, start=1):
            data[f"filename{idx}"] = fname
            data[f"file{idx}"] = encode_base64_bytes(jpeg_bytes)

        # Envoi
        requests.post(API_URL, data=data, timeout=60)

def get_credit(session_id: str):
    r = requests.get(
        API_URL,
        params={"key": API_KEY, "PHPSESSID": session_id, "call": "getCredits", "product_ID": ""},
        timeout=30,
    )
    if r.status_code == 200:
        return r.json()
    return None

def get_quantity_for_product_4(credit_data):
    try:
        return credit_data["4"]["quantity"]
    except Exception:
        return "N/A"

# =========================
# UI
# =========================
st.title("Formulaire de dépôt FIDEALIS — pré-processing optimisé")

session_id = api_login()
if session_id:
    credit_data = get_credit(session_id)
    if isinstance(credit_data, dict):
        st.write(f"Crédit restant (Produit 4) : {get_quantity_for_product_4(credit_data)}")
    else:
        st.error("Échec de la récupération des crédits.")
else:
    st.error("Échec de la connexion à Fidealis.")
    st.stop()

client_name = st.text_input("Nom du client")
address = st.text_input("Adresse complète (ex: 123 rue Exemple, Paris, France)")

# GPS
latitude = st.session_state.get("latitude", "")
longitude = st.session_state.get("longitude", "")

if st.button("Générer les coordonnées GPS"):
    if address:
        lat, lng = get_coordinates(address)
        if lat is not None and lng is not None:
            st.session_state["latitude"] = str(lat)
            st.session_state["longitude"] = str(lng)
            latitude = str(lat)
            longitude = str(lng)
        else:
            st.error("Impossible de générer les coordonnées GPS pour l'adresse fournie.")

latitude = st.text_input("Latitude", value=latitude)
longitude = st.text_input("Longitude", value=longitude)

uploaded_files = st.file_uploader(
    "Téléchargez les photos (JPEG/PNG)", accept_multiple_files=True, type=["jpg", "jpeg", "png"]
)

# Petites options (facultatif)
with st.expander("Options d'optimisation"):
    MAX_DIM = st.slider("Dimension max (px)", 800, 4000, MAX_DIM, step=100)
    JPEG_QUALITY = st.slider("Qualité JPEG", 50, 95, JPEG_QUALITY, step=1)

if st.button("Soumettre"):
    if not client_name or not address or not latitude or not longitude or not uploaded_files:
        st.error("Veuillez remplir tous les champs et sélectionner au moins une photo.")
        st.stop()

    st.info("Pré-processing des images (orientation, redimensionnement, compression)…")
    progress = st.progress(0.0)
    prepared: list[tuple[str, bytes]] = []

    total = len(uploaded_files)
    for idx, up in enumerate(uploaded_files, start=1):
        try:
            # Important : re-positionner le curseur (certains navigateurs)
            up.seek(0)
            fname, jpeg_bytes = preprocess_to_jpeg_bytes(up, max_dim=MAX_DIM, quality=JPEG_QUALITY)
            # Préfixer par client + index pour un nom plus propre côté Fidealis
            prepared.append((f"{client_name}_{idx:05d}.jpg", jpeg_bytes))
        except Exception as e:
            st.error(f"Erreur lors du traitement de {up.name} : {e}")
        progress.progress(idx / total)

    if not prepared:
        st.error("Aucune image valide après pré-processing.")
        st.stop()

    description = (
        f"SCELLÉ NUMERIQUE — Bénéficiaire: {client_name} — Adresse: {address} — "
        f"Coordonnées GPS: Latitude {latitude}, Longitude {longitude}"
    )

    st.info("Envoi à Fidealis (par lots de 12)…")
    api_upload_files(description, prepared, session_id)

    st.success("Dépôt envoyé avec succès !")
