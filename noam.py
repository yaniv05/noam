import streamlit as st
import os
import io
import re
import base64
import zipfile
import tempfile
import requests
from PIL import Image, ImageOps

# =========================
# Config (env)
# =========================
API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")
ACCOUNT_KEY = os.getenv("ACCOUNT_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# =========================
# Helpers
# =========================
def get_coordinates(address: str):
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    r = requests.get(url, params={"address": address, "key": GOOGLE_API_KEY}, timeout=30)
    if r.status_code == 200:
        data = r.json()
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

def api_upload_files(description: str, filepaths: list[str], session_id: str):
    """
    Envoie les fichiers (collages) par lots de 12 à Fidealis.
    filepaths: chemins locaux des JPG à envoyer
    """
    for i in range(0, len(filepaths), 12):
        batch_files = filepaths[i:i + 12]
        data = {
            "key": API_KEY,
            "PHPSESSID": session_id,
            "call": "setDeposit",
            "description": description,
            "type": "deposit",
            "hidden": "0",
            "sendmail": "1",
            "background": "2",  # réponse plus rapide côté Fidealis
        }
        for idx, fp in enumerate(batch_files, start=1):
            with open(fp, "rb") as f:
                encoded = base64.b64encode(f.read()).decode("utf-8")
            data[f"filename{idx}"] = os.path.basename(fp)
            data[f"file{idx}"] = encoded
        requests.post(API_URL, data=data, timeout=60)

def preprocess_to_jpeg_bytes(raw_bytes: bytes, max_dim=1600, quality=80) -> bytes:
    """
    Redresse EXIF, convertit en RGB, redimensionne, compresse en JPEG.
    Retourne les bytes JPEG.
    """
    with Image.open(io.BytesIO(raw_bytes)) as img:
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

def create_collage(images: list[Image.Image], output_path: str):
    """
    Crée un collage à partir d'objets PIL Image et l'enregistre en output_path (JPEG).
    Collage horizontal avec marge blanche.
    """
    min_h = min(img.size[1] for img in images)
    resized = [ImageOps.fit(img, (int(img.size[0] * min_h / img.size[1]), min_h)) for img in images]
    total_w = sum(img.size[0] for img in resized) + (len(resized) - 1) * 20 + 50
    collage = Image.new("RGB", (total_w, min_h + 50), (255, 255, 255))
    x = 25
    for img in resized:
        collage.paste(img, (x, 25))
        x += img.size[0] + 20
    collage.save(output_path, "JPEG", quality=80, optimize=True)
    collage.close()
    for img in images:
        img.close()

def create_all_collages(filepaths: list[str], client_name: str, workdir: str) -> list[str]:
    """
    Lit les images des filepaths, crée un collage par groupe de 3.
    Enregistre les collages dans workdir et retourne la liste des chemins des collages.
    """
    collages = []
    # Regroupe par 3
    for i in range(0, len(filepaths), 3):
        group = filepaths[i:i + 3]
        pil_images = []
        for fp in group:
            with open(fp, "rb") as f:
                jb = preprocess_to_jpeg_bytes(f.read())  # normalise tout en JPEG
            img = Image.open(io.BytesIO(jb))
            pil_images.append(ImageOps.exif_transpose(img) if hasattr(ImageOps, "exif_transpose") else img)
        collage_name = f"c_{client_name}_{len(collages) + 1}.jpg"
        collage_path = os.path.join(workdir, collage_name)
        create_collage(pil_images, collage_path)
        collages.append(collage_path)
    # Renommer le 1er collage comme dans ta logique existante
    if collages:
        first = collages[0]
        renamed = os.path.join(workdir, f"{client_name}_1.jpg")
        os.replace(first, renamed)
        collages[0] = renamed
    return collages

# Parsing "ClientName - Address"
def parse_folder_name(name: str) -> tuple[str, str] | None:
    # Accepte "Client - Address" avec espaces facultatifs autour du tiret
    m = re.match(r"^\s*(.+?)\s*-\s*(.+?)\s*$", name)
    if not m:
        return None
    return m.group(1).strip(), m.group(2).strip()

# Extensions images acceptées
IMG_EXTS = {".jpg", ".jpeg", ".png", ".JPG", ".JPEG", ".PNG"}

# =========================
# UI
# =========================
st.title("FIDEALIS — Upload ZIP multi-clients (collages + batchs)")

# Login Fidealis une fois
session_id = api_login()
if not session_id:
    st.error("Échec de la connexion à Fidealis.")
    st.stop()

st.success("Connecté à Fidealis.")
zip_file = st.file_uploader("Chargez un ZIP contenant des sous-dossiers `ClientName - Address`", type=["zip"])

with st.expander("Options"):
    max_dim = st.slider("Dimension max (px)", 800, 4000, 1600, step=100)
    jpeg_quality = st.slider("Qualité JPEG (collage)", 50, 95, 80, step=1)
    geocode_if_missing = st.checkbox("Géocoder même si l'adresse paraît valide", value=False)

if st.button("Traiter le ZIP") and zip_file:
    # Espace de travail temporaire
    with tempfile.TemporaryDirectory(dir="/tmp") as tmpdir:
        zpath = os.path.join(tmpdir, "upload.zip")
        with open(zpath, "wb") as f:
            f.write(zip_file.read())

        # Extraire en /tmp
        with zipfile.ZipFile(zpath, "r") as z:
            z.extractall(tmpdir)

        # Lister les sous-dossiers (clients)
        # On considère tous les dossiers de 1er niveau (pas les fichiers isolés à la racine)
        client_dirs = []
        for entry in os.listdir(tmpdir):
            full = os.path.join(tmpdir, entry)
            if os.path.isdir(full) and entry != "__MACOSX":
                client_dirs.append(full)

        if not client_dirs:
            st.error("Aucun sous-dossier trouvé dans le ZIP.")
            st.stop()

        st.write(f"📁 {len(client_dirs)} dossier(s) client détecté(s).")

        processed_count = 0
        for cdir in client_dirs:
            folder_name = os.path.basename(cdir)
            parsed = parse_folder_name(folder_name)
            if not parsed:
                st.warning(f"Ignoré (nom invalide) : {folder_name} — attendu `ClientName - Address`.")
                continue

            client_name, address = parsed
            # Récupérer coordonnées
            lat, lng = get_coordinates(address) if (geocode_if_missing or True) else (None, None)
            if lat is None or lng is None:
                st.warning(f"Adresse non géocodée pour '{folder_name}'. Dépôt avec adresse brute.")
                lat, lng = ("N/A", "N/A")

            # Collecter les images de ce sous-dossier (triées par nom)
            files = []
            for root, _, fnames in os.walk(cdir):
                for fn in sorted(fnames):
                    ext = os.path.splitext(fn)[1]
                    if ext in IMG_EXTS:
                        files.append(os.path.join(root, fn))

            if not files:
                st.warning(f"Aucune image dans '{folder_name}'.")
                continue

            st.info(f"👤 {client_name} — {len(files)} image(s)")
            # Normaliser/convertir en JPEG compressé vers /tmp (pour économiser RAM)
            normalized_paths = []
            for idx, fp in enumerate(files, start=1):
                with open(fp, "rb") as f:
                    jb = preprocess_to_jpeg_bytes(f.read(), max_dim=max_dim, quality=jpeg_quality)
                outp = os.path.join(tmpdir, f"{client_name}_{idx:05d}.jpg")
                with open(outp, "wb") as o:
                    o.write(jb)
                normalized_paths.append(outp)

            # Collages par 3
            collages = create_all_collages(normalized_paths, client_name, tmpdir)

            # Description (format identique à ta version)
            description = (
                f"SCELLÉ NUMERIQUE Bénéficiaire: Nom: {client_name}, "
                f"Adresse: {address}, Coordonnées GPS: Latitude {lat}, Longitude {lng}"
            )

            # Envoi à Fidealis par lots de 12
            api_upload_files(description, collages, session_id)

            processed_count += 1
            st.success(f"✅ Dépôt envoyé pour '{client_name}' ({len(collages)} collage(s)).")

        st.success(f"🎉 Traitement terminé. {processed_count} client(s) traités.")

else:
    st.caption("Charge un fichier .zip puis clique sur 'Traiter le ZIP'.")
