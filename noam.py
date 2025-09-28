# app.py
import os
import io
import re
import base64
import zipfile
import tempfile
import requests
import streamlit as st
from typing import List, Tuple, Optional
from PIL import Image, ImageOps, ImageFile

# Tolérer certains JPEG tronqués
ImageFile.LOAD_TRUNCATED_IMAGES = True

# =========================
# Config via ENV
# =========================
API_URL = os.getenv("API_URL")                 # ex: https://www.fidealis.com/api/
API_KEY = os.getenv("API_KEY")
ACCOUNT_KEY = os.getenv("ACCOUNT_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# =========================
# Constantes
# =========================
IMG_EXTS = {".jpg", ".jpeg", ".png", ".JPG", ".JPEG", ".PNG"}

# =========================
# Helpers API
# =========================
def api_login() -> Optional[str]:
    """Connexion à Fidealis -> retourne PHPSESSID."""
    try:
        r = requests.get(
            API_URL,
            params={"key": API_KEY, "call": "loginUserFromAccountKey", "accountKey": ACCOUNT_KEY},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        return data.get("PHPSESSID")
    except Exception:
        return None

def api_upload_files(description: str, filepaths: List[str], session_id: str):
    """
    Envoie les collages à Fidealis par lots de 12.
    filepaths: chemins locaux .jpg
    """
    for i in range(0, len(filepaths), 12):
        batch = filepaths[i:i + 12]
        data = {
            "key": API_KEY,
            "PHPSESSID": session_id,
            "call": "setDeposit",
            "description": description,
            "type": "deposit",
            "hidden": "0",
            "sendmail": "1",
            "background": "2",  # traitement côté Fidealis en arrière-plan
        }
        for idx, fp in enumerate(batch, start=1):
            with open(fp, "rb") as f:
                encoded = base64.b64encode(f.read()).decode("utf-8")
            data[f"filename{idx}"] = os.path.basename(fp)
            data[f"file{idx}"] = encoded

        # POST par lot
        resp = requests.post(API_URL, data=data, timeout=60)
        # Optionnel: vérifier les erreurs de l'API Fidealis
        # resp.raise_for_status()

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
# Helpers géocodage
# =========================
def get_coordinates(address: str) -> Tuple[Optional[float], Optional[float]]:
    """Retourne (lat, lng) via Google Geocoding ou (None, None)."""
    if not GOOGLE_API_KEY:
        return None, None
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    try:
        r = requests.get(url, params={"address": address, "key": GOOGLE_API_KEY}, timeout=30)
        r.raise_for_status()
        data = r.json()
        if data.get("status") == "OK" and data.get("results"):
            loc = data["results"][0]["geometry"]["location"]
            return loc["lat"], loc["lng"]
    except Exception:
        pass
    return None, None

# =========================
# Helpers images
# =========================
def preprocess_to_jpeg_bytes(raw_bytes: bytes, max_dim=1600, quality=80) -> bytes:
    """
    - Redresse EXIF
    - Convertit en RGB si nécessaire
    - Redimensionne pour que max(width, height) <= max_dim
    - Exporte en JPEG 'quality'
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

def create_collage(pil_images: List[Image.Image], output_path: str, quality=80):
    """
    Collage horizontal: marge blanche 25px autour, 20px entre images, hauteur normalisée.
    """
    min_h = min(img.size[1] for img in pil_images)
    resized = [ImageOps.fit(img, (int(img.size[0] * min_h / img.size[1]), min_h)) for img in pil_images]
    total_w = sum(img.size[0] for img in resized) + (len(resized) - 1) * 20 + 50
    collage = Image.new("RGB", (total_w, min_h + 50), (255, 255, 255))
    x = 25
    for img in resized:
        collage.paste(img, (x, 25))
        x += img.size[0] + 20
    collage.save(output_path, "JPEG", quality=quality, optimize=True)
    collage.close()
    for im in pil_images:
        im.close()

def create_all_collages(filepaths: List[str], client_name: str, workdir: str, max_dim=1600, quality=80) -> List[str]:
    """
    Crée des collages par groupe de 3 images.
    - Normalise chaque image (JPEG) avant de l'ouvrir avec PIL pour le collage.
    - Sauvegarde les collages dans 'workdir'.
    - Renomme le 1er collage en '{client_name}_1.jpg' (comme dans ton code actuel).
    Retourne les chemins des collages.
    """
    collages: List[str] = []
    for i in range(0, len(filepaths), 3):
        group = filepaths[i:i + 3]
        pil_images: List[Image.Image] = []

        for fp in group:
            with open(fp, "rb") as f:
                jb = preprocess_to_jpeg_bytes(f.read(), max_dim=max_dim, quality=quality)
            img = Image.open(io.BytesIO(jb))
            pil_images.append(ImageOps.exif_transpose(img) if hasattr(ImageOps, "exif_transpose") else img)

        collage_name = f"c_{client_name}_{len(collages) + 1}.jpg"
        collage_path = os.path.join(workdir, collage_name)
        create_collage(pil_images, collage_path, quality=quality)
        collages.append(collage_path)

    # Renommer le premier collage comme tu le fais d’habitude
    if collages:
        first = collages[0]
        renamed = os.path.join(workdir, f"{client_name}_1.jpg")
        os.replace(first, renamed)
        collages[0] = renamed

    return collages

# =========================
# Helpers ZIP / dossiers clients
# =========================
def parse_folder_name(name: str) -> Optional[Tuple[str, str]]:
    """
    Parse 'ClientName - Address' (espaces facultatifs).
    """
    m = re.match(r"^\s*(.+?)\s*-\s*(.+?)\s*$", name)
    if not m:
        return None
    return m.group(1).strip(), m.group(2).strip()

def dir_contains_images(path: str) -> bool:
    for _, _, files in os.walk(path):
        for fn in files:
            if os.path.splitext(fn)[1] in IMG_EXTS:
                return True
    return False

def find_client_dirs(extract_root: str) -> List[str]:
    """
    Gère les ZIP avec un dossier racine unique OU avec les dossiers clients au niveau racine.
    On retient les sous-dossiers de 1er niveau dont le nom matche 'Client - Address' ET contenant des images.
    """
    entries = [e for e in os.listdir(extract_root) if e not in {"__MACOSX", ".DS_Store"}]
    fulls   = [os.path.join(extract_root, e) for e in entries]

    only_dirs = [p for p in fulls if os.path.isdir(p)]
    has_images_top = any(
        os.path.splitext(e)[1] in IMG_EXTS and os.path.isfile(os.path.join(extract_root, e))
        for e in entries
    )

    base = extract_root
    if len(only_dirs) == 1 and not has_images_top:
        base = only_dirs[0]  # descendre dans le dossier racine unique

    client_dirs = []
    for e in os.listdir(base):
        p = os.path.join(base, e)
        if os.path.isdir(p) and e not in {"__MACOSX", ".DS_Store"}:
            if parse_folder_name(e) and dir_contains_images(p):
                client_dirs.append(p)

    return client_dirs

def list_images_under(path: str) -> List[str]:
    files: List[str] = []
    for root, _, fnames in os.walk(path):
        for fn in sorted(fnames):
            if os.path.splitext(fn)[1] in IMG_EXTS:
                files.append(os.path.join(root, fn))
    return files

# =========================
# UI
# =========================
st.title("FIDEALIS — Upload ZIP multi-clients (collages + envoi par lots)")

# Connexion Fidealis
session_id = api_login()
if not session_id:
    st.error("Échec de la connexion à Fidealis. Vérifie API_URL / API_KEY / ACCOUNT_KEY.")
    st.stop()

credits = get_credit(session_id)
if isinstance(credits, dict):
    st.info(f"Crédit restant (Produit 4) : {get_quantity_for_product_4(credits)}")

zip_file = st.file_uploader(
    "Chargez un .zip contenant des sous-dossiers `ClientName - Address` (chacun avec des photos)",
    type=["zip"]
)

with st.expander("Options"):
    max_dim = st.slider("Dimension max (px) avant collage", 800, 4000, 1600, step=100)
    jpeg_quality = st.slider("Qualité JPEG (collage & normalisation)", 50, 95, 80, step=1)
    force_geocode = st.checkbox("Toujours géocoder (même si l'adresse paraît déjà propre)", value=True)

if st.button("Traiter le ZIP") and zip_file:
    with tempfile.TemporaryDirectory(dir="/tmp") as tmpdir:
        # Écrire le zip sur disque
        zpath = os.path.join(tmpdir, "upload.zip")
        with open(zpath, "wb") as f:
            f.write(zip_file.read())

        # Extraire
        with zipfile.ZipFile(zpath, "r") as z:
            z.extractall(tmpdir)

        # Trouver les dossiers clients
        client_dirs = find_client_dirs(tmpdir)
        if not client_dirs:
            st.error("Aucun sous-dossier client trouvé. Attendu des dossiers nommés `ClientName - Address` contenant des images.")
            st.stop()

        st.write(f"📁 {len(client_dirs)} client(s) détecté(s).")
        progress_clients = st.progress(0.0)

        for idx, cdir in enumerate(client_dirs, start=1):
            folder_name = os.path.basename(cdir)
            parsed = parse_folder_name(folder_name)
            if not parsed:
                st.warning(f"Ignoré (nom invalide) : {folder_name}")
                progress_clients.progress(idx / len(client_dirs))
                continue

            client_name, address = parsed

            # Géocodage
            lat, lng = (None, None)
            if force_geocode or True:
                lat, lng = get_coordinates(address)
            if lat is None or lng is None:
                lat, lng = ("N/A", "N/A")
                st.warning(f"⚠️ Géocodage indisponible pour: {folder_name} — envoi avec adresse brute.")

            # Lister images
            raw_files = list_images_under(cdir)
            if not raw_files:
                st.warning(f"⚠️ Aucune image dans: {folder_name}")
                progress_clients.progress(idx / len(client_dirs))
                continue

            st.info(f"👤 {client_name} — {len(raw_files)} image(s) détectée(s)")

            # Normalisation JPEG vers /tmp
            normalized_paths: List[str] = []
            progress_imgs = st.progress(0.0)
            for j, fp in enumerate(raw_files, start=1):
                try:
                    with open(fp, "rb") as f:
                        jb = preprocess_to_jpeg_bytes(f.read(), max_dim=max_dim, quality=jpeg_quality)
                    outp = os.path.join(tmpdir, f"{client_name}_{j:05d}.jpg")
                    with open(outp, "wb") as o:
                        o.write(jb)
                    normalized_paths.append(outp)
                except Exception as e:
                    st.error(f"Erreur normalisation {os.path.basename(fp)} : {e}")
                progress_imgs.progress(j / len(raw_files))

            if not normalized_paths:
                st.error(f"Échec normalisation pour: {client_name}")
                progress_clients.progress(idx / len(client_dirs))
                continue

            # Collages par 3 + renommage du 1er
            collages = create_all_collages(normalized_paths, client_name, tmpdir, max_dim=max_dim, quality=jpeg_quality)
            if not collages:
                st.error(f"Échec création de collages pour: {client_name}")
                progress_clients.progress(idx / len(client_dirs))
                continue

            # Description identique à ton format actuel
            description = (
                f"SCELLÉ NUMERIQUE Bénéficiaire: Nom: {client_name}, "
                f"Adresse: {address}, Coordonnées GPS: Latitude {lat}, Longitude {lng}"
            )

            # Envoi à Fidealis par batches de 12
            api_upload_files(description, collages, session_id)

            st.success(f"✅ {client_name} — {len(collages)} collage(s) envoyé(s) à Fidealis.")
            progress_clients.progress(idx / len(client_dirs))

        st.balloons()
        st.success("🎉 Traitement terminé pour tous les clients détectés.")

else:
    st.caption("Charge un ZIP puis clique sur 'Traiter le ZIP'.")
