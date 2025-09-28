import os
import io
import re
import json
import uuid
import base64
import zipfile
import tempfile
import requests
import streamlit as st
import boto3
from typing import List, Tuple, Optional
from PIL import Image, ImageOps, ImageFile

# --- Tolérer certains JPEG tronqués ---
ImageFile.LOAD_TRUNCATED_IMAGES = True

# ========= ENV =========
API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")
ACCOUNT_KEY = os.getenv("ACCOUNT_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.getenv("R2_BUCKET")
R2_REGION = os.getenv("R2_REGION", "auto")  # R2 accepte "auto"

R2_ENDPOINT = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

# ========= R2 (S3-compatible) client =========
s3 = boto3.client(
    "s3",
    region_name=R2_REGION,
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
)

# ========= Constantes =========
IMG_EXTS = {".jpg", ".jpeg", ".png", ".JPG", ".JPEG", ".PNG"}

# ========= Fidealis API =========
def api_login() -> Optional[str]:
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
    Envoie les collages à Fidealis par lots de 12 (depuis le disque local).
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
            "background": "2",
        }
        for idx, fp in enumerate(batch, start=1):
            with open(fp, "rb") as f:
                encoded = base64.b64encode(f.read()).decode("utf-8")
            data[f"filename{idx}"] = os.path.basename(fp)
            data[f"file{idx}"] = encoded
        requests.post(API_URL, data=data, timeout=60)

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

# ========= Geocoding =========
def get_coordinates(address: str) -> Tuple[Optional[float], Optional[float]]:
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

# ========= Images (préprocess + collages) =========
def preprocess_to_jpeg_bytes(raw_bytes: bytes, max_dim=1600, quality=80) -> bytes:
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
    collages: List[str] = []
    for i in range(0, len(filepaths), 3):
        group = filepaths[i:i + 3]
        pil_images: List[Image.Image] = []
        for fp in group:
            with open(fp, "rb") as f:
                jb = preprocess_to_jpeg_bytes(f.read(), max_dim=max_dim, quality=quality)
            img = Image.open(io.BytesIO(jb))
            pil_images.append(ImageOps.exif_transpose(img))
        collage_name = f"c_{client_name}_{len(collages) + 1}.jpg"
        collage_path = os.path.join(workdir, collage_name)
        create_collage(pil_images, collage_path, quality=quality)
        collages.append(collage_path)

    if collages:
        first = collages[0]
        renamed = os.path.join(workdir, f"{client_name}_1.jpg")
        os.replace(first, renamed)
        collages[0] = renamed
    return collages

# ========= ZIP parsing (ClientName - Address) =========
def parse_folder_name(name: str) -> Optional[Tuple[str, str]]:
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
    entries = [e for e in os.listdir(extract_root) if e not in {"__MACOSX", ".DS_Store"}]
    fulls   = [os.path.join(extract_root, e) for e in entries]
    only_dirs = [p for p in fulls if os.path.isdir(p)]
    has_images_top = any(
        os.path.splitext(e)[1] in IMG_EXTS and os.path.isfile(os.path.join(extract_root, e))
        for e in entries
    )
    base = extract_root
    if len(only_dirs) == 1 and not has_images_top:
        base = only_dirs[0]
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

# ========= R2 Presign (POST) =========
def r2_presign_post(key: str, content_type: str, max_mb=1024, expires=3600):
    """
    Génère un pre-signed POST pour envoyer un fichier depuis le navigateur vers R2.
    """
    return s3.generate_presigned_post(
        Bucket=R2_BUCKET,
        Key=key,
        Fields={"Content-Type": content_type},
        Conditions=[
            {"Content-Type": content_type},
            ["content-length-range", 1, max_mb * 1024 * 1024],
        ],
        ExpiresIn=expires,
    )

# ========= UI =========
st.title("FIDEALIS — Démo upload direct R2 (ZIP) → traitement multi-clients")

# Connexion Fidealis
session_id = api_login()
if not session_id:
    st.error("Connexion Fidealis échouée. Vérifie API_URL / API_KEY / ACCOUNT_KEY.")
    st.stop()

credits = get_credit(session_id)
if isinstance(credits, dict):
    st.info(f"Crédit restant (Produit 4) : {get_quantity_for_product_4(credits)}")

with st.expander("Options"):
    max_dim = st.slider("Dimension max (px) avant collage", 800, 4000, 1600, step=100)
    jpeg_quality = st.slider("Qualité JPEG", 50, 95, 80, step=1)

# 1) Générer un pre-signed pour un ZIP à uploader direct → R2
zip_local = st.file_uploader("Sélectionne un ZIP (démo : upload direct vers R2)", type=["zip"])
if st.button("Uploader ce ZIP directement vers R2") and zip_local:
    # On ne passe pas le binaire du ZIP dans Streamlit → on redemande le fichier côté navigateur (sécurité 0 DEMO)
    # On fabrique une clé unique dans R2
    key = f"uploads/demo/{uuid.uuid4()}_{zip_local.name}"
    pres = r2_presign_post(key, "application/zip", max_mb=2048, expires=3600)  # jusqu'à 2 Go démo

    st.session_state["last_zip_key"] = key
    st.write("Clé R2:", key)

    st.components.v1.html(f"""
<html><body>
<h4>Uploader le même ZIP vers R2</h4>
<input id="zipfile" type="file" accept=".zip" />
<pre id="log" style="white-space:pre-wrap;border:1px solid #eee;padding:8px;border-radius:6px;"></pre>
<script>
const pres = {json.dumps(pres)};
const log = (m) => document.getElementById('log').textContent += m + "\\n";
document.getElementById('zipfile').addEventListener('change', async (ev) => {{
  const f = ev.target.files[0];
  if (!f) return;
  const form = new FormData();
  Object.entries(pres.fields).forEach(([k,v]) => form.append(k,v));
  form.append('Content-Type', 'application/zip');
  form.append('file', f);
  const t0 = performance.now();
  const res = await fetch(pres.url, {{ method:'POST', body: form }});
  const dt = ((performance.now()-t0)/1000).toFixed(2);
  log(res.ok ? "OK upload en "+dt+"s" : "FAIL "+res.status+" en "+dt+"s");
}});
</script>
</body></html>
""", height=260)
    st.info("Après l'upload, clique sur “Traiter le ZIP depuis R2” ci-dessous.")

# 2) Traiter le ZIP depuis R2 (pipeline complet)
if st.button("Traiter le ZIP depuis R2") and st.session_state.get("last_zip_key"):
    zip_key = st.session_state["last_zip_key"]
    with tempfile.TemporaryDirectory(dir="/tmp") as tmpdir:
        # Télécharger le ZIP depuis R2 → fichier local
        zip_path = os.path.join(tmpdir, "batch.zip")
        with open(zip_path, "wb") as f:
            s3.download_fileobj(R2_BUCKET, zip_key, f)

        # Extraire
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(tmpdir)

        # Détecter dossiers clients
        client_dirs = find_client_dirs(tmpdir)
        if not client_dirs:
            st.error("Aucun sous-dossier client `ClientName - Address` trouvé dans le ZIP.")
            st.stop()

        st.write(f"📁 {len(client_dirs)} client(s) détecté(s).")
        p_clients = st.progress(0.0)

        for idx, cdir in enumerate(client_dirs, start=1):
            folder = os.path.basename(cdir)
            parsed = parse_folder_name(folder)
            if not parsed:
                st.warning(f"Ignoré (nom invalide) : {folder}")
                p_clients.progress(idx / len(client_dirs))
                continue

            client_name, address = parsed
            lat, lng = get_coordinates(address)
            if lat is None or lng is None:
                lat, lng = ("N/A", "N/A")
                st.warning(f"⚠️ Géocodage indisponible pour: {folder}")

            # Lister images du dossier
            raw_files = list_images_under(cdir)
            if not raw_files:
                st.warning(f"Aucune image dans: {folder}")
                p_clients.progress(idx / len(client_dirs))
                continue

            st.info(f"👤 {client_name} — {len(raw_files)} image(s)")
            # Normaliser en JPEG vers /tmp (noms client_index)
            normalized: List[str] = []
            p_imgs = st.progress(0.0)
            for j, fp in enumerate(raw_files, start=1):
                try:
                    with open(fp, "rb") as f:
                        jb = preprocess_to_jpeg_bytes(f.read(), max_dim=max_dim, quality=jpeg_quality)
                    outp = os.path.join(tmpdir, f"{client_name}_{j:05d}.jpg")
                    with open(outp, "wb") as o:
                        o.write(jb)
                    normalized.append(outp)
                except Exception as e:
                    st.error(f"Erreur normalisation {os.path.basename(fp)} : {e}")
                p_imgs.progress(j / len(raw_files))

            if not normalized:
                st.error(f"Échec normalisation pour: {client_name}")
                p_clients.progress(idx / len(client_dirs))
                continue

            # Collages par 3 + renommage du 1er
            collages = create_all_collages(normalized, client_name, tmpdir, max_dim=max_dim, quality=jpeg_quality)
            if not collages:
                st.error(f"Échec collages pour: {client_name}")
                p_clients.progress(idx / len(client_dirs))
                continue

            description = (
                f"SCELLÉ NUMERIQUE Bénéficiaire: Nom: {client_name}, "
                f"Adresse: {address}, Coordonnées GPS: Latitude {lat}, Longitude {lng}"
            )

            # Envoi Fidealis par lots de 12 (comme avant)
            api_upload_files(description, collages, session_id)

            st.success(f"✅ {client_name} — {len(collages)} collage(s) envoyé(s).")
            p_clients.progress(idx / len(client_dirs))

        st.balloons()
        st.success("🎉 Traitement terminé.")

else:
    st.caption("1) Upload ZIP → R2, 2) Traiter depuis R2.")
