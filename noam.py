# app.py
import os
import io
import re
import json
import uuid
import base64
import tempfile
import requests
import streamlit as st
import boto3
from typing import List, Tuple, Optional, Dict
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
    """Envoie les collages à Fidealis par lots de 12 (depuis le disque local)."""
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

# ========= Regrouper par "ClientName - Address" via clés R2 =========
CLIENT_FOLDER_RE = re.compile(r"^\s*(.+?)\s*-\s*(.+?)\s*$")

def split_client_address(foldername: str) -> Optional[Tuple[str, str]]:
    m = CLIENT_FOLDER_RE.match(foldername)
    if not m:
        return None
    return m.group(1).strip(), m.group(2).strip()

def list_objects(prefix: str) -> List[str]:
    """Liste toutes les clés sous un préfixe R2/S3."""
    keys: List[str] = []
    token = None
    while True:
        kw = {"Bucket": R2_BUCKET, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for obj in resp.get("Contents", []):
            keys.append(obj["Key"])
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys

def group_keys_by_client(keys: List[str], batch_prefix: str) -> Dict[str, List[str]]:
    """
    Regroupe les clés par dossier client = 1er segment après batch_prefix.
    On s'attend à batch_prefix + "<ClientName - Address>/**"
    """
    groups: Dict[str, List[str]] = {}
    for k in keys:
        if not k.startswith(batch_prefix):
            continue
        # extrait le chemin relatif après prefix
        rel = k[len(batch_prefix):]
        # rel = "<ClientName - Address>/.../file.jpg"
        parts = rel.split("/", 1)
        if len(parts) < 2:
            continue
        top = parts[0]  # dossier client
        if split_client_address(top):
            groups.setdefault(top, []).append(k)
    # trier les listes pour un ordre stable
    for v in groups.values():
        v.sort()
    return groups

# ========= Pre-signed POST (starts-with) =========
def r2_presign_post_for_prefix(prefix: str, max_mb=1024, expires=3600):
    """
    Génère un pre-signed POST **réutilisable** pour tout objet dont la clé commence par `prefix`.
    NB: pas de champ 'key' figé; on met une Condition 'starts-with' sur $key.
    Le navigateur devra inclure un champ `key` pour CHAQUE fichier.
    """
    # boto3 ne propose pas directement 'starts-with' sur $key via arguments high-level;
    # mais on peut "tricher" en passant Fields minimal + Conditions custom.
    # On utilise une petite astuce: on génère d'abord un POST avec une key bidon puis on remplace la policy.
    # → plus simple: on appelle generate_presigned_post sans 'Key', et on fournit Conditions.
    return s3.generate_presigned_post(
        Bucket=R2_BUCKET,
        Key=prefix + "${filename}",  # champ 'Key' par défaut (sera écrasé par notre field 'key')
        Fields={
            "Content-Type": "application/octet-stream",
        },
        Conditions=[
            ["starts-with", "$key", prefix],
            ["starts-with", "$Content-Type", ""],
            ["content-length-range", 1, max_mb * 1024 * 1024],
        ],
        ExpiresIn=expires,
    )

# ========= UI =========
st.title("FIDEALIS — Démo dossier → R2 (uploads parallèles) → traitement multi-clients")

# Connexion Fidealis
session_id = api_login()
if not session_id:
    st.error("Connexion Fidealis échouée. Vérifie API_URL / API_KEY / ACCOUNT_KEY.")
    st.stop()

credits = get_credit(session_id)
if isinstance(credits, dict):
    st.info(f"Crédit restant (Produit 4) : {get_quantity_for_product_4(credits)}")

with st.expander("Options de traitement"):
    max_dim = st.slider("Dimension max (px) avant collage", 800, 4000, 1600, step=100)
    jpeg_quality = st.slider("Qualité JPEG", 50, 95, 80, step=1)

# 1) Prépare un batch et une politique POST "starts-with"
if st.button("1) Créer un batch & obtenir l'URL d'upload"):
    batch_id = str(uuid.uuid4())
    prefix = f"uploads/{batch_id}/"
    post = r2_presign_post_for_prefix(prefix, max_mb=2048, expires=3600)  # 2 Go/fichier démo
    st.session_state["batch_prefix"] = prefix
    st.session_state["post"] = post
    st.success(f"Batch créé. Préfixe R2: {prefix}")

    # Formulaire HTML : input dossier + upload parallèle (sécurité 0, démo)
    st.components.v1.html(f"""
<!doctype html><html><body>
<h4>Sélectionne un dossier (avec sous-dossiers par client)</h4>
<input id="picker" type="file" webkitdirectory directory multiple />
<br/><br/>
<label>Parallèle max:</label> <input id="k" type="number" value="8" min="1" max="16" />
<button id="go">Uploader vers R2</button>
<pre id="log" style="white-space:pre-wrap;border:1px solid #eee;padding:8px;border-radius:6px;max-height:300px;overflow:auto"></pre>
<script>
const pres = {json.dumps(post)};
const prefix = {json.dumps(prefix)};
const log = (m) => document.getElementById('log').textContent += m + "\\n";

async function uploadOne(file, relPath) {{
  // Clé S3 = prefix + chemin relatif (on normalise les séparateurs et supprime ./)
  const key = prefix + relPath.replace(/^\\.\\//,'').replaceAll('\\\\','/');

  const form = new FormData();
  // champs signés
  Object.entries(pres.fields).forEach(([k,v]) => form.append(k,v));
  // notre 'key' réel (autorisé par starts-with)
  form.append('key', key);
  // type
  form.append('Content-Type', file.type || 'application/octet-stream');
  // fichier
  form.append('file', file);

  const t0 = performance.now();
  const res = await fetch(pres.url, {{ method: 'POST', body: form }});
  const dt = ((performance.now()-t0)/1000).toFixed(2);
  if (!res.ok) throw new Error("HTTP " + res.status + " pour " + relPath + " ("+dt+"s)");
  return {{ key, sec: dt }};
}}

document.getElementById('go').addEventListener('click', async () => {{
  const inp = document.getElementById('picker');
  const files = Array.from(inp.files || []);
  if (!files.length) {{ log("Aucun fichier sélectionné."); return; }}
  const k = Math.max(1, Math.min(16, parseInt(document.getElementById('k').value || '8')));
  log("Fichiers: " + files.length + " | parallèle=" + k);

  // Construire le chemin relatif (webkitRelativePath dispo sur chrome/edge)
  const items = files.map(f => {{
    const rel = f.webkitRelativePath && f.webkitRelativePath.length ? f.webkitRelativePath : f.name;
    return {{ file: f, rel: rel }};
  }});

  // Uploader avec file queue + k parallèles
  let done = 0;
  const queue = items.slice();
  const worker = async () => {{
    while (queue.length) {{
      const it = queue.shift();
      try {{
        const r = await uploadOne(it.file, it.rel);
        done++;
        log("OK " + it.rel + " → " + r.key + " ("+ r.sec +"s) [" + done + "/" + items.length + "]");
      }} catch (e) {{
        log("FAIL " + it.rel + " :: " + e.message);
      }}
    }}
  }};
  await Promise.all(Array.from({{length:k}}, worker));
  log("Terminé. Reviens dans l'app et clique '2) Traiter ce batch'.");
}});
</script>
</body></html>
""", height=420)
    st.info("Après l’upload navigateur → R2, passe à l’étape 2.")

# 2) Lister sous le préfixe et traiter (collages + Fidealis)
if st.button("2) Traiter ce batch") and "batch_prefix" in st.session_state:
    prefix = st.session_state["batch_prefix"]
    st.write(f"Préfixe: `{prefix}`")
    keys = list_objects(prefix)
    st.write(f"Trouvé {len(keys)} fichier(s) sur R2.")

    # Regrouper par dossier client (top-level après prefix)
    groups = group_keys_by_client(keys, prefix)
    if not groups:
        st.error("Aucun dossier client `ClientName - Address` détecté sous ce batch.")
        st.stop()

    st.write(f"👥 {len(groups)} client(s) détecté(s).")
    p_clients = st.progress(0.0)

    for idx, (client_folder, client_keys) in enumerate(groups.items(), start=1):
        parsed = split_client_address(client_folder)
        if not parsed:
            st.warning(f"Ignoré (nom invalide): {client_folder}")
            p_clients.progress(idx / len(groups))
            continue

        client_name, address = parsed
        lat, lng = get_coordinates(address)
        if lat is None or lng is None:
            lat, lng = ("N/A", "N/A")
            st.warning(f"⚠️ Géocodage indisponible pour: {client_folder}")

        st.info(f"👤 {client_name} — {len(client_keys)} fichier(s)")

        # Télécharger localement chaque image du client vers /tmp
        normalized_paths: List[str] = []
        p_imgs = st.progress(0.0)
        with tempfile.TemporaryDirectory(dir="/tmp") as tmpdir:
            for j, key in enumerate(client_keys, start=1):
                # skip dossiers "virtuels" (clé finissant par '/')
                if key.endswith("/"):
                    p_imgs.progress(j / len(client_keys))
                    continue
                # charge l'objet en mémoire
                buf = io.BytesIO()
                s3.download_fileobj(R2_BUCKET, key, buf)
                jb = preprocess_to_jpeg_bytes(buf.getvalue(), max_dim=max_dim, quality=jpeg_quality)
                outp = os.path.join(tmpdir, f"{client_name}_{j:05d}.jpg")
                with open(outp, "wb") as o:
                    o.write(jb)
                normalized_paths.append(outp)
                p_imgs.progress(j / len(client_keys))

            if not normalized_paths:
                st.error(f"Aucune image exploitable pour: {client_name}")
                p_clients.progress(idx / len(groups))
                continue

            # Collages par 3 + renommage du 1er
            collages = create_all_collages(normalized_paths, client_name, tmpdir, max_dim=max_dim, quality=jpeg_quality)
            if not collages:
                st.error(f"Échec collages: {client_name}")
                p_clients.progress(idx / len(groups))
                continue

            description = (
                f"SCELLÉ NUMERIQUE Bénéficiaire: Nom: {client_name}, "
                f"Adresse: {address}, Coordonnées GPS: Latitude {lat}, Longitude {lng}"
            )

            # Envoi Fidealis par 12
            api_upload_files(description, collages, session_id)

        st.success(f"✅ {client_name} — {len(collages)} collage(s) envoyé(s).")
        p_clients.progress(idx / len(groups))

    st.balloons()
    st.success("🎉 Batch terminé.")
