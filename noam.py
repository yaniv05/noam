# app.py
import os, io, re, json, uuid, base64, time, tempfile
import requests, streamlit as st, boto3
from typing import List, Tuple, Optional, Dict, Set
from PIL import Image, ImageOps, ImageFile
import botocore.client

ImageFile.LOAD_TRUNCATED_IMAGES = True

# ========= ENV (Fidealis) =========
API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")
ACCOUNT_KEY = os.getenv("ACCOUNT_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# ========= ENV (Cloudflare R2) =========
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_BUCKET = os.getenv("R2_BUCKET")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_REGION = os.getenv("R2_REGION", "auto")

# Navigateur (virtual-hosted) & SDK endpoint
R2_BUCKET_HOST = f"{R2_BUCKET}.{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
R2_ENDPOINT = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

# ========= boto3 client =========
s3 = boto3.client(
    "s3",
    region_name=R2_REGION,
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    config=botocore.client.Config(s3={"addressing_style": "virtual"})
)

IMG_EXTS = {".jpg",".jpeg",".png",".JPG",".JPEG",".PNG"}
CLIENT_RE = re.compile(r"^\s*(.+?)\s*-\s*(.+?)\s*$")

# ---------- Fidealis ----------
def api_login() -> Optional[str]:
    try:
        r = requests.get(API_URL, params={"key":API_KEY,"call":"loginUserFromAccountKey","accountKey":ACCOUNT_KEY}, timeout=30)
        r.raise_for_status()
        return r.json().get("PHPSESSID")
    except Exception:
        return None

def api_upload_files(description: str, filepaths: List[str], session_id: str, log_write):
    total = len(filepaths)
    if total == 0:
        return
    log_write(f"Envoi Fidealis: {total} fichier(s), lots de 12.")
    for start in range(0, total, 12):
        batch = filepaths[start:start+12]
        data = {
            "key": API_KEY, "PHPSESSID": session_id, "call": "setDeposit",
            "description": description, "type":"deposit","hidden":"0","sendmail":"1","background":"2"
        }
        for idx, fp in enumerate(batch, start=1):
            with open(fp, "rb") as f:
                data[f"file{idx}"] = base64.b64encode(f.read()).decode("utf-8")
            data[f"filename{idx}"] = os.path.basename(fp)
        try:
            r = requests.post(API_URL, data=data, timeout=120)
            try:
                js = r.json()
                log_write(f"  Lot {start+1}-{start+len(batch)}: HTTP {r.status_code} — retour: {js}")
            except Exception:
                log_write(f"  Lot {start+1}-{start+len(batch)}: HTTP {r.status_code}")
        except Exception as e:
            log_write(f"  Lot {start+1}-{start+len(batch)}: erreur requête: {e}")

def get_credit(session_id: str):
    try:
        r = requests.get(API_URL, params={"key":API_KEY,"PHPSESSID":session_id,"call":"getCredits","product_ID":""}, timeout=30)
        if r.status_code==200: return r.json()
    except Exception: pass
    return None

def get_quantity_for_product_4(credit_data):
    try: return credit_data["4"]["quantity"]
    except Exception: return "N/A"

# ---------- Geocoding ----------
def get_coordinates(address: str) -> Tuple[Optional[float], Optional[float]]:
    if not GOOGLE_API_KEY: return None, None
    try:
        r = requests.get("https://maps.googleapis.com/maps/api/geocode/json",
                         params={"address":address,"key":GOOGLE_API_KEY}, timeout=30)
        r.raise_for_status()
        data = r.json()
        if data.get("status")=="OK" and data.get("results"):
            loc = data["results"][0]["geometry"]["location"]
            return loc["lat"], loc["lng"]
    except Exception: pass
    return None, None

# ---------- Images ----------
def preprocess_to_jpeg_bytes(raw: bytes, max_dim=1600, quality=80) -> bytes:
    with Image.open(io.BytesIO(raw)) as img:
        img = ImageOps.exif_transpose(img)
        if img.mode not in ("RGB","L"): img = img.convert("RGB")
        w,h = img.size
        s = min(1.0, max_dim/max(w,h))
        if s < 1.0:
            img = img.resize((int(w*s), int(h*s)), Image.LANCZOS)
        out = io.BytesIO()
        img.save(out, "JPEG", quality=quality, optimize=True)
        return out.getvalue()

def create_collage(pil_images: List[Image.Image], out_path: str, quality=80):
    min_h = min(i.size[1] for i in pil_images)
    resized = [ImageOps.fit(i, (int(i.size[0]*min_h/i.size[1]), min_h)) for i in pil_images]
    total_w = sum(i.size[0] for i in resized) + (len(resized)-1)*20 + 50
    canvas = Image.new("RGB", (total_w, min_h+50), (255,255,255))
    x = 25
    for i in resized:
        canvas.paste(i, (x,25)); x += i.size[0] + 20
    canvas.save(out_path, "JPEG", quality=quality, optimize=True)
    canvas.close()
    for i in pil_images: i.close()

def create_collages_from_paths(img_paths: List[str], client_name: str, workdir: str, q=80) -> List[str]:
    out = []
    for i in range(0, len(img_paths), 3):
        group = img_paths[i:i+3]
        imgs = [Image.open(p) for p in group]
        p = os.path.join(workdir, f"c_{client_name}_{len(out)+1}.jpg")
        create_collage(imgs, p, quality=q)
        out.append(p)
    if out:
        renamed = os.path.join(workdir, f"{client_name}_1.jpg")
        os.replace(out[0], renamed)
        out[0] = renamed
    return out

# ---------- R2 (serveur) ----------
def list_objects(prefix: str) -> List[str]:
    keys, token = [], None
    while True:
        kw={"Bucket":R2_BUCKET,"Prefix":prefix,"MaxKeys":1000}
        if token: kw["ContinuationToken"]=token
        resp = s3.list_objects_v2(**kw)
        for obj in resp.get("Contents", []):
            keys.append(obj["Key"])
        if resp.get("IsTruncated"): token = resp.get("NextContinuationToken")
        else: break
    return keys

def split_client(folder: str) -> Optional[Tuple[str,str]]:
    m = CLIENT_RE.match(folder)
    return (m.group(1).strip(), m.group(2).strip()) if m else None

def find_client_segment(path_rel: str) -> Optional[str]:
    parts = path_rel.strip("/").split("/")
    for seg in parts:
        if CLIENT_RE.match(seg or ""):
            return seg
    return None

def group_keys_by_client(keys: List[str], batch_prefix: str) -> Dict[str, List[str]]:
    groups: Dict[str,List[str]] = {}
    for k in keys:
        if not k.startswith(batch_prefix) or k.endswith("/"):
            continue
        rel = k[len(batch_prefix):]
        client_seg = find_client_segment(rel)
        if client_seg:
            groups.setdefault(client_seg, []).append(k)
    for v in groups.values():
        v.sort()
    return groups

# ---------- Diagnostic R2 ----------
def r2_health():
    st.subheader("Diagnostic R2")
    st.write("Bucket host (navigateur):", R2_BUCKET_HOST)
    st.write("Endpoint (SDK):", R2_ENDPOINT)
    try:
        s3.head_bucket(Bucket=R2_BUCKET)
        st.success("head_bucket OK")
    except Exception as e:
        st.error(f"head_bucket erreur: {e}")
    try:
        test_key = f"diagnostics/{uuid.uuid4()}-ping.txt"
        s3.put_object(Bucket=R2_BUCKET, Key=test_key, Body=b"ping")
        obj = s3.get_object(Bucket=R2_BUCKET, Key=test_key)
        body = obj["Body"].read()
        st.success(f"PUT/GET serveur OK ({body!r})")
        s3.delete_object(Bucket=R2_BUCKET, Key=test_key)
    except Exception as e:
        st.error(f"PUT/GET serveur erreur: {e}")

# ---------- Traitement en flux (live) ----------
class LiveState:
    def __init__(self):
        self.seen_by_client: Dict[str, Set[str]] = {}    # clés déjà traitées (téléchargées + normalisées)
        self.buffer_by_client: Dict[str, List[str]] = {} # chemins locaux normalisés en attente de collage/envoi

def process_ready_chunks(client_name: str, address: str, latlng: Tuple[Optional[float],Optional[float]],
                         buffer_paths: List[str], jpeg_q: int, session_id: str, log_write):
    # Prend par paquets de 36 images -> collages -> envoi
    count = len(buffer_paths)
    n_chunks = count // 36
    if n_chunks == 0:
        return 0
    used = 0
    with tempfile.TemporaryDirectory(dir="/tmp") as tmpdir2:
        for c in range(n_chunks):
            block = buffer_paths[c*36:(c+1)*36]
            collages = create_collages_from_paths(block, client_name, tmpdir2, q=jpeg_q)
            lat, lng = latlng
            description = (f"SCELLÉ NUMERIQUE Bénéficiaire: Nom: {client_name}, "
                           f"Adresse: {address}, Coordonnées GPS: Latitude {lat}, Longitude {lng}")
            api_upload_files(description, collages, session_id, log_write)
            used += len(block)
    return used

def live_watch_and_process(batch_id: str, max_dim: int, jpeg_q: int, session_id: str,
                           inactivity_stop_s: int, poll_every_s: float, log_write):
    batch_prefix = f"uploads/{batch_id}/"
    log_write(f"Traitement en direct pour batch: {batch_id} (prefix {batch_prefix})")
    state = LiveState()
    last_progress_ts = time.time()

    while True:
        keys = list_objects(batch_prefix)
        groups = group_keys_by_client(keys, batch_prefix)
        updated_something = False

        for client_folder, client_keys in groups.items():
            parsed = split_client(client_folder)
            if not parsed:
                continue
            client_name, address = parsed
            if client_folder not in state.seen_by_client:
                state.seen_by_client[client_folder] = set()
                state.buffer_by_client[client_folder] = []
                log_write(f"Nouveau client détecté: {client_name} — {address}")

            seen = state.seen_by_client[client_folder]
            buf  = state.buffer_by_client[client_folder]

            # Prétraiter uniquement les nouvelles clés
            new_keys = [k for k in client_keys if k not in seen]
            if new_keys:
                # Récupérer lat/lng à la première occasion
                lat, lng = get_coordinates(address)
                if lat is None or lng is None:
                    lat, lng = ("N/A", "N/A")
                # Télécharger et normaliser
                with tempfile.TemporaryDirectory(dir="/tmp") as tmpdir:
                    for j, key in enumerate(new_keys, start=1):
                        try:
                            b = io.BytesIO()
                            s3.download_fileobj(R2_BUCKET, key, b)
                            jb = preprocess_to_jpeg_bytes(b.getvalue(), max_dim=max_dim, quality=jpeg_q)
                            outp = os.path.join(tmpdir, f"{uuid.uuid4().hex}.jpg")
                            with open(outp, "wb") as o: o.write(jb)
                            # On garde une copie locale persistante pour le batch courant
                            # pour limiter l'usage disque, on recopie vers /tmp global client
                            # Ici on garde tel quel dans mem: on ajoute chemin temp courant
                            # Note: chaque itération de tmpdir va être détruite:
                            # -> on recopie dans un fichier durable:
                            durable = os.path.join("/tmp", f"{uuid.uuid4().hex}.jpg")
                            with open(outp, "rb") as i, open(durable, "wb") as o2:
                                o2.write(i.read())
                            buf.append(durable)
                            seen.add(key)
                        except Exception as e:
                            log_write(f"Erreur normalisation {key}: {e}")
                # Dès qu'on a des multiples de 36, on envoie
                used = process_ready_chunks(client_name, address, (lat,lng), buf, jpeg_q, session_id, log_write)
                if used > 0:
                    del buf[:used]
                    updated_something = True

        now = time.time()
        if updated_something:
            last_progress_ts = now
        # Arrêt si inactif trop longtemps
        if now - last_progress_ts > inactivity_stop_s:
            log_write(f"Aucune nouvelle image depuis {inactivity_stop_s}s. Arrêt du mode en direct.")
            break

        time.sleep(poll_every_s)

    # A la fin, on ne force pas la vidange ici: l’utilisateur clique "Finaliser"
    log_write("Mode en direct terminé.")

def finalize_leftovers(batch_id: str, jpeg_q: int, session_id: str, log_write):
    # On relit tout puis on envoie le reliquat par client (< 36 aussi)
    batch_prefix = f"uploads/{batch_id}/"
    keys = list_objects(batch_prefix)
    groups = group_keys_by_client(keys, batch_prefix)
    if not groups:
        log_write("Aucun client trouvé lors de la finalisation.")
        return
    for client_folder, client_keys in groups.items():
        parsed = split_client(client_folder)
        if not parsed:
            continue
        client_name, address = parsed
        lat, lng = get_coordinates(address)
        if lat is None or lng is None:
            lat, lng = ("N/A","N/A")

        log_write(f"Finalisation client: {client_name}")
        # Télécharger/normaliser à nouveau pour simplicité (petit coût, mais fiable)
        normalized: List[str] = []
        with tempfile.TemporaryDirectory(dir="/tmp") as tmpdir:
            for key in client_keys:
                try:
                    b = io.BytesIO()
                    s3.download_fileobj(R2_BUCKET, key, b)
                    jb = preprocess_to_jpeg_bytes(b.getvalue(), max_dim=1600, quality=jpeg_q)
                    outp = os.path.join(tmpdir, f"{uuid.uuid4().hex}.jpg")
                    with open(outp, "wb") as o: o.write(jb)
                    normalized.append(outp)
                except Exception as e:
                    log_write(f"Erreur normalisation {key}: {e}")

            if not normalized:
                log_write("Aucune image normalisée.")
                continue

            # Collages par 3, puis envoyer en lots de 12 fichiers
            collages = create_collages_from_paths(normalized, client_name, tmpdir, q=jpeg_q)
            description = (f"SCELLÉ NUMERIQUE Bénéficiaire: Nom: {client_name}, "
                           f"Adresse: {address}, Coordonnées GPS: Latitude {lat}, Longitude {lng}")
            api_upload_files(description, collages, session_id, log_write)
        log_write(f"Finalisation terminée pour {client_name}.")

# ========== UI ==========
st.set_page_config(page_title="FIDEALIS — Dossier → R2 → Traitement auto", layout="centered")
st.title("FIDEALIS — Dossier → R2 → Collages → Dépôt")

session_id = api_login()
if not session_id:
    st.error("Connexion Fidealis échouée (API_URL/API_KEY/ACCOUNT_KEY).")
    st.stop()

credits = get_credit(session_id)
if isinstance(credits, dict):
    st.caption(f"Crédit restant (Produit 4) : {get_quantity_for_product_4(credits)}")

with st.expander("Diagnostic R2", expanded=False):
    r2_health()

with st.expander("Options de traitement"):
    max_dim = st.slider("Dimension max (px) avant collage", 800, 4000, 1600, step=100)
    jpeg_q  = st.slider("Qualité JPEG", 50, 95, 80, step=1)
    inactivity_stop_s = st.slider("Arrêt auto du live s'il n'y a plus de nouvelles images (secondes)", 10, 300, 45, step=5)
    poll_every_s = st.slider("Intervalle de polling R2 (secondes)", 0.5, 5.0, 2.0, step=0.5)

# Zone de logs
log_box = st.empty()
def log_write(msg: str):
    old = st.session_state.get("log_text", "")
    new = old + (msg.strip() + "\n")
    st.session_state["log_text"] = new
    log_box.text(new if len(new) < 20000 else new[-20000:])  # limite d'affichage

# --- Query params: support ?batch=... (optionnel) ---
try:
    params = st.query_params
except Exception:
    params = st.experimental_get_query_params()

if "batch" in params:
    st.session_state["last_batch_id"] = params["batch"] if isinstance(params["batch"], str) else params["batch"][0]
    st.info(f"Batch détecté dans l'URL: {st.session_state['last_batch_id']}")

# --- Ecran initial: upload -> R2 (PUT signé) ---
st.markdown("Étape 1. Uploader un dossier complet vers R2")

if st.button("Choisir le dossier et uploader vers R2", type="primary"):
    if not all([R2_ACCOUNT_ID, R2_BUCKET, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]):
        st.error("R2: variables d'environnement manquantes (ACCOUNT_ID/BUCKET/ACCESS_KEY/SECRET).")
        st.stop()

    batch_id = str(uuid.uuid4())
    st.session_state["last_batch_id"] = batch_id
    prefix = f"uploads/{batch_id}/"

    st.components.v1.html(f"""
<!doctype html><html>
<body>
<input id="picker" type="file" webkitdirectory directory multiple style="display:none" />
<button id="go" style="padding:10px 16px;">Choisir le dossier…</button>
<pre id="log" style="white-space:pre-wrap;border:1px solid #eee;padding:8px;border-radius:6px;max-height:280px;overflow:auto;margin-top:10px;"></pre>

<script type="module">
import {{ AwsClient }} from "https://esm.sh/aws4fetch@1.0.17";

const ACCESS_KEY_ID = {json.dumps(R2_ACCESS_KEY_ID)};
const SECRET_ACCESS_KEY = {json.dumps(R2_SECRET_ACCESS_KEY)};
const ACCOUNT_ID = {json.dumps(R2_ACCOUNT_ID)};
const BUCKET = {json.dumps(R2_BUCKET)};
const BUCKET_HOST = `${{BUCKET}}.${{ACCOUNT_ID}}.r2.cloudflarestorage.com`;
const PREFIX = {json.dumps(prefix)};

const client = new AwsClient({{
  accessKeyId: ACCESS_KEY_ID,
  secretAccessKey: SECRET_ACCESS_KEY,
  service: "s3",
  region: "auto"
}});

const log = (m)=>document.getElementById('log').textContent += m + "\\n";
const pick = document.getElementById('picker');
document.getElementById('go').addEventListener('click', ()=> pick.click());

function normRelPath(rel) {{
  return rel.replace(/^\\.\\//,'').replaceAll('\\\\','/');
}}
function keyFor(rel) {{
  return PREFIX + normRelPath(rel);
}}

pick.addEventListener('change', async () => {{
  const files = Array.from(pick.files||[]);
  if (!files.length) {{ log("Aucun fichier sélectionné."); return; }}

  const allowed = new Set([".jpg",".jpeg",".png",".JPG",".JPEG",".PNG"]);
  const items = files
    .map(f=>{{ const rel=f.webkitRelativePath||f.name;
               const ext=rel.slice(rel.lastIndexOf('.'));
               return {{file:f, rel, ext}}; }})
    .filter(x=> allowed.has(x.ext));

  if (!items.length) {{ log("Aucune image détectée dans le dossier."); return; }}
  log(`Fichiers détectés: ${{items.length}} — upload en parallèle (PUT signé).`);

  const K = 8;
  const queue = items.slice();
  let ok=0, ko=0, tStart=performance.now();

  async function uploadOne(it) {{
    const url = `https://${{BUCKET_HOST}}/${{keyFor(it.rel)}}`;
    try {{
      const res = await client.fetch(url, {{
        method: "PUT",
        body: it.file,
        headers: {{ "Content-Type": it.file.type || "application/octet-stream" }}
      }});
      if (!res.ok) throw new Error(`HTTP ${{res.status}}`);
      ok++;
      if ((ok+ko) % 10 === 0) {{
        const pct = Math.round(((ok+ko)/items.length)*100);
        log(`Progression: ${{ok+ko}}/${{items.length}} (${pct}%)`);
      }}
    }} catch (e) {{
      ko++;
      log("Echec: " + it.rel + " :: " + (e && e.message ? e.message : e));
    }}
  }}

  async function worker() {{
    while (queue.length) {{
      const it = queue.shift();
      await uploadOne(it);
    }}
  }}

  await Promise.all(Array.from({{length:K}}, worker));
  const dt = ((performance.now()-tStart)/1000).toFixed(1);
  log(`Terminé. OK=${{ok}}, FAIL=${{ko}}. Durée: ${{dt}}s`);
  log("Reviens dans l'application et lance le traitement en direct ou la finalisation.");
}});
</script>
</body></html>
""", height=360)

# Étape 2: traitement live (parallèle à l'upload)
st.markdown("Étape 2. Traitement en direct pendant l'upload (tranches de 36 par client)")

col1, col2 = st.columns(2)
with col1:
    if st.button("Démarrer le traitement en direct"):
        batch_id = st.session_state.get("last_batch_id")
        if not batch_id:
            st.error("Aucun batch en mémoire. Lance d'abord l'upload.")
        else:
            log_write(f"Début du traitement en direct pour batch {batch_id}")
            live_watch_and_process(batch_id, max_dim, jpeg_q, session_id,
                                   inactivity_stop_s=inactivity_stop_s,
                                   poll_every_s=poll_every_s,
                                   log_write=log_write)

with col2:
    if st.button("Finaliser (envoyer le reliquat)"):
        batch_id = st.session_state.get("last_batch_id")
        if not batch_id:
            st.error("Aucun batch en mémoire. Lance d'abord l'upload.")
        else:
            log_write(f"Finalisation du batch {batch_id}")
            finalize_leftovers(batch_id, jpeg_q, session_id, log_write)
            log_write("Finalisation terminée.")
