# app.py
import os, io, re, json, uuid, base64, tempfile
import requests, streamlit as st, boto3
from typing import List, Tuple, Optional, Dict
from PIL import Image, ImageOps, ImageFile
import botocore.client

ImageFile.LOAD_TRUNCATED_IMAGES = True

# ========= ENV (Fidealis) =========
API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")
ACCOUNT_KEY = os.getenv("ACCOUNT_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# ========= ENV (Cloudflare R2) =========
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")           # ex: f02b01cf197682b810e6a39fdbcbaad3
R2_BUCKET = os.getenv("R2_BUCKET")                   # ex: fidealis-demo
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_REGION = os.getenv("R2_REGION", "auto")

# Virtual-hosted style (navigateur) + endpoint SDK (serveur)
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

def api_upload_files(description: str, filepaths: List[str], session_id: str):
    for i in range(0, len(filepaths), 12):
        batch = filepaths[i:i+12]
        data = {
            "key": API_KEY, "PHPSESSID": session_id, "call": "setDeposit",
            "description": description, "type":"deposit","hidden":"0","sendmail":"1","background":"2"
        }
        for idx, fp in enumerate(batch, start=1):
            with open(fp, "rb") as f:
                data[f"file{idx}"] = base64.b64encode(f.read()).decode("utf-8")
            data[f"filename{idx}"] = os.path.basename(fp)
        requests.post(API_URL, data=data, timeout=60)

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

def create_all_collages(filepaths: List[str], client_name: str, workdir: str, max_dim=1600, q=80) -> List[str]:
    out = []
    for i in range(0, len(filepaths), 3):
        group = filepaths[i:i+3]
        imgs=[]
        for fp in group:
            with open(fp,"rb") as f: jb = preprocess_to_jpeg_bytes(f.read(), max_dim=max_dim, quality=q)
            imgs.append(Image.open(io.BytesIO(jb)))
        p = os.path.join(workdir, f"c_{client_name}_{len(out)+1}.jpg")
        create_collage(imgs, p, quality=q)
        out.append(p)
    if out:
        renamed = os.path.join(workdir, f"{client_name}_1.jpg")
        os.replace(out[0], renamed); out[0] = renamed
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
    """
    Ex: "Racine/Client A - 18 rue X/img.jpg" -> "Client A - 18 rue X"
        "Client B - 2 av Y/img.jpg"         -> "Client B - 2 av Y"
    """
    parts = path_rel.strip("/").split("/")
    for seg in parts:
        if CLIENT_RE.match(seg or ""):
            return seg
    return None

def group_by_client(keys: List[str], batch_prefix: str) -> Dict[str,List[str]]:
    groups: Dict[str,List[str]] = {}
    samples = []
    for k in keys:
        if not k.startswith(batch_prefix) or k.endswith("/"):
            continue
        rel = k[len(batch_prefix):]            # tout ce qui vient après uploads/<batch>/
        if not samples and rel:
            samples.append(rel)
        client_seg = find_client_segment(rel)
        if client_seg:
            groups.setdefault(client_seg, []).append(k)
    for v in groups.values():
        v.sort()
    # aide au debug si vide
    if not groups and samples:
        st.warning(f"Exemple de chemin relatif (pour debug) : {samples[0]}")
    return groups

# ---------- Diagnostic R2 ----------
def r2_health():
    st.subheader("🔍 Diagnostic R2")
    st.write("Bucket host (browser):", R2_BUCKET_HOST)
    st.write("Endpoint (SDK):", R2_ENDPOINT)
    try:
        s3.head_bucket(Bucket=R2_BUCKET)
        st.success("head_bucket ✅")
    except Exception as e:
        st.error(f"head_bucket ❌ : {e}")

    # Test PUT/GET serveur
    try:
        test_key = f"diagnostics/{uuid.uuid4()}-ping.txt"
        s3.put_object(Bucket=R2_BUCKET, Key=test_key, Body=b"ping")
        obj = s3.get_object(Bucket=R2_BUCKET, Key=test_key)
        body = obj["Body"].read()
        st.success(f"PUT/GET serveur ✅ ({body!r})")
        s3.delete_object(Bucket=R2_BUCKET, Key=test_key)
    except Exception as e:
        st.error(f"PUT/GET serveur ❌ : {e}")

# ========== UI ==========
st.set_page_config(page_title="FIDEALIS — Dossier → R2 → Traitement auto", layout="centered")
st.title("FIDEALIS — Dossier → R2 (auto) → Collages → Dépôt")

session_id = api_login()
if not session_id:
    st.error("Connexion Fidealis échouée (API_URL/API_KEY/ACCOUNT_KEY).")
    st.stop()

credits = get_credit(session_id)
if isinstance(credits, dict):
    st.caption(f"Crédit restant (Produit 4) : {get_quantity_for_product_4(credits)}")

with st.expander("🧪 Diagnostic R2", expanded=False):
    r2_health()

with st.expander("Options de traitement"):
    max_dim = st.slider("Dimension max (px) avant collage", 800, 4000, 1600, step=100)
    jpeg_q  = st.slider("Qualité JPEG", 50, 95, 80, step=1)

# --- Query params ---
try:
    params = st.query_params
except Exception:
    params = st.experimental_get_query_params()

# --- Mode post-upload (?batch=...) ---
if "batch" in params:
    batch_id = params["batch"] if isinstance(params["batch"], str) else params["batch"][0]
    batch_prefix = f"uploads/{batch_id}/"
    st.info(f"Traitement en cours pour le batch : {batch_id}")
    keys = list_objects(batch_prefix)
    st.write(f"{len(keys)} fichier(s) détecté(s) dans R2 sous {batch_prefix}")
    groups = group_by_client(keys, batch_prefix)
    if not groups:
        st.error("Aucun dossier client `ClientName - Address` détecté sous ce batch.")
        st.stop()

    st.write(f"👥 {len(groups)} client(s) détecté(s).")
    p_clients = st.progress(0.0)

    for idx, (client_folder, client_keys) in enumerate(groups.items(), start=1):
        parsed = split_client(client_folder)
        if not parsed:
            p_clients.progress(idx/len(groups)); continue
        client_name, address = parsed
        lat, lng = get_coordinates(address)
        if lat is None or lng is None:
            lat, lng = ("N/A","N/A")
            st.warning(f"Géocodage indisponible : {client_folder}")

        st.info(f"👤 {client_name} — {len(client_keys)} fichier(s)")
        with tempfile.TemporaryDirectory(dir="/tmp") as tmpdir:
            normalized: List[str] = []
            p_imgs = st.progress(0.0)
            for j, key in enumerate(client_keys, start=1):
                buf = io.BytesIO()
                s3.download_fileobj(R2_BUCKET, key, buf)
                jb = preprocess_to_jpeg_bytes(buf.getvalue(), max_dim=max_dim, quality=jpeg_q)
                outp = os.path.join(tmpdir, f"{client_name}_{j:05d}.jpg")
                with open(outp,"wb") as o: o.write(jb)
                normalized.append(outp)
                p_imgs.progress(j/len(client_keys))

            collages = create_all_collages(normalized, client_name, tmpdir, max_dim=max_dim, q=jpeg_q)
            description = (f"SCELLÉ NUMERIQUE Bénéficiaire: Nom: {client_name}, "
                           f"Adresse: {address}, Coordonnées GPS: Latitude {lat}, Longitude {lng}")
            api_upload_files(description, collages, session_id)
        st.success(f"✅ {client_name} — {len(collages)} collage(s) envoyé(s).")
        p_clients.progress(idx/len(groups))
    st.balloons(); st.success("🎉 Batch terminé.")
    st.stop()

# --- Ecran initial : upload -> R2 (PUT signé) ---
st.markdown("### 1 clic : choisir le dossier et **Soumettre**")
if st.button("Sélectionner un dossier et Soumettre", type="primary"):
    if not all([R2_ACCOUNT_ID, R2_BUCKET, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]):
        st.error("R2 env manquantes (ACCOUNT_ID/BUCKET/ACCESS_KEY/SECRET).")
        st.stop()

    batch_id = str(uuid.uuid4())
    prefix = f"uploads/{batch_id}/"

    st.components.v1.html(f"""
<!doctype html><html>
<body>
<input id="picker" type="file" webkitdirectory directory multiple style="display:none" />
<button id="go" style="padding:10px 16px;font-size:16px;">Choisir le dossier…</button>
<pre id="log" style="white-space:pre-wrap;border:1px solid #eee;padding:8px;border-radius:6px;max-height:280px;overflow:auto;margin-top:10px;"></pre>

<script type="module">
import {{ AwsClient }} from "https://esm.sh/aws4fetch@1.0.17";

const ACCESS_KEY_ID = {json.dumps(R2_ACCESS_KEY_ID)};
const SECRET_ACCESS_KEY = {json.dumps(R2_SECRET_ACCESS_KEY)};
const ACCOUNT_ID = {json.dumps(R2_ACCOUNT_ID)};
const BUCKET = {json.dumps(R2_BUCKET)};
const BUCKET_HOST = `${{BUCKET}}.${{ACCOUNT_ID}}.r2.cloudflarestorage.com`;
const PREFIX = {json.dumps(prefix)};
const BATCH_ID = {json.dumps(batch_id)};

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
  return rel.replace(/^\\.\\//,'').replaceAll('\\\\','/'); // Windows → /
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
  log(`Fichiers détectés: ${{items.length}} — upload en parallèle (PUT signé)…`);

  const K = 8; // parallélisme
  const queue = items.slice();
  let ok=0, ko=0;

  async function uploadOne(it) {{
    const url = `https://${{BUCKET_HOST}}/${{keyFor(it.rel)}}`;
    const t0 = performance.now();
    const res = await client.fetch(url, {{
      method: "PUT",
      body: it.file,
      headers: {{ "Content-Type": it.file.type || "application/octet-stream" }}
    }});
    const dt = ((performance.now()-t0)/1000).toFixed(2);
    if (!res.ok) throw new Error(`HTTP ${{res.status}}`);
    ok++; if (ok % 10 === 0) log(`... ${{ok}}/${{items.length}} OK`);
  }}

  async function worker() {{
    while (queue.length) {{
      const it = queue.shift();
      try {{ await uploadOne(it); }}
      catch(e) {{ ko++; log("FAIL "+it.rel+" :: "+(e && e.message?e.message:e)); }}
    }}
  }}

  await Promise.all(Array.from({{length:K}}, worker));
  log(`Terminé — OK=${{ok}}, FAIL=${{ko}}`);

  // ⚠️ IMPORTANT : redirige la PAGE PARENTE (pas l'iframe)
  const target = window.top.location.origin + window.top.location.pathname + "?batch=" + BATCH_ID;
  window.top.location.href = target;
}});
</script>
</body></html>
""", height=360)
    st.stop()
