# app.py
import os, io, re, json, uuid, base64, tempfile
import requests, streamlit as st, boto3
from typing import List, Tuple, Optional, Dict
from PIL import Image, ImageOps, ImageFile

# --- Tolérer certains JPEG tronqués ---
ImageFile.LOAD_TRUNCATED_IMAGES = True

# ===== ENV (Fidealis) =====
API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")
ACCOUNT_KEY = os.getenv("ACCOUNT_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# ===== ENV (Cloudflare R2) =====
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.getenv("R2_BUCKET")
R2_REGION = os.getenv("R2_REGION", "auto")
R2_ENDPOINT = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

# ===== S3 client (R2) =====
s3 = boto3.client(
    "s3",
    region_name=R2_REGION,
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
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

# ---------- R2 util ----------
def r2_presign_post_for_prefix(prefix: str, max_mb=2048, expires=3600):
    """
    Pre-signed POST réutilisable pour TOUTE clé commençant par `prefix`.
    On n'impose pas 'key' ni 'Content-Type' dans Fields → on les ajoute côté navigateur.
    """
    return s3.generate_presigned_post(
        Bucket=R2_BUCKET,
        Key=prefix + "${filename}",   # placeholder
        Fields={},                    # pas de 'key' / 'Content-Type' figés
        Conditions=[
            ["starts-with","$key", prefix],
            ["starts-with","$Content-Type",""],
            ["content-length-range", 1, max_mb*1024*1024],
        ],
        ExpiresIn=expires,
    )

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

def group_by_client(keys: List[str], batch_prefix: str) -> Dict[str,List[str]]:
    groups: Dict[str,List[str]] = {}
    for k in keys:
        if not k.startswith(batch_prefix) or k.endswith("/"): continue
        rel = k[len(batch_prefix):]
        top = rel.split("/",1)[0]
        if split_client(top):
            groups.setdefault(top, []).append(k)
    for v in groups.values(): v.sort()
    return groups

# ---------- DIAGNOSTIC R2 ----------
def r2_health(server_prefix="diagnostics/"):
    st.subheader("🔍 Diagnostic R2")
    st.write("Endpoint:", f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com")
    st.write("Bucket:", R2_BUCKET)
    st.write("Region:", R2_REGION or "auto")
    st.write("Access Key ID (fin):", (R2_ACCESS_KEY_ID or "")[-4:] if R2_ACCESS_KEY_ID else "ABSENT")
    st.write("Secret (présent ?):", "OK" if R2_SECRET_ACCESS_KEY else "ABSENT")

    try:
        s3.head_bucket(Bucket=R2_BUCKET)
        st.success("head_bucket ✅")
    except Exception as e:
        st.error(f"head_bucket ❌ : {e}")

    test_key = f"{server_prefix}{uuid.uuid4()}-ping.txt"
    try:
        s3.put_object(Bucket=R2_BUCKET, Key=test_key, Body=b"ping")
        st.success("put_object ✅")
        obj = s3.get_object(Bucket=R2_BUCKET, Key=test_key)
        body = obj["Body"].read()
        st.success(f"get_object ✅ (contenu={body!r})")
        s3.delete_object(Bucket=R2_BUCKET, Key=test_key)
        st.success("delete_object ✅")
    except Exception as e:
        st.error(f"put/get/delete ❌ : {e}")

    try:
        test_prefix = f"{server_prefix}{uuid.uuid4()}/"
        pres = s3.generate_presigned_post(
            Bucket=R2_BUCKET,
            Key=test_prefix + "${filename}",
            Fields={},
            Conditions=[
                ["starts-with","$key", test_prefix],
                ["starts-with","$Content-Type",""],
                ["content-length-range", 1, 1024 * 1024],
            ],
            ExpiresIn=600,
        )
        st.info(f"Préfixe de test: `{test_prefix}`")
        st.components.v1.html(f"""
<html><body>
<button id="btn" style="padding:8px 12px;">Tester upload navigateur → R2</button>
<pre id="out" style="white-space:pre-wrap;border:1px solid #eee;padding:8px;border-radius:6px;max-height:220px;overflow:auto;margin-top:8px;"></pre>
<script>
const pres = {json.dumps(pres)};
const prefix = {json.dumps(test_prefix)};
const log = m => document.getElementById('out').textContent += m + "\\n";
async function testUpload() {{
  const filename = "ping.txt";
  const key = prefix + filename;
  const blob = new Blob(["hello-r2"], {{type: "text/plain"}});
  const form = new FormData();
  for (const [k,v] of Object.entries(pres.fields)) {{
    if (k !== 'key' && k !== 'Content-Type') form.append(k, v);
  }}
  form.append('key', key);
  form.append('file', blob);
  try {{
    const t0 = performance.now();
    const res = await fetch(pres.url, {{ method: 'POST', body: form }});
    const dt = ((performance.now()-t0)/1000).toFixed(2);
    let text = "";
    try {{ text = await res.text(); }} catch (e) {{ text = "(body non lisible, CORS)" }}
    if (res.ok) log("POST ✅ 200 OK en " + dt + "s");
    else {{ log("POST ❌ HTTP " + res.status + " en " + dt + "s"); log("Body: " + text); }}
  }} catch (e) {{
    log("POST ❌ Failed to fetch: " + (e && e.message ? e.message : e));
    log("=> Probable CORS / mauvaise URL / connectivité.");
  }}
}}
document.getElementById('btn').addEventListener('click', testUpload);
</script>
</body></html>
""", height=320)
    except Exception as e:
        st.error(f"Préparation pre-signed POST ❌ : {e}")

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

with st.expander("🧪 Diagnostic R2 (si 'Failed to fetch')", expanded=False):
    r2_health()

with st.expander("Options de traitement"):
    max_dim = st.slider("Dimension max (px) avant collage", 800, 4000, 1600, step=100)
    jpeg_q  = st.slider("Qualité JPEG", 50, 95, 80, step=1)

# --- Lecture des query params (compat large) ---
try:
    params = st.query_params  # Streamlit >= 1.30
except Exception:
    params = st.experimental_get_query_params()

# --- Mode post-upload (?batch=...) ---
if "batch" in params:
    batch_id = params["batch"] if isinstance(params["batch"], str) else params["batch"][0]
    batch_prefix = f"uploads/{batch_id}/"
    st.info(f"Traitement en cours pour le batch : {batch_id}")
    keys = list_objects(batch_prefix)
    st.write(f"{len(keys)} fichiers détectés dans R2.")
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

# --- Écran initial : 1 bouton ---
st.markdown("### 1 clic : choisir le dossier et **Soumettre**")
if st.button("Sélectionner un dossier et Soumettre", type="primary"):
    batch_id = str(uuid.uuid4())
    prefix = f"uploads/{batch_id}/"
    post = r2_presign_post_for_prefix(prefix, max_mb=2048, expires=3600)

    st.components.v1.html(f"""
<!doctype html><html><body>
<input id="picker" type="file" webkitdirectory directory multiple style="display:none" />
<button id="go" style="padding:10px 16px;font-size:16px;">Choisir le dossier…</button>
<pre id="log" style="white-space:pre-wrap;border:1px solid #eee;padding:8px;border-radius:6px;max-height:240px;overflow:auto;margin-top:10px;"></pre>
<script>
const pres = {json.dumps(post)};
const prefix = {json.dumps(prefix)};
const batchId = {json.dumps(batch_id)};
const log = (m)=>document.getElementById('log').textContent += m + "\\n";
const pick = document.getElementById('picker');

document.getElementById('go').addEventListener('click', ()=> pick.click());

function keyFor(rel) {{
  return (prefix + rel.replace(/^\\.\\//,'').replaceAll('\\\\','/'));
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
  log(`Fichiers détectés: ${{items.length}} — upload en parallèle…`);

  const K = 8;             // parallélisme
  const queue = items.slice();
  let done=0;

  async function uploadOne(it) {{
    const form = new FormData();
    for (const [k,v] of Object.entries(pres.fields)) {{
      if (k !== 'key' && k !== 'Content-Type') form.append(k, v);
    }}
    form.append('key', keyFor(it.rel));              // clé dynamique (avec sous-dossiers)
    form.append('file', it.file);                    // ne pas ajouter Content-Type → policy l'autorise
    const t0 = performance.now();
    const res = await fetch(pres.url, {{ method:'POST', body: form }});
    const dt = ((performance.now()-t0)/1000).toFixed(2);
    if (!res.ok) throw new Error(`HTTP ${{res.status}}`);
    if (++done % 10 === 0) log(`... ${{done}}/${{items.length}}`);
  }}

  async function worker() {{
    while (queue.length) {{
      const it = queue.shift();
      try {{ await uploadOne(it); }}
      catch(e) {{ log("FAIL "+it.rel+" :: "+e.message); }}
    }}
  }}

  await Promise.all(Array.from({{length:K}}, worker));
  // redirige vers le traitement serveur
  window.location.search = "?batch=" + batchId;
}});
</script>
</body></html>
""", height=320)
    st.stop()
