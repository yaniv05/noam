# app.py
import os, io, re, json, uuid, base64, time, tempfile, threading, shutil
import requests, streamlit as st, boto3
from typing import List, Tuple, Optional, Dict, Set
from PIL import Image, ImageOps, ImageFile
import botocore.client
from datetime import datetime

# ---------- PIL tolère certains JPEG tronqués ----------
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

# Navigateur (virtual-hosted) & SDK endpoint (boto3)
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

# ========= Constantes =========
IMG_EXTS = {".jpg",".jpeg",".png",".JPG",".JPEG",".PNG"}
CLIENT_RE = re.compile(r"^\s*(.+?)\s*-\s*(.+?)\s*$")  # "ClientName - Address"

# ========= Utilitaires =========
def now_str() -> str:
    return datetime.now().strftime("%H:%M:%S")

def safe_join(*parts: str) -> str:
    p = os.path.join(*parts)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    return p

def slug(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", s).strip("_")

# ---------- Fidealis ----------
def api_login() -> Optional[str]:
    try:
        r = requests.get(API_URL, params={"key":API_KEY,"call":"loginUserFromAccountKey","accountKey":ACCOUNT_KEY}, timeout=30)
        r.raise_for_status()
        return r.json().get("PHPSESSID")
    except Exception:
        return None

def api_upload_files(description: str, filepaths: List[str], session_id: str, log_cb):
    total = len(filepaths)
    if total == 0:
        return
    log_cb(f"{now_str()}  Envoi Fidealis: {total} fichier(s) en lots de 12")
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
                log_cb(f"{now_str()}    Lot {start+1}-{start+len(batch)}: HTTP {r.status_code} — retour: {js}")
            except Exception:
                log_cb(f"{now_str()}    Lot {start+1}-{start+len(batch)}: HTTP {r.status_code}")
        except Exception as e:
            log_cb(f"{now_str()}    Lot {start+1}-{start+len(batch)}: erreur requête: {e}")

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

# ========= État partagé (thread) =========
LOCK = threading.Lock()

def init_state():
    # structure : st.session_state["runner"] et st.session_state["clients"]
    if "runner" not in st.session_state:
        st.session_state["runner"] = {
            "batch_id": None,
            "running": False,
            "started_ts": None,
            "last_update": None,
            "inactivity_s": 45.0,
            "poll_s": 2.0,
            "max_dim": 1600,
            "jpeg_q": 80,
            "root_tmp": None,
            "ended": False,
            "error": None,
        }
    if "clients" not in st.session_state:
        st.session_state["clients"] = {}  # client_folder -> dict
    if "global_log" not in st.session_state:
        st.session_state["global_log"] = []
    if "upload_panel_shown" not in st.session_state:
        st.session_state["upload_panel_shown"] = False

def append_log(msg: str):
    with LOCK:
        st.session_state["global_log"].append(msg)
        # keep last 1000 lines
        if len(st.session_state["global_log"]) > 1000:
            st.session_state["global_log"] = st.session_state["global_log"][-1000:]

def ensure_client(client_folder: str, name: str, address: str):
    clients = st.session_state["clients"]
    if client_folder not in clients:
        lat, lng = get_coordinates(address)
        if lat is None or lng is None:
            lat, lng = ("N/A", "N/A")
        clients[client_folder] = {
            "name": name,
            "address": address,
            "lat": lat, "lng": lng,
            "seen_keys": set(),               # clés R2 déjà normalisées
            "normalized_count": 0,            # nb images normalisées sur disque
            "buffer_paths": [],               # chemins locaux en attente d'envoi
            "files_sent": 0,                  # nb images déjà envoyées (par blocs de 36)
            "collages_sent": 0,               # nb collages envoyés
            "api_calls": 0,                   # nb appels Fidealis faits
            "status": "en attente",
            "last_event": now_str(),
        }

def client_root_dir(batch_root: str, client_folder: str) -> str:
    return os.path.join(batch_root, slug(client_folder))

# ========= Thread de traitement =========
def processor_thread():
    runner = st.session_state["runner"]
    batch_id = runner["batch_id"]
    batch_prefix = f"uploads/{batch_id}/"
    root_tmp = runner["root_tmp"]
    inactivity_s = float(runner["inactivity_s"])
    poll_s = float(runner["poll_s"])
    max_dim = int(runner["max_dim"])
    jpeg_q = int(runner["jpeg_q"])

    session_id = st.session_state.get("fidealis_session_id")
    if not session_id:
        append_log(f"{now_str()}  Erreur: session Fidealis absente")
        with LOCK:
            runner["running"] = False
            runner["ended"] = True
            runner["error"] = "Session Fidealis absente"
        return

    append_log(f"{now_str()}  Démarrage traitement batch {batch_id}")
    last_activity = time.time()

    try:
        while True:
            # 1) lister clés et grouper par client
            keys = list_objects(batch_prefix)
            groups = group_keys_by_client(keys, batch_prefix)
            updated = False

            for client_folder, client_keys in groups.items():
                parsed = split_client(client_folder)
                if not parsed:
                    continue
                client_name, address = parsed
                ensure_client(client_folder, client_name, address)
                c = st.session_state["clients"][client_folder]
                c["status"] = "traitement"
                c["last_event"] = now_str()

                # 2) nouvelles clés
                new_keys = [k for k in client_keys if k not in c["seen_keys"]]
                if new_keys:
                    updated = True
                    # normaliser et stocker localement
                    cdir = client_root_dir(root_tmp, client_folder)
                    os.makedirs(cdir, exist_ok=True)
                    for key in new_keys:
                        try:
                            b = io.BytesIO()
                            s3.download_fileobj(R2_BUCKET, key, b)
                            jb = preprocess_to_jpeg_bytes(b.getvalue(), max_dim=max_dim, quality=jpeg_q)
                            outp = safe_join(cdir, f"{uuid.uuid4().hex}.jpg")
                            with open(outp, "wb") as o: o.write(jb)
                            c["buffer_paths"].append(outp)
                            c["normalized_count"] += 1
                            c["seen_keys"].add(key)
                        except Exception as e:
                            append_log(f"{now_str()}  Normalisation en échec {key}: {e}")

                    # 3) dès qu'on a des multiples de 36, on envoie
                    blocks = len(c["buffer_paths"]) // 36
                    if blocks > 0:
                        used_total = 0
                        for _ in range(blocks):
                            block = c["buffer_paths"][:36]
                            # collages pour ce block
                            tmp_send = safe_join(root_tmp, f"send_{uuid.uuid4().hex}", "")
                            os.makedirs(tmp_send, exist_ok=True)
                            collages = create_collages_from_paths(block, c["name"], tmp_send, q=jpeg_q)
                            description = (f"SCELLÉ NUMERIQUE Bénéficiaire: Nom: {c['name']}, "
                                           f"Adresse: {c['address']}, Coordonnées GPS: Latitude {c['lat']}, Longitude {c['lng']}")
                            # appel Fidealis
                            api_upload_files(description, collages, session_id, append_log)
                            c["files_sent"] += len(block)
                            c["collages_sent"] += len(collages)
                            c["api_calls"] += max(1, (len(collages)+11)//12)
                            # ménage
                            try:
                                shutil.rmtree(tmp_send, ignore_errors=True)
                            except Exception:
                                pass
                            del c["buffer_paths"][:36]
                            used_total += 36
                        append_log(f"{now_str()}  Client {c['name']}: bloc(s) de 36 traité(s) x{blocks}")
                # status
                st.session_state["clients"][client_folder] = c

            if updated:
                last_activity = time.time()

            # inactivité => finalisation reliquats et stop
            if time.time() - last_activity > inactivity_s:
                append_log(f"{now_str()}  Inactivité {int(inactivity_s)}s: finalisation des reliquats")
                # finalisation
                for client_folder, c in list(st.session_state["clients"].items()):
                    if not c["buffer_paths"]:
                        continue
                    tmp_send = safe_join(root_tmp, f"send_{uuid.uuid4().hex}", "")
                    os.makedirs(tmp_send, exist_ok=True)
                    collages = create_collages_from_paths(c["buffer_paths"], c["name"], tmp_send, q=jpeg_q)
                    description = (f"SCELLÉ NUMERIQUE Bénéficiaire: Nom: {c['name']}, "
                                   f"Adresse: {c['address']}, Coordonnées GPS: Latitude {c['lat']}, Longitude {c['lng']}")
                    api_upload_files(description, collages, session_id, append_log)
                    c["files_sent"] += len(c["buffer_paths"])
                    c["collages_sent"] += len(collages)
                    c["api_calls"] += max(1, (len(collages)+11)//12) if collages else 0
                    c["buffer_paths"].clear()
                    c["status"] = "terminé"
                    c["last_event"] = now_str()
                    try:
                        shutil.rmtree(tmp_send, ignore_errors=True)
                    except Exception:
                        pass
                    st.session_state["clients"][client_folder] = c
                break

            time.sleep(poll_s)

    except Exception as e:
        append_log(f"{now_str()}  Erreur fatale traitement: {e}")
        with LOCK:
            runner["error"] = str(e)

    finally:
        with LOCK:
            runner["running"] = False
            runner["ended"] = True
        append_log(f"{now_str()}  Traitement terminé")
        # Option: nettoyer l’arborescence /tmp du batch
        try:
            shutil.rmtree(root_tmp, ignore_errors=True)
        except Exception:
            pass

# ========= UI =========
st.set_page_config(page_title="FIDEALIS — Dossier → R2 → Traitement automatique", layout="wide")
st.title("FIDEALIS — Dossier → R2 → Collages → Dépôt (automatique)")

init_state()

# Connexion Fidealis
session_id = api_login()
if not session_id:
    st.error("Connexion Fidealis échouée (API_URL/API_KEY/ACCOUNT_KEY).")
    st.stop()
st.session_state["fidealis_session_id"] = session_id

credits = get_credit(session_id)
if isinstance(credits, dict):
    st.caption(f"Crédit restant (Produit 4) : {get_quantity_for_product_4(credits)}")

# Options
with st.expander("Options de traitement"):
    st.session_state["runner"]["max_dim"] = st.slider("Dimension max (px) avant collage", 800, 4000, st.session_state["runner"]["max_dim"], step=100)
    st.session_state["runner"]["jpeg_q"]  = st.slider("Qualité JPEG", 50, 95, st.session_state["runner"]["jpeg_q"], step=1)
    st.session_state["runner"]["inactivity_s"] = st.slider("Arrêt auto si plus de nouvelles images (s)", 10, 300, int(st.session_state["runner"]["inactivity_s"]), step=5)
    st.session_state["runner"]["poll_s"] = st.slider("Intervalle de polling R2 (s)", 0.5, 5.0, float(st.session_state["runner"]["poll_s"]), step=0.5)

# Démarrage : un seul bouton “Choisir dossier…”
colA, colB = st.columns([1,2])
with colA:
    start_clicked = st.button("Choisir le dossier et tout lancer", type="primary")
with colB:
    st.write("Après sélection, l’upload vers R2 démarre et le traitement s’exécute en parallèle. Le suivi apparaît ci-dessous.")

if start_clicked:
    if not all([R2_ACCOUNT_ID, R2_BUCKET, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]):
        st.error("R2: variables d’environnement manquantes (ACCOUNT_ID/BUCKET/ACCESS_KEY/SECRET).")
        st.stop()

    # reset état
    st.session_state["clients"].clear()
    st.session_state["global_log"].clear()

    batch_id = str(uuid.uuid4())
    runner = st.session_state["runner"]
    runner["batch_id"] = batch_id
    runner["running"] = True
    runner["ended"] = False
    runner["error"] = None
    runner["started_ts"] = now_str()
    runner["last_update"] = now_str()
    runner["root_tmp"] = os.path.join("/tmp", f"batch_{batch_id}")
    os.makedirs(runner["root_tmp"], exist_ok=True)

    # Panneau Upload (navigateur → R2 via PUT signé)
    prefix = f"uploads/{batch_id}/"
    st.session_state["upload_panel_shown"] = True
    st.subheader("Upload vers R2 (client)")
    st.components.v1.html(f"""
<!doctype html><html>
<body>
<input id="picker" type="file" webkitdirectory directory multiple style="display:none" />
<button id="go" style="padding:10px 16px;">Choisir le dossier…</button>
<pre id="log" style="white-space:pre-wrap;border:1px solid #ccc;padding:8px;border-radius:6px;max-height:280px;overflow:auto;margin-top:10px;"></pre>

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
        log(`Progression upload: ${{ok+ko}}/${{items.length}} (${{pct}}%)`);
      }}
    }} catch (e) {{
      ko++;
      log("Echec upload: " + it.rel + " :: " + (e && e.message ? e.message : e));
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
  log(`Upload terminé. OK=${{ok}}, FAIL=${{ko}}. Durée: ${{dt}}s`);
}});
</script>
</body></html>
""", height=360)

    # Lance le thread de traitement immédiatement
    t = threading.Thread(target=processor_thread, daemon=True)
    t.start()

# Affichages de suivi (se mettent à jour automatiquement)
runner = st.session_state["runner"]

# Auto-refresh léger pendant que ça tourne
if runner["running"] and not runner["ended"]:
    st.experimental_rerun  # no-op placeholder to remind; Streamlit auto-reruns on UI events
st_autorefresh_ms = 1000 if runner["running"] and not runner["ended"] else 0
if st_autorefresh_ms:
    st.experimental_set_query_params(batch=runner["batch_id"])
    st.autorefresh(interval=st_autorefresh_ms, key="auto_refresh_key", limit=None)

# Panneau progression traitement
st.subheader("Traitement par client")
clients = st.session_state["clients"]
if clients:
    # tableau synthétique
    import pandas as pd
    rows = []
    for cf, c in clients.items():
        rows.append({
            "Client": c["name"],
            "Adresse": c["address"],
            "Normalisées": c["normalized_count"],
            "En attente (buffer)": len(c["buffer_paths"]),
            "Images envoyées": c["files_sent"],
            "Collages envoyés": c["collages_sent"],
            "Appels Fidealis": c["api_calls"],
            "Statut": c["status"],
            "Dernier évènement": c["last_event"],
        })
    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)

    # barres de progression par client (approx: envoyées / (normalisées + en attente))
    for cf, c in clients.items():
        total = c["files_sent"] + len(c["buffer_paths"])
        done = c["files_sent"]
        pct = (done / total) if total else 0.0
        st.write(f"{c['name']} — progression envoi API")
        st.progress(pct)

else:
    st.info("Aucun client détecté pour l’instant.")

# Logs serveur
st.subheader("Journal de traitement (serveur)")
st.text("\n".join(st.session_state["global_log"][-400:]))

# État final
if runner["ended"]:
    if runner["error"]:
        st.error(f"Terminé avec erreur: {runner['error']}")
    else:
        st.success("Traitement terminé.")
