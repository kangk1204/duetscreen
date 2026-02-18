#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOOLS_DIR="${TOOLS_DIR:-$ROOT_DIR/third_party/docking_tools}"
BIN_DIR="$TOOLS_DIR/bin"
P2RANK_DIR="$TOOLS_DIR/p2rank"
GNINA_DIR="$TOOLS_DIR/gnina"
DIFFDOCK_DIR="${DIFFDOCK_DIR:-$ROOT_DIR/third_party/DiffDock}"
DIFFDOCK_ENV="${DIFFDOCK_ENV:-diffdock}"

mkdir -p "$TOOLS_DIR" "$BIN_DIR"

fetch_p2rank() {
  mkdir -p "$P2RANK_DIR"
  python - "$P2RANK_DIR" <<'PY'
import json, os, re, sys, urllib.request, zipfile, tarfile
dest = sys.argv[1]
api = "https://api.github.com/repos/rdk/p2rank/releases/latest"
data = json.load(urllib.request.urlopen(api))
assets = data.get("assets", [])
def ok(name):
    name = name.lower()
    return ("p2rank" in name) and name.endswith((".zip", ".tar.gz", ".tgz"))
filtered = [a for a in assets if ok(a.get("name",""))]
if not filtered:
    raise SystemExit("P2Rank: no suitable assets in latest release.")
filtered.sort(key=lambda a: a.get("size", 0), reverse=True)
asset = filtered[0]
out = os.path.join(dest, asset["name"])
urllib.request.urlretrieve(asset["browser_download_url"], out)
if out.endswith(".zip"):
    with zipfile.ZipFile(out, "r") as zf:
        zf.extractall(dest)
elif out.endswith(".tar.gz") or out.endswith(".tgz"):
    with tarfile.open(out, "r:gz") as tf:
        tf.extractall(dest)
os.remove(out)
PY

  local prank_path
  prank_path=$(find "$P2RANK_DIR" -maxdepth 4 -type f \( -name prank -o -name prank.sh -o -name p2rank \) | head -n 1 || true)
  if [[ -z "$prank_path" ]]; then
    echo "P2Rank prank binary not found in $P2RANK_DIR" >&2
    exit 1
  fi
  chmod +x "$prank_path"
  cat > "$BIN_DIR/prank" <<EOF
#!/usr/bin/env bash
exec "$prank_path" "\$@"
EOF
  chmod +x "$BIN_DIR/prank"
}

fetch_gnina() {
  mkdir -p "$GNINA_DIR"
  python - "$GNINA_DIR" <<'PY'
import json, os, re, sys, tarfile, zipfile, urllib.request
dest = sys.argv[1]
api = "https://api.github.com/repos/gnina/gnina/releases/latest"
data = json.load(urllib.request.urlopen(api))
assets = data.get("assets", [])
if not assets:
    raise SystemExit("GNINA: no assets in latest release.")
def score(name):
    name = name.lower()
    score = 0
    if "cuda12.8" in name: score += 100
    if "cuda12" in name: score += 80
    if "cuda11" in name: score += 60
    if "linux" in name or "ubuntu" in name: score += 40
    if name.endswith((".tar.gz",".tgz",".zip",".tar.xz",".tar.bz2")): score += 10
    return score
assets.sort(key=lambda a: score(a.get("name","")), reverse=True)
asset = assets[0]
out = os.path.join(dest, asset["name"])
urllib.request.urlretrieve(asset["browser_download_url"], out)
extracted = False
if out.endswith(".zip"):
    with zipfile.ZipFile(out, "r") as zf:
        zf.extractall(dest)
    extracted = True
elif out.endswith(".tar.gz") or out.endswith(".tgz"):
    with tarfile.open(out, "r:gz") as tf:
        tf.extractall(dest)
    extracted = True
elif out.endswith(".tar.xz"):
    with tarfile.open(out, "r:xz") as tf:
        tf.extractall(dest)
    extracted = True
elif out.endswith(".tar.bz2"):
    with tarfile.open(out, "r:bz2") as tf:
        tf.extractall(dest)
    extracted = True

# If it's a raw binary, keep it and name it "gnina".
if not extracted:
    gnina_path = os.path.join(dest, "gnina")
    if os.path.abspath(out) != os.path.abspath(gnina_path):
        os.rename(out, gnina_path)
else:
    os.remove(out)
PY

  local gnina_path
  gnina_path=$(find "$GNINA_DIR" -maxdepth 5 -type f -name gnina | head -n 1 || true)
  if [[ -z "$gnina_path" ]]; then
    echo "GNINA binary not found in $GNINA_DIR" >&2
    exit 1
  fi
  chmod +x "$gnina_path"
  ln -sf "$gnina_path" "$BIN_DIR/gnina"
}

install_diffdock() {
  if [[ ! -d "$DIFFDOCK_DIR/.git" ]]; then
    git clone --depth 1 https://github.com/gcorso/DiffDock.git "$DIFFDOCK_DIR"
  fi
  if ! conda env list | awk '{print $1}' | grep -qx "$DIFFDOCK_ENV"; then
    conda env create -n "$DIFFDOCK_ENV" -f "$DIFFDOCK_DIR/environment.yml"
  fi
}

echo "[1/3] Installing P2Rank..."
fetch_p2rank
echo "[2/3] Installing GNINA..."
fetch_gnina
echo "[3/3] Installing DiffDock..."
install_diffdock

cat <<EOF

Done.
Add tools to PATH for this session:
  export PATH="$BIN_DIR:\$PATH"

DiffDock env:
  export DIFFDOCK_DIR="$DIFFDOCK_DIR"
  export DIFFDOCK_CONDA_ENV="$DIFFDOCK_ENV"
EOF
