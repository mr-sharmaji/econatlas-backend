# Making the backend public (accessible outside your WiFi)

Your backend runs on your Windows machine (e.g. **http://192.168.0.103:8000** on LAN). To reach it from the internet you can use **Cloudflare Tunnel** (recommended, with your own domain) or **port forwarding**. This guide focuses on **Cloudflare Tunnel** with a **custom domain** managed in Cloudflare.

---

## Option 1: Cloudflare Tunnel with your own domain (recommended)

Use a **domain you own** and add it to Cloudflare. Then run **cloudflared** as a tunnel so traffic to your domain (e.g. `https://api.yourdomain.com`) is forwarded to your local backend. No router config, no open ports; runs as a Windows service so it survives restarts.

### Prerequisites

- Backend running (e.g. `docker compose up -d`), reachable at **http://localhost:8000** on the same PC.
- A **domain** you own, added to Cloudflare (Add a site → update nameservers at your registrar).

### Step 1: Install cloudflared on Windows

1. Download: [github.com/cloudflare/cloudflared/releases](https://github.com/cloudflare/cloudflared/releases) → **windows_amd64.exe**.
2. Rename to `cloudflared.exe` and place in a folder (e.g. `C:\Program Files\cloudflared\`). Optionally add that folder to your **PATH**.
3. In PowerShell: `cloudflared --version` to confirm.

### Step 2: Log in to Cloudflare (one-time)

```powershell
cloudflared tunnel login
```

A browser opens. Sign in to your Cloudflare account and **select the domain** you added. Authorize; a certificate is saved under `%USERPROFILE%\.cloudflared\`.

### Step 3: Create a named tunnel

```powershell
cloudflared tunnel create econatlas-backend
```

Note the **Tunnel ID** (e.g. `a1b2c3d4-e5f6-7890-abcd-ef1234567890`).

### Step 4: Create the tunnel config

1. Ensure folder exists: `mkdir $env:USERPROFILE\.cloudflared`
2. Create or edit `%USERPROFILE%\.cloudflared\config.yml`:

```yaml
tunnel: TUNNEL_ID
credentials-file: C:\Users\YOUR_USERNAME\.cloudflared\TUNNEL_ID.json

ingress:
  - hostname: api.yourdomain.com
    service: http://localhost:8000
  - service: http_status:404
```

Replace **TUNNEL_ID** with the ID from Step 3, **YOUR_USERNAME** with your Windows username, and **api.yourdomain.com** with the hostname you want (e.g. `api.velqon.xyz`).

### Step 5: Create DNS CNAME in Cloudflare

1. In [dash.cloudflare.com](https://dash.cloudflare.com) → your domain → **DNS** → **Records**.
2. Add record: **Type** CNAME, **Name** `api` (or your subdomain), **Target** `TUNNEL_ID.cfargotunnel.com`, **Proxy** enabled (orange cloud). Save.

### Step 6: Install and run as a Windows service

Run PowerShell **as Administrator**:

```powershell
cloudflared service install
sc start cloudflared
sc config cloudflared start= auto
```

The service uses the config at `%USERPROFILE%\.cloudflared\config.yml` by default.

### Step 7: Verify

- Open **https://api.yourdomain.com** (or your hostname) from any network. You should see your API (e.g. `/docs`).
- In the app, set the API base URL to this **https** URL.

### Useful commands (PowerShell as Administrator)

| Action              | Command                      |
|---------------------|------------------------------|
| Start tunnel        | `sc start cloudflared`       |
| Stop tunnel         | `sc stop cloudflared`        |
| Check status        | `sc query cloudflared`       |
| Uninstall service   | `cloudflared service uninstall` |

---

## Option 2: Port forwarding (router)

Use this if you prefer not to use a tunnel and your ISP allows inbound port forwarding.

### 1. Find your public IP

From a device on your network: [whatismyip.com](https://whatismyip.com). Note the public IP.

### 2. Port forward on your router

- Log in to your router. Find **Port Forwarding** / **Virtual Server** / **NAT**.
- Add a rule: **External port** 80 (or 8080), **Internal IP** the PC running the backend (e.g. `192.168.0.103`), **Internal port** 8000. Save.

### 3. Windows Firewall

On the PC running the backend:

```powershell
New-NetFirewallRule -DisplayName "EconAtlas Backend" -Direction Inbound -Protocol TCP -LocalPort 8000 -Action Allow
```

### 4. Use in app

From outside your WiFi: `http://<public-ip>:80` (or `:8080`). Set this as the API base URL in the app.

---

## Summary

| Goal                  | Action |
|-----------------------|--------|
| Backend on same WiFi  | Use `http://192.168.0.103:8000`. |
| Backend from internet | **Option 1:** Cloudflare Tunnel + custom domain → e.g. `https://api.yourdomain.com`. **Option 2:** Port forward → `http://<public-ip>:80`. |
| Use in app            | Set API base URL to the tunnel URL (HTTPS) or `http://<public-ip>:80`. |

---

## Monitoring the Windows host (real CPU/RAM/disk, not the WSL2 VM)

Docker Desktop on Windows runs containers inside a hidden WSL2 Linux
VM. Anything `psutil` reports from inside the FastAPI container — and
therefore everything in the `system` section of `/ops/health` and the
default Grafana dashboard — describes that **VM**, not Windows 11.
Symptom: Grafana shows 31 % memory used while Task Manager shows 90 %.

To get the real Windows host metrics in Grafana you install
[`windows_exporter`](https://github.com/prometheus-community/windows_exporter)
as a Windows service. Prometheus is already preconfigured to scrape it
on `host.docker.internal:9182`, and the `EconAtlas API` dashboard has a
"🪟 Windows Host" row that lights up automatically once the exporter
starts responding.

### Install (PowerShell as Administrator)

The fastest route is `winget`:

```powershell
winget install --id Prometheus.WindowsExporter
```

If `winget` isn't available, grab the latest `windows_exporter-X.Y.Z-amd64.msi`
from the [GitHub releases page](https://github.com/prometheus-community/windows_exporter/releases)
and run:

```powershell
msiexec /i windows_exporter-0.30.0-amd64.msi `
  ENABLED_COLLECTORS=cpu,cs,logical_disk,net,os,memory,system `
  LISTEN_PORT=9182 /qn
```

Either method installs and starts a Windows service called
`windows_exporter` that listens on `http://localhost:9182/metrics`.

### Open the firewall (so Docker can reach it)

```powershell
New-NetFirewallRule -DisplayName "windows_exporter" `
  -Direction Inbound -Protocol TCP -LocalPort 9182 -Action Allow
```

`host.docker.internal` already routes from inside any container to the
Windows host — no extra Docker network config needed.

### Verify

From your Windows shell:

```powershell
curl http://localhost:9182/metrics | Select-String windows_cs_physical_memory_bytes
```

You should see one line ending with the total RAM in bytes.

From inside the `app` container:

```bash
docker compose exec app curl -s http://host.docker.internal:9182/metrics | head -5
```

### Pick up in the stack

After installing the exporter, restart Prometheus so it loads the new
scrape job and Grafana so it re-provisions the dashboard:

```bash
docker compose restart prometheus grafana
```

Within ~30 seconds the Grafana dashboard's "🪟 Windows Host" row should
populate, and `GET /ops/health` will include a new top-level `host`
section alongside `system` — `system` is the WSL2 VM view (left in
place for backward compatibility), `host` is the real Windows 11.

If `windows_exporter` isn't installed, both `host` and the dashboard
panels stay empty. Nothing else breaks — `psutil` continues to report
the WSL2 VM as before.
