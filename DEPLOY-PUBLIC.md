# Making the backend public (accessible outside your WiFi)

Your backend runs on your Windows machine (e.g. **http://192.168.0.103:8000** on LAN). To reach it from the internet you can use **port forwarding** or a **Cloudflare Tunnel**. This guide focuses on **Cloudflare Tunnel as a Windows service** so it runs in the background and survives restarts.

---

## 100% free – no paid plans or credit card

Everything in this guide is free:

| Item | Free? |
|------|--------|
| **Cloudflare account** | Free sign-up, no credit card. [dash.cloudflare.com/sign-up](https://dash.cloudflare.com/sign-up) |
| **Cloudflare Tunnel (cloudflared)** | Free. Unlimited tunnels and bandwidth for personal use. |
| **Tunnel hostname** | Use **cfargotunnel.com** (e.g. `econatlas.cfargotunnel.com`) – free, no own domain needed. |
| **Zero Trust dashboard** (for tunnel hostname) | Free. Used only to assign the `*.cfargotunnel.com` hostname to your tunnel. |
| **Port forwarding** (Option 2) | Free (your router, no subscription). |
| **Windows service** | Free (built into Windows). |

You do **not** need a custom domain, a paid Cloudflare plan, or ngrok paid. The recommended path uses only the free **cfargotunnel.com** hostname.

---

## Option 1: Cloudflare Tunnel as Windows service (recommended)

Free, no router config, runs after restart. Do this on the **same Windows machine** where the backend (Docker) runs.

### Prerequisites

- Backend running (e.g. `docker compose up -d`), reachable at **http://localhost:8000** on this machine.
- A **free** Cloudflare account: [dash.cloudflare.com/sign-up](https://dash.cloudflare.com/sign-up). No credit card.

---

### Step 1: Download and install cloudflared on Windows

1. Open: [https://github.com/cloudflare/cloudflared/releases](https://github.com/cloudflare/cloudflared/releases)
2. Download the latest **windows_amd64.exe** (e.g. `cloudflared-windows-amd64.exe`).
3. Rename it to `cloudflared.exe` and move it to a permanent folder, e.g. `C:\Program Files\cloudflared\cloudflared.exe`.
4. Add that folder to your **PATH** (optional but helpful):
   - Settings → System → About → Advanced system settings → Environment Variables.
   - Under "System variables", select **Path** → Edit → New → `C:\Program Files\cloudflared` → OK.
5. Open a **new** PowerShell or Command Prompt and run:

   ```powershell
   cloudflared --version
   ```

   You should see a version number.

---

### Step 2: Log in to Cloudflare (one-time, free)

1. Run:

   ```powershell
   cloudflared tunnel login
   ```

2. A browser window opens. Sign in with your **free** Cloudflare account (or sign up – no credit card).
3. You may be asked to **choose a domain**. If you have a domain already on Cloudflare (free DNS), select it. If you don’t have a domain, go to [dash.cloudflare.com](https://dash.cloudflare.com) → **Add a site** and add any domain you own (or use a free domain from a free provider); Cloudflare DNS is free. For the tunnel URL you will use the free **cfargotunnel.com** hostname (Step 5b), so the domain you pick here is only for the certificate – it can be any domain in your account.
4. Click **Authorize**. A certificate is saved under `%USERPROFILE%\.cloudflared\`. Leave it there.

---

### Step 3: Create a named tunnel

1. Create a tunnel (replace `econatlas-backend` with any name you like):

   ```powershell
   cloudflared tunnel create econatlas-backend
   ```

2. Note the **Tunnel ID** shown (e.g. `a1b2c3d4-e5f6-7890-abcd-ef1234567890`). You’ll need it for the config file.

---

### Step 4: Create the tunnel config file

1. Open the config folder (create it if it doesn’t exist):

   ```powershell
   mkdir $env:USERPROFILE\.cloudflared
   notepad $env:USERPROFILE\.cloudflared\config.yml
   ```

2. Paste the following. Replace **TUNNEL_ID** with the ID from Step 3, and **YOUR_WINDOWS_USERNAME** with your Windows username (e.g. `John`). The credentials file is at `C:\Users\YOUR_WINDOWS_USERNAME\.cloudflared\TUNNEL_ID.json`.

   **Recommended (100% free – no own domain):** use the free **cfargotunnel.com** hostname. You’ll set the hostname in Step 5b; for now use a placeholder and replace it after Step 5b:

   ```yaml
   tunnel: TUNNEL_ID
   credentials-file: C:\Users\YOUR_WINDOWS_USERNAME\.cloudflared\TUNNEL_ID.json

   ingress:
     - hostname: econatlas.cfargotunnel.com
       service: http://localhost:8000
     - service: http_status:404
   ```

   Use `econatlas` or any subdomain you’ll create in Step 5b. Service is **http://localhost:8000** if the backend runs on this PC.

3. Save and close the file.

---

### Step 5a: (Optional) Custom domain – DNS CNAME

Only if you use **your own domain** (e.g. `api.yourdomain.com`). If you use the free **cfargotunnel.com** hostname, skip to Step 5b.

- Go to [dash.cloudflare.com](https://dash.cloudflare.com) → your domain → **DNS** → **Records**.
- Add CNAME: **Name** `api` (or your subdomain), **Target** `TUNNEL_ID.cfargotunnel.com`, **Proxy** on.

---

### Step 5b: (Recommended) Free cfargotunnel.com hostname – no own domain

Everything here is **free**; no paid plan or domain needed.

1. Go to **Zero Trust** (free): [one.dash.cloudflare.com](https://one.dash.cloudflare.com). Sign in with your Cloudflare account. You may be asked to set up a team name – choose any; the free tier is enough.
2. Left menu: **Networks** → **Tunnels**. You should see your tunnel (e.g. `econatlas-backend`).
3. Click the tunnel → **Public Hostname** tab → **Add a hostname**.
4. **Subdomain:** e.g. `econatlas`. **Domain:** choose **cfargotunnel.com** (free). **Service type:** HTTP, **URL:** `localhost:8000` (or `192.168.0.103:8000` if the backend is on another machine). Save.
5. Your public URL is **https://econatlas.cfargotunnel.com** (or whatever subdomain you chose). Use this exact hostname in `config.yml` (Step 4) if you used a placeholder.
6. Restart the tunnel service after changing config: `sc stop cloudflared` then `sc start cloudflared`.

---

### Step 6: Install and run as a Windows service

Run PowerShell **as Administrator**:

1. Install the service (use the same tunnel name as in Step 3):

   ```powershell
   cloudflared service install
   ```

   If the installer asks for a config path, use:

   ```powershell
   C:\Users\YOUR_WINDOWS_USERNAME\.cloudflared\config.yml
   ```

   Or, if you need to point the service at your config explicitly:

   ```powershell
   cloudflared service install
   ```

   The service reads config from `%USERPROFILE%\.cloudflared\config.yml` by default.

2. Start the service:

   ```powershell
   sc start cloudflared
   ```

   Or via Services GUI: Win + R → `services.msc` → find **cloudflared** → Start.

3. Set startup type to **Automatic** (so it runs after restart):

   ```powershell
   sc config cloudflared start= auto
   ```

   Or in Services: cloudflared → Properties → Startup type: **Automatic**.

---

### Step 7: Verify

- From this PC: open your tunnel URL (e.g. **https://econatlas.cfargotunnel.com**).
- From another network (e.g. phone on mobile data): open the same URL. You should see your API (e.g. `/docs`).
- In the app, set the API base URL to this **https** URL (all free).

---

### Useful commands (PowerShell as Administrator)

| Action              | Command                    |
|---------------------|----------------------------|
| Start tunnel service | `sc start cloudflared`   |
| Stop tunnel service  | `sc stop cloudflared`    |
| Check status         | `sc query cloudflared`   |
| Uninstall service    | `cloudflared service uninstall` |

---

### Running multiple services (still free)

You can expose **multiple local apps** through the same tunnel using different hostnames or paths.

**One tunnel, multiple hostnames**

In `config.yml`, add more `ingress` entries (order matters; put the catch-all last):

```yaml
tunnel: TUNNEL_ID
credentials-file: C:\Users\YOUR_USERNAME\.cloudflared\TUNNEL_ID.json

ingress:
  - hostname: api.econatlas.cfargotunnel.com
    service: http://localhost:8000
  - hostname: app.econatlas.cfargotunnel.com
    service: http://localhost:3000
  - hostname: admin.econatlas.cfargotunnel.com
    service: http://localhost:8080
  - service: http_status:404
```

Then in Zero Trust → your tunnel → **Public Hostname**, add a hostname for each (e.g. subdomain `api`, domain `econatlas.cfargotunnel.com` → `localhost:8000`; subdomain `app` → `localhost:3000`; etc.). All of this stays on the **free** tier.

**Multiple tunnels**

You can also create separate tunnels (e.g. `cloudflared tunnel create other-app`) and run multiple Windows services, each with its own config. Free tier allows multiple tunnels.

---

### Alternative: Quick Tunnel (100% free, no login – temporary URL)

If you only want a quick public URL **without** any login or config:

```powershell
cloudflared tunnel --url http://localhost:8000
```

A **free** URL like `https://xxxx-xx-xx-xx.trycloudflare.com` is printed. Use it in the app. This process must stay running; closing the window stops the tunnel. For a **stable URL and run-after-restart**, use the named tunnel + Windows service (Steps 1–7) with the free **cfargotunnel.com** hostname.

---

## Option 2: Port forwarding (router)

Use this if your ISP allows inbound port forwarding and you prefer not to use a tunnel.

### 1. Find your public IP

- From a device on your home network, open [https://whatismyip.com](https://whatismyip.com). Note the **public** IP (e.g. `103.101.212.172`).

### 2. Port forward on your router

- Log in to your router (e.g. `http://192.168.0.1` or `http://172.24.82.129`).
- Find **Port Forwarding** / **Virtual Server** / **NAT**.
- Add a rule:
  - **External port:** 80 (HTTP) or 8080
  - **Internal IP:** **192.168.0.103** (the PC running Docker)
  - **Internal port:** 8000
- Save.

### 3. Windows Firewall

On the PC running the backend (192.168.0.103):

```powershell
New-NetFirewallRule -DisplayName "EconAtlas Backend" -Direction Inbound -Protocol TCP -LocalPort 8000 -Action Allow
```

### 4. Test and use in app

- From outside your WiFi: `http://<public-ip>:80` (or `:8080`).
- In the app, set API base URL to that.

---

## Summary

| Goal                    | Action |
|-------------------------|--------|
| Backend on same WiFi    | Use `http://192.168.0.103:8000`. |
| Backend from internet   | **Option 1 (all free):** Cloudflare Tunnel + free **cfargotunnel.com** hostname → e.g. `https://econatlas.cfargotunnel.com`. **Option 2 (free):** Port forward 80 → 192.168.0.103:8000 → `http://<public-ip>:80`. |
| Survives PC restart     | Use **Cloudflare Tunnel + Windows service** (Option 1) with `sc config cloudflared start= auto`. |
| Use in app              | Set API base URL to the tunnel URL (HTTPS) or `http://<public-ip>:80`. |

All options above are **free** (no paid Cloudflare plan, no credit card). The tunnel gives you HTTPS at no cost. If you later add your own domain to Cloudflare, you can point it at the same tunnel and still use the free tier.
