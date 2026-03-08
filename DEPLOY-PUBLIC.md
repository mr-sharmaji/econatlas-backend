# Making the backend public (accessible outside your WiFi)

Your backend runs on your Windows machine (e.g. **http://192.168.0.103:8000** on LAN). To reach it from the internet you can use **ngrok** (easiest, no custom domain) or **port forwarding**. This guide focuses on **ngrok** so you get a public HTTPS URL with no router or domain setup.

---

## Option 1: ngrok (recommended – no custom domain)

**No custom domain needed.** ngrok’s free tier gives you a public HTTPS URL (e.g. `https://abc123.ngrok-free.app`) that forwards to your local backend. Works from any network (e.g. phone on mobile data).

### Free tier

| Item | Free? |
|------|--------|
| **ngrok account** | Free sign-up at [ngrok.com](https://ngrok.com). No credit card for basic use. |
| **HTTP/HTTPS tunnel** | Free. One tunnel at a time; random URL (e.g. `https://xxxx.ngrok-free.app`). |
| **Stable URL / reserved domain** | Paid. On the free tier the URL **changes each time** you start ngrok. |

### Step 1: Install ngrok on Windows

1. Download: [ngrok.com/download](https://ngrok.com/download) → choose **Windows**.
2. Unzip and put `ngrok.exe` in a folder (e.g. `C:\Program Files\ngrok\`). Optionally add that folder to your **PATH** (Settings → System → About → Advanced system settings → Environment Variables → Path).
3. Open PowerShell or Command Prompt and run:

   ```powershell
   ngrok version
   ```

   You should see a version number.

### Step 2: Sign up and add your authtoken

1. Sign up at [dashboard.ngrok.com/signup](https://dashboard.ngrok.com/signup) (free).
2. In the dashboard, copy your **authtoken** (or go to [dashboard.ngrok.com/get-started/your-authtoken](https://dashboard.ngrok.com/get-started/your-authtoken)).
3. In PowerShell or Command Prompt, run (replace with your token):

   ```powershell
   ngrok config add-authtoken YOUR_AUTHTOKEN
   ```

   You only need to do this once per machine.

### Step 3: Start your backend and run ngrok

1. Start your backend (e.g. `docker compose up -d`) so it’s reachable at **http://localhost:8000**.
2. Run:

   ```powershell
   ngrok http 8000
   ```

3. ngrok prints a **Forwarding** URL, e.g. `https://abc123.ngrok-free.app` → `http://localhost:8000`. Copy the **https** URL.
4. In your app, set the **API base URL** to that URL (e.g. `https://abc123.ngrok-free.app`). You can open it from any device (same WiFi or mobile data).
5. **Keep the ngrok window open** while you need the tunnel. Closing it stops the tunnel. When you run `ngrok http 8000` again, you’ll get a **new URL** (free tier); update the app if needed.

### Optional: Run ngrok in the background or after restart

- **Background:** Run `ngrok http 8000` in a separate PowerShell window, or use `Start-Process` to run it detached.
- **After restart:** Start your backend (e.g. Docker), then run `ngrok http 8000` again. The URL will change on the free tier unless you use a paid reserved domain.

### Optional: Inspect traffic

With ngrok running, open **http://localhost:4040** in your browser to see the ngrok web interface (requests, replay, etc.).

---

## Option 2: Port forwarding (router)

Use this if your ISP allows inbound port forwarding and you prefer not to use ngrok.

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

| Goal | Action |
|------|--------|
| Backend on same WiFi | Use `http://192.168.0.103:8000`. |
| Backend from internet (no domain) | **Option 1:** ngrok → run `ngrok http 8000`, use the printed **https** URL in the app. |
| Backend from internet (no tunnel) | **Option 2:** Port forward 80 → 192.168.0.103:8000 → use `http://<public-ip>:80` in the app. |
| Use in app | Set API base URL to the ngrok URL (HTTPS) or `http://<public-ip>:80`. |

**ngrok free tier:** No custom domain needed. The URL changes each time you start ngrok; for a fixed URL you’d need a paid ngrok plan (reserved domain). Keep the `ngrok http 8000` process running while you need the tunnel.
