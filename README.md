# HVAC AI Voice Agent

A production-grade inbound voice agent for HVAC emergency dispatch, built with [Pipecat](https://docs.pipecat.ai/). The agent handles phone calls, triages HVAC issues, and books technician appointments via a Frappe CRM backend — all in real-time.

## Architecture

The system runs **three separate processes** that work together:

```
┌──────────────┐       ┌─────────────────────────────┐       ┌──────────────────────────────┐
│   Twilio     │──WS──▶│  FastAPI Server (main.py)    │──SSE─▶│  FastMCP Server              │
│  (Inbound    │       │  :7860                       │       │  (mcp_server.py) :8000       │
│   Calls)     │       │                              │       │                              │
└──────────────┘       │  Deepgram STT ─▶ OpenAI LLM  │       │  Pydantic Validation         │
                       │  ─▶ Cartesia TTS             │       │  Redis Pessimistic Locking   │
                       └─────────────────────────────┘       │  Frappe CRM Proxy            │
                                                              └──────────┬───────────────────┘
                                                                         │
                                                              ┌──────────▼───────────────────┐
                                                              │  Redis (:6379)               │
                                                              │  Frappe CRM (Cloud)          │
                                                              └──────────────────────────────┘
```

| Process | File | Default Port | Purpose |
|---------|------|-------------|---------|
| **Redis** | system service | `6379` | Distributed locking to prevent double-booking |
| **MCP Server** | `server/mcp_server.py` | `8000` | Tool execution layer (booking logic, CRM proxy) |
| **FastAPI Server** | `server/main.py` | `7860` | Telephony orchestration (STT → LLM → TTS pipeline) |
| **ngrok** *(optional)* | `server/ngrok` | — | Exposes local server to Twilio's public webhook |

### Pipeline

```
Twilio Audio → Deepgram Nova-3 (STT) → GPT-4o-mini (LLM) → Cartesia Sonic (TTS) → Twilio Audio
                                              │
                                              ▼
                                     MCP Tool Calls
                                     (check_and_book_slot)
```

## Prerequisites

### System Dependencies

| Dependency | Minimum Version | Install |
|-----------|----------------|---------|
| **Python** | 3.11+ | [python.org](https://www.python.org/downloads/) |
| **uv** | latest | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| **Redis** | 6.x+ | `sudo apt install redis-server` (Ubuntu/Debian) |

Verify:

```bash
python3 --version    # Should be 3.11+
uv --version         # Should print a version
redis-cli ping       # Should print PONG
```

### API Keys (5 Services)

| Service | Purpose | Sign Up |
|---------|---------|---------|
| **Deepgram** | Speech-to-Text (Nova-3) | [console.deepgram.com](https://console.deepgram.com/) |
| **OpenAI** | LLM Reasoning (GPT-4o-mini) | [platform.openai.com/api-keys](https://platform.openai.com/api-keys) |
| **Cartesia** | Text-to-Speech (Sonic) | [play.cartesia.ai](https://play.cartesia.ai/) |
| **Twilio** | Telephony Transport | [console.twilio.com](https://console.twilio.com/) |
| **Frappe CRM** | Backend appointment storage | Your Frappe Cloud instance |

> **⚠️ All keys are validated at startup.** If any key is missing or empty, the app will crash immediately with a clear error — this is intentional ("fail fast" via `config.py`).

### Frappe CRM Setup

The MCP server proxies bookings to a Frappe CRM instance. You can either use **Frappe Cloud** (managed hosting) or run CRM **locally via Docker** for testing.

#### Option A: Local Docker Setup (Recommended for Testing)

**Prerequisites:** Docker and Docker Compose installed ([docs.docker.com](https://docs.docker.com/)).

**Step 1:** Start the containers from the included `frappe-crm/` directory:

```bash
cd frappe-crm
docker compose up -d
```

This pulls and starts three containers:
- **MariaDB 10.8** — database
- **Redis** — caching/queue
- **Frappe Bench** — runs the `init.sh` script which automatically initializes bench (Frappe v15), clones and installs the CRM app, creates a site, and starts the dev server

> **⏱️ First run takes ~15–20 minutes** (downloading ~1GB Docker image + installing Python/JS dependencies). Subsequent restarts are instant thanks to the `bench-data` persistent volume.

**Step 2:** Add the hostname to `/etc/hosts`:

```bash
sudo sh -c 'echo "127.0.0.1 crm.localhost" >> /etc/hosts'
```

**Step 3:** Access CRM at [http://crm.localhost:8000/crm](http://crm.localhost:8000/crm):

| | |
|---|---|
| **URL** | `http://crm.localhost:8000/crm` |
| **Username** | `Administrator` |
| **Password** | `admin` |

> **🔒 Change the default password** after your first login.

**Docker Management:**

| Action | Command |
|--------|---------|
| Stop containers | `docker compose -f frappe-crm/docker-compose.yml stop` |
| Start containers | `docker compose -f frappe-crm/docker-compose.yml start` |
| View logs | `docker compose -f frappe-crm/docker-compose.yml logs frappe -f` |
| Tear down (keeps data) | `docker compose -f frappe-crm/docker-compose.yml down` |
| Tear down + delete data | `docker compose -f frappe-crm/docker-compose.yml down -v` |

#### Option B: Frappe Cloud (Managed Hosting)

Sign up at [frappecloud.com/crm/signup](https://frappecloud.com/crm/signup) for a fully managed instance.

#### CRM Configuration (Both Options)

Once your CRM instance is running, you need:

1. A custom DocType called **`TechnicianSchedule`** with these fields:

   | Field | Type |
   |-------|------|
   | `technician` | Data |
   | `customer_name` | Data |
   | `customer_phone` | Data |
   | `schedule_date` | Date |
   | `schedule_time` | Data |
   | `service_type` | Data |
   | `issue_description` | Small Text |
   | `status` | Data |

2. An API key + secret pair generated from **Settings → API Access** in your Frappe site
3. Update `server/.env` with your CRM connection details:
   ```env
   # For local Docker setup:
   FRAPPE_CRM_URL=http://crm.localhost:8000
   # For Frappe Cloud:
   # FRAPPE_CRM_URL=https://your-site.frappe.cloud
   FRAPPE_CRM_API_KEY=your_frappe_api_key
   FRAPPE_CRM_API_SECRET=your_frappe_api_secret
   ```

## Getting Started

### 1. Clone and Install

```bash
git clone git@github.com:ZyadHesham1/pipecat.git
cd pipecat-quickstart/server
uv sync
```

### 2. Configure Environment

```bash
cp .env.example .env
```

Edit `server/.env` and fill in **every** value:

```env
# Twilio (Telephony Transport)
TWILIO_ACCOUNT_SID=ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
TWILIO_AUTH_TOKEN=your_twilio_auth_token_here

# Deepgram (STT — Nova-3)
DEEPGRAM_API_KEY=your_deepgram_api_key

# OpenAI (LLM — GPT-4o-mini)
OPENAI_API_KEY=sk-proj-xxxxxxxxxxxxx

# Cartesia (TTS — Sonic)
CARTESIA_API_KEY=sk_car_xxxxxxxxxxxxx

# Redis (Distributed Locking)
REDIS_URL=redis://localhost:6379/0

# MCP Server (must match the port mcp_server.py runs on)
MCP_SERVER_URL=http://localhost:8000/sse

# Frappe CRM (Backend)
FRAPPE_CRM_API_KEY=your_frappe_api_key
FRAPPE_CRM_API_SECRET=your_frappe_api_secret
FRAPPE_CRM_URL=https://your-site.frappe.cloud
```

> **🔒 Never commit your `.env` file.** It is already in `.gitignore`.

### 3. Start All Services

Run these in **4 separate terminals**, in this order:

**Terminal 1 — Redis:**

```bash
redis-server
```

Verify: `redis-cli ping` → `PONG`

**Terminal 2 — MCP Server:**

```bash
cd server
uv run python mcp_server.py
```

You should see:
```
INFO     FRAPPE_CRM_URL = https://your-site.frappe.cloud
INFO     FRAPPE_CRM_API_KEY = xxxx****
INFO     REDIS_URL = redis://localhost:6379/0
```

**Terminal 3 — Main FastAPI Server:**

```bash
cd server
uv run uvicorn main:app --host 0.0.0.0 --port 7860
```

You should see:
```
INFO     Initializing persistent MCP Client connection to http://localhost:8000/sse...
INFO     MCP Client connected and ready.
INFO     Uvicorn running on http://0.0.0.0:7860
```

**Terminal 4 — ngrok Tunnel** *(for Twilio only)*:

```bash
cd server

# First time only — set your ngrok auth token:
./ngrok authtoken YOUR_NGROK_AUTH_TOKEN

# Start the tunnel:
./ngrok http 7860
```

Note the forwarding URL (e.g. `https://abc123.ngrok-free.app`).

> **📌 Startup order matters:** Redis → MCP Server → FastAPI → ngrok

### 4. Configure Twilio Webhook

1. Go to [Twilio Console](https://console.twilio.com/) → **Phone Numbers** → select your number
2. Under **Voice Configuration**:
   - **"A call comes in"** → **Webhook**
   - URL: `https://YOUR-NGROK-URL/twiml`
   - Method: **POST**
3. Save

Inbound calls to that number will now be routed to your local voice agent.

## Local Testing (Without Twilio)

Test the voice agent directly in your browser using your microphone — no phone calls or ngrok needed:

```bash
cd server
uv run bot_webrtc.py
```

This opens a browser-based WebRTC session via Daily/SmallWebRTC where you can talk to the agent directly.

> **Note:** Redis (Step 1) and the MCP Server (Step 2) must still be running. Only ngrok and Twilio are skipped.

## Project Structure

```
pipecat-quickstart/
├── server/
│   ├── main.py              # FastAPI entrypoint (Twilio webhook + WebSocket)
│   ├── mcp_server.py         # FastMCP server (booking tool + Redis locking)
│   ├── pipeline_builder.py   # Pipecat pipeline construction (STT → LLM → TTS)
│   ├── bot_webrtc.py         # WebRTC test runner (browser-based testing)
│   ├── config.py             # Pydantic-settings env validation (fail-fast)
│   ├── error_handlers.py     # Graceful fallback on pipeline errors
│   ├── pyproject.toml        # Python dependencies
│   ├── uv.lock               # Locked dependency versions
│   ├── .env.example           # Environment variables template
│   ├── .env                   # Your API keys (git-ignored)
│   ├── Dockerfile             # Container image for Pipecat Cloud
│   ├── pcc-deploy.toml        # Pipecat Cloud deployment config
│   └── ngrok                  # ngrok binary for tunneling
├── frappe-crm/
│   ├── docker-compose.yml     # Docker Compose for local Frappe CRM
│   └── init.sh                # Bench init + CRM install automation script
├── Pipecat HVAC AI Agent Guide.md  # Detailed architecture reference
├── .gitignore
└── README.md
```

## Deploying to Pipecat Cloud

This project is configured for deployment to [Pipecat Cloud](https://docs.pipecat.ai/getting-started/quickstart#step-2-deploy-to-production). See the [Pipecat Cloud Documentation](https://docs.pipecat.ai/deployment/pipecat-cloud/introduction) for details.

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| MCP Server won't start | Redis not running | Start `redis-server` first |
| Main server crashes on startup | Missing env var | Fill in every field in `.env` — the error message will say which one |
| Main server crashes on startup | MCP Server not running | Start `mcp_server.py` before `main.py` |
| Calls connect but no audio | Invalid Deepgram/OpenAI/Cartesia key | Verify API keys in `.env` |
| Booking returns CRITICAL_ERROR | Frappe CRM unreachable or misconfigured | Check `FRAPPE_CRM_URL`, API key/secret, and `TechnicianSchedule` DocType |
| ngrok connection refused | Auth token not set | Run `./ngrok authtoken YOUR_TOKEN` |
| Port conflict on 8000 | Another service on that port | Change FastMCP port and update `MCP_SERVER_URL` in `.env` |

## Learn More

- [Pipecat Documentation](https://docs.pipecat.ai/)
- [Pipecat GitHub](https://github.com/pipecat-ai/pipecat)
- [FastMCP Documentation](https://gofastmcp.com/)
- [Pipecat HVAC AI Agent Guide](Pipecat%20HVAC%20AI%20Agent%20Guide.md) — Detailed architecture deep-dive