# WebsiteStats Backend (FastAPI) — Endpoints Only

This repository contains **only the FastAPI/Python endpoint code** for evaluation purposes.  
It **does not include** the full Dockerized infrastructure (Elasticsearch, Redis, MySQL, Frontend, Indexer, Kibana) nor any real data or credentials.

> In the actual development/production environment, all services run as **Docker containers** via `docker-compose` on a private server.  
> This public repository remains intentionally minimal for security reasons.

---

## Quick Start (Local, Mock / Degraded Mode)

Clone the repo and install dependencies:


python(python3) -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload


Health check:


curl http://127.0.0.1:8000/health
# {"ok": false, "elasticsearch": false, "redis": false}


This is expected without external services.  
The app is **runnable** for review, in mock/degraded mode.

---

## Environment Variables

Copy `.env.example` to `.env` and adjust values as needed.

Example `.env.example`:

.env
APP_ENV=dev
PORT=8000
TZ=Europe/Athens

DATABASE_URL=sqlite:///./local_data/troubleshoot.db
JWT_SECRET=change_me

REDIS_URL=redis://localhost:6379/0

GOOGLE_PROJECT_ID=project-id
GOOGLE_CALENDAR_ID=primary
GOOGLE_CREDENTIALS_FILE=/absolute/path/outside-repo/service-account.json


---

## Not Included in This Repository

- `docker-compose.yml` with all services (runs privately).  
- Real databases, images, or credentials (`.env`, service-account JSON).  
- Full frontend or Node indexer source.

**In the private setup**, the stack includes:

- `elasticsearch:8.14.x` (single-node, dev mode)  
- `redis:7` (appendonly)  
- `mysql:8` (timezone: Europe/Athens)  
- `kibana:8.14.x` (for ES debugging)  
- `frontend` (nginx) + `frontend-dev` (node)  
- `indexer` (Node.js 20) 
- `api` (this FastAPI backend)  

These are intentionally omitted from the public repo.

---

## Endpoints Overview (subset)

- **Health & KPI**  
  - `GET /health` – Health status  
  - `GET /kpi/overview` – KPI metrics  
  - `GET /kpi/quote` – Random quote  

- **Auth**  
  - `POST /auth/register` – Register user  
  - `POST /auth/login` – Login user  
  - `GET /auth/roles` – Retrieve roles  
  - `GET /auth/validate` – Validate token  

- **Companies**  
  - `GET /companies/active` – Active companies  
  - `GET /companies/{id}/full-details` – Company details  
  - `POST /companies/search` – Company search  

- **Sales**  
  - `GET /sales/leads` – List leads  
  - `POST /sales/leads` – Create lead  
  - `GET /sales/leads/{id}` – Lead details  
  - `PUT /sales/leads/{id}` – Update lead  
  - `GET /sales/leads/{id}/offers` – List offers  
  - `POST /sales/leads/{id}/offers` – Create offer  
  - `POST /sales/offers/{id}/send` – Send offer  
  - `POST /sales/offers/{id}/status` – Update offer status  

- **Installations**  
  - `GET /installations/jobs` – List jobs  
  - `GET /installations/recent` – Recent installations  
  - `GET /installations/undone-jobs` – Undone jobs  
  - `POST /installations/update-dates` – Update installation dates  
  - `PUT /installations/{company_id}/update` – Update full installation  
  - `GET /technicians` – List technicians  

- **Notifications**  
  - `GET /notifications` – List notifications  
  - `GET /notifications/unread-count` – Unread count  
  - `POST /notifications/mark-read` – Mark all as read  
  - `POST /notifications/{id}/mark-read` – Mark one as read  

- **Troubleshooting Docs**  
  - `GET /docs/categories` / `POST /docs/categories`  
  - `GET /docs/subcategories/{id}` / `POST /docs/subcategories`  
  - `GET /docs/subsubcategories/{id}` / `POST /docs/subsubcategories`  
  - `GET /docs/steps/{id}` / `POST /docs/steps`  

- **Search**  
  - `GET /search/results`  
  - `POST /search/advanced-results`  
  - `POST /search/latest-tickets`  
  - `POST /search/options`  

- **Sheet Data**  
  - `GET /api/sheet-data`  
  - `POST /api/update-sheet`  

