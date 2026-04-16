# ⚡ Kakashi Checker Bot v3.0

**@ReallyTsugikuni**

## Architecture
```
Bot (Docker) ──HTTPS──▶ Engine API (Vercel)
```

## Setup
1. Deploy `vercel-engine/` to Vercel
2. Set `ENGINE_SECRET` on Vercel env
3. Set `.env` → `VERCEL_URI` + `ENGINE_SECRET` + `TELEGRAM_BOT_TOKEN`
4. `docker-compose up -d`
5. `/id` → get Chat ID → add to `SUPER_ADMIN_IDS` → restart

## Commands
| Cmd | Who | What |
|---|---|---|
| /start | All | Menu |
| /help | All | Guide |
| /id | All | Chat ID |
| /ping | All | Latency |
| /admin | Admin | Panel |
| /export | Admin | Hits .zip |
| /addadmin | Super | Add admin |
| /removeadmin | Super | Remove admin |
| /listadmins | Admin | List admins |

---
*@ReallyTsugikuni*
