# GrimReaperBot

A Discord bot for **Star Citizen PvP kill-tracking** built for **Black Widow Company**.  
It integrates with the external kill-tracker API, reports real-time kills to Discord, posts weekly tallies, and announces streaks + milestones.

---

## 🚀 Features
- Real-time kill feed → posts to `#sc-public`
- Weekly tally → posts to `#sc-announcements`
- Kill streak + milestone announcements
- Containerized for easy deployment (Docker + Compose)
- Auto-updates with Watchtower

---

## 🛠 Deployment

### 1. Clone repo
```bash
git clone https://github.com/BWC-BigSarge/grimreaperbot.git
cd grimreaperbot
