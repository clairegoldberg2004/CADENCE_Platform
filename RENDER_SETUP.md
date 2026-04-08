# Render Deployment Guide

## Quick Setup Steps

### Option 1: Deploy Backend Only (Recommended)

1. **Go to Render Dashboard**: https://dashboard.render.com
2. **Click "New +"** → **"Web Service"**
3. **Connect your GitHub repository** (private repos are supported on free tier)
4. **Configure the service**:
   - **Name**: `interdependency-model-api` (or any name you prefer)
   - **Region**: Choose closest to you
   - **Branch**: `main` (or your default branch)
   - **Root Directory**: Leave empty (root)
   - **Runtime**: `Python 3`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `gunicorn app:app`
   - **Plan**: `Free`

5. **Click "Create Web Service"**
6. **Wait for deployment** (first build takes 5-10 minutes)
7. **Copy your service URL** (e.g., `https://interdependency-model-api.onrender.com`)

### Option 2: Deploy Both Frontend and Backend

1. **Deploy Backend** (follow Option 1 steps above)
2. **Deploy Frontend**:
   - Click "New +" → **"Static Site"**
   - Connect same GitHub repo
   - **Name**: `interdependency-model-frontend`
   - **Build Command**: `cd frontend && npm install && npm run build`
   - **Publish Directory**: `frontend/build`
   - **Environment Variable**: 
     - Key: `REACT_APP_API_URL`
     - Value: `https://your-backend-url.onrender.com` (from step 1)
   - **Plan**: `Free`
   - Click "Create Static Site"

### Option 3: Use render.yaml (Easiest)

1. **Go to Render Dashboard**
2. **Click "New +"** → **"Blueprint"**
3. **Connect your GitHub repository**
4. **Select the `render.yaml` file** (it's in your repo root)
5. **Render will automatically create both services**
6. **Update the frontend environment variable** with your backend URL after it deploys

## Important Notes

### CSV File Location
- Make sure `capacity_delta_subset(in).csv` is in your repository root
- Render will have access to it at runtime

### Python version (important)
- New Render web services default to **Python 3.14**. Older pinned packages (e.g. pandas 2.1.x) may **not have wheels** for 3.14 and will try to compile from source, which often fails.
- This repo pins Python with a **`.python-version`** file at the repo root (`3.11.7`). Render reads this automatically ([docs](https://render.com/docs/python-version)).
- Alternatively, in the Render dashboard → your service → **Environment**, set **`PYTHON_VERSION`** to **`3.11.7`** (fully qualified). That overrides the default.

### Other environment variables
- The CSV path is hardcoded to `capacity_delta_subset(in).csv` in the repo root; no extra env vars required for that.

### Free Tier Limitations
- Services spin down after 15 minutes of inactivity
- First request after spin-down takes ~30 seconds (cold start)
- No timeout limits (unlike Vercel's 10-second limit)
- Perfect for your model which takes 30-60 seconds

### Updating Your Deployment
- Push to GitHub → Render automatically redeploys
- Or manually trigger redeploy from Render dashboard

## Troubleshooting

### Backend won't start
- Check logs in Render dashboard
- Ensure `gunicorn` is in requirements.txt
- Verify `app.py` has the Flask app instance named `app`

### Frontend can't connect to backend
- Check `REACT_APP_API_URL` environment variable
- Ensure backend URL includes `https://` and no trailing slash
- Check CORS settings in `app.py` (should allow your frontend domain)

### CSV file not found
- Verify CSV is committed to GitHub
- Check file path matches exactly: `capacity_delta_subset(in).csv`
- File should be in repository root

## Testing Locally Before Deploying

```bash
# Install gunicorn
pip install gunicorn

# Test backend locally
gunicorn app:app --bind 0.0.0.0:5000

# In another terminal, test frontend build
cd frontend
npm install
npm run build
```
