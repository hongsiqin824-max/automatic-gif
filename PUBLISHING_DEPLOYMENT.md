# GIF Article Publishing Deployment

The Dashboard may keep listening on plain HTTP at `127.0.0.1:8899`. HTTPS is
terminated by Nginx. Formal articles always use
`https://matchgif.aisportsapp.com/publish-gifs/<sha256>.gif`; publication stops
before calling the Open Platform if that URL is not publicly readable.

## 1. Persistent Storage

Create storage owned by the same Linux user that runs `dashboard_server.py`:

```bash
sudo install -d -m 0755 -o <service-user> -g <service-group> \
  /var/lib/automatic-gif/published-gifs
sudo install -d -m 0700 -o <service-user> -g <service-group> \
  /var/lib/automatic-gif/private
```

Add these values to the server `.env` (never commit the real secret):

```bash
GIF_PUBLIC_ORIGIN=https://matchgif.aisportsapp.com
ARTICLE_PUBLISH_GIF_DIR=/var/lib/automatic-gif/published-gifs
ARTICLE_PUBLISH_DB_PATH=/var/lib/automatic-gif/private/article-publish.sqlite3
ARTICLE_PUBLISH_VERIFY_PUBLIC_URL=true
OPEN_PLATFORM_APPID=<appid>
OPEN_PLATFORM_APP_SECRET=<app-secret>
OPEN_PLATFORM_API_NAME=admin-archive-createarticle
OPEN_PLATFORM_REDIRECT_URI=https://matchgif.aisportsapp.com/api/open-platform/oauth/callback
OPEN_PLATFORM_TOKEN_PATH=/var/lib/automatic-gif/private/open-platform-token.json
```

Keep both directories outside versioned release folders. Code deployment must
not delete them; otherwise old article GIF URLs and duplicate-publish records
will be lost.

## 2. Nginx and HTTPS

The domain must resolve to the server, ports 80/443 must be open, and Nginx
must proxy both the Dashboard and permanent GIF route:

```nginx
location /publish-gifs/ {
    proxy_pass http://127.0.0.1:8899;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
}

location / {
    proxy_pass http://127.0.0.1:8899;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
}
```

Validate and obtain a certificate on Ubuntu/Debian:

```bash
sudo nginx -t
sudo apt-get install -y certbot python3-certbot-nginx
sudo certbot --nginx -d matchgif.aisportsapp.com --redirect
curl -I https://matchgif.aisportsapp.com/api/health
```

The health request should return HTTP 200. An individual `/publish-gifs/...`
URL returns 404 until the first GIF has been prepared; that is expected.

## 3. First Authorization

Restart the Dashboard after adding configuration, then inspect:

```bash
curl -s https://matchgif.aisportsapp.com/api/article-publish/status
```

Open `https://matchgif.aisportsapp.com/api/open-platform/oauth/start` once in
a browser and complete the authorization. The callback stores a refresh token
in the configured persistent token path. Normal code updates do not require a
new authorization unless the refresh token expires or the AppID changes.

## 4. Release Check

Generate one default GIF, click **发布**, and verify all four stages shown by
the Dashboard: GIF validation, permanent URL check, Open Platform publication,
and returned article ID. A repeated click/retry uses the saved idempotency
record and must not create another article.
