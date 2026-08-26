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
ARTICLE_PUBLISH_ENABLED=false
GIF_UPLOAD_TOKEN=<random-upload-token>
# 服务器不要配置 OPEN_PLATFORM_APPID、OPEN_PLATFORM_APP_SECRET 或发布令牌。
```

Generate the upload token once on the server and keep the same value in the
local upload command (do not commit it):

```bash
openssl rand -hex 32
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
    client_max_body_size 60m;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
}

location / {
    proxy_pass http://127.0.0.1:8899;
    client_max_body_size 60m;
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

Authorization belongs to the local publishing process. After configuring and
starting the local Dashboard, inspect:

```bash
curl -s http://127.0.0.1:8899/api/article-publish/status
```

If the response says the local publisher is not authorized, open
`http://127.0.0.1:8899/api/open-platform/oauth/start` and complete OAuth. The
configured redirect URI must return to this local publishing process (or to an
existing central token service used by it). The server that stores GIFs does
not need an Open Platform token. Normal code updates do not require a new
authorization unless the refresh token expires or the AppID changes.

## 4. Local publishing with remote GIF storage

The intended production flow is **local machine generates and publishes**;
the server only stores GIF bytes and serves their HTTPS URL. Configure these
additional values in the local `.env` (the server must not set
`GIF_UPLOAD_ENDPOINT`):

```bash
GIF_UPLOAD_ENDPOINT=https://matchgif.aisportsapp.com/api/article-publish/upload
GIF_UPLOAD_TOKEN=<the-same-token-configured-on-the-server>
GIF_UPLOAD_TIMEOUT_SECONDS=120
ARTICLE_PUBLISH_ENABLED=true
GIF_PUBLIC_ORIGIN=https://matchgif.aisportsapp.com
OPEN_PLATFORM_APPID=<appid>
OPEN_PLATFORM_APP_SECRET=<app-secret>
OPEN_PLATFORM_API_NAME=admin-archive-createarticle
OPEN_PLATFORM_REDIRECT_URI=<本地发布端可访问的 OAuth 回调地址>
OPEN_PLATFORM_TOKEN_PATH=./data/open-platform-token.json
```

When a local Dashboard **发布** button is clicked, the local process:

1. validates the generated GIF and uploads it to the server with the token;
2. receives the content ID and public `https://matchgif.aisportsapp.com/publish-gifs/...` URL;
3. checks that URL is reachable; and
4. calls the Dongqiudi Open Platform from the local process using that URL.

The server upload route never calls the Open Platform. Do not click a publish
button on the server Dashboard for this workflow; use the local Dashboard at
`http://127.0.0.1:8899`. If upload fails, the local response identifies the
connection, token, GIF format/size, or server-storage problem, and no article
request is sent. If publication fails after upload, retrying reuses the saved
local upload mapping instead of uploading the same bytes again.

## 5. Release Check

Generate one default GIF, click **发布**, and verify all four stages shown by
the Dashboard: GIF validation, permanent URL check, Open Platform publication,
and returned article ID. A repeated click/retry uses the saved idempotency
record and must not create another article.
