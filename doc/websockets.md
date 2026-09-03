# Running Thoth

Thoth is a standalone Pusher Protocol v7 server. Maat applications authorize
private channels and publish signed events; browser and Node clients connect
with the official `pusher-js` package.

Install the package with `dart pub add thoth_realtime`, then register the
broadcasting and Thoth providers and expose the two commands:

## Configure the application

```dart
import 'package:maat/maat.dart';
import 'package:thoth_realtime/thoth_realtime.dart';

final app = await Application.configure(basePath: Directory.current.path)
    .withConfig({'broadcasting': broadcasting, 'thoth': thoth})
    .withProviders([
      BroadcastServiceProvider.new,
      ThothServiceProvider.new,
    ])
    .create();

final sesh = Sesh(
  app,
  commands: [ThothStartCommand(), ThothPingCommand()],
);
```

Use the same credentials for Maat's `pusher` connection and Thoth:

```dart
Map<String, dynamic> get thoth => {
  'host': env('THOTH_HOST', '0.0.0.0'),
  'port': envInt('THOTH_PORT', 6001),
  'app': {
    'id': env('PUSHER_APP_ID', ''),
    'key': env('PUSHER_APP_KEY', ''),
    'secret': env('PUSHER_APP_SECRET', ''),
  },
  'client_events': envBool('THOTH_CLIENT_EVENTS', false),
  'activity_timeout': envInt('THOTH_ACTIVITY_TIMEOUT', 120),
  'pong_timeout': envInt('THOTH_PONG_TIMEOUT', 30),
  'max_connections': envInt('THOTH_MAX_CONNECTIONS', 0),
};
```

| Key | Default | Purpose |
| --- | --- | --- |
| `host` | `0.0.0.0` | Address used by the standalone server. |
| `port` | `6001` | HTTP and WebSocket port. |
| `app.id` | required | Application ID used by the signed HTTP API. |
| `app.key` | required | Public key used by clients. |
| `app.secret` | required | Secret used only to verify signatures. |
| `client_events` | `false` | Allows `client-*` events on private and presence channels. |
| `activity_timeout` | `120` | Idle seconds before `pusher:ping`. |
| `pong_timeout` | `30` | Seconds allowed for `pusher:pong`. |
| `max_connections` | `0` | Process connection limit; `0` means unlimited. |

Thoth refuses to start when the ID, key, or secret is empty. Keep the secret
in environment variables and never send it to a browser.

## Run and supervise Thoth

```bash
maat thoth:start --host=0.0.0.0 --port=6001
maat thoth:ping --host=127.0.0.1 --port=6001
```

`thoth:start` handles `SIGINT` and `SIGTERM`. Shutdown stops accepting new
connections, closes active WebSockets with code `1001`, and gives connection
cleanup `5` seconds before forcing the underlying server closed. Internal
connection failures are logged with the app secret redacted and close only the
affected socket with code `1011`.

A Supervisor program can keep the compiled command alive:

```ini
[program:thoth]
directory=/srv/example
command=/srv/example/bin/maat thoth:start --host=127.0.0.1 --port=6001
autostart=true
autorestart=true
stopasgroup=true
killasgroup=true
stopsignal=TERM
stdout_logfile=/var/log/thoth.log
stderr_logfile=/var/log/thoth-error.log
environment=APP_ENV="production"
```

## Connect with pusher-js

```js
const pusher = new Pusher('your-app-key', {
  cluster: 'mt1',
  wsHost: 'realtime.example.com',
  wssPort: 443,
  forceTLS: true,
  enabledTransports: ['ws', 'wss'],
  channelAuthorization: {
    endpoint: 'https://app.example.com/broadcasting/auth',
  },
});

pusher.subscribe('tasks').bind('TaskChanged', ({ id }) => {
  console.log(`Task ${id} changed`);
});
```

Private and presence channels use Maat's `/broadcasting/auth` endpoint. Define
authorization rules with `Broadcast.channel(...)`; the callback returns `true`
for a private channel or a member-data map for a presence channel.

## Terminate TLS with nginx

Proxy WebSocket upgrades to a private Thoth port:

```nginx
server {
    listen 443 ssl http2;
    server_name realtime.example.com;

    ssl_certificate /etc/letsencrypt/live/realtime.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/realtime.example.com/privkey.pem;

    location /app/ {
        proxy_pass http://127.0.0.1:6001;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_read_timeout 180s;
    }

    location /apps/ {
        proxy_pass http://127.0.0.1:6001;
        proxy_set_header Host $host;
    }

    location = /health {
        proxy_pass http://127.0.0.1:6001/health;
    }
}
```

Clients use `wss://`; nginx owns certificates and TLS renewal while Thoth
receives plain HTTP on the private interface.

## Monitor and inspect

| Route | Authentication | Purpose |
| --- | --- | --- |
| `GET /health` | none | Liveness check returning `{"status":"ok"}`. |
| `POST /apps/{id}/events` | Pusher HTTP signature | Publish an event. |
| `GET /apps/{id}/channels` | Pusher HTTP signature | List occupied channels. |
| `GET /apps/{id}/channels/{name}` | Pusher HTTP signature | Read occupancy and subscription counts. |
| `GET /apps/{id}/channels/{name}/users` | Pusher HTTP signature | List presence member IDs. |

Expose `/health` to the load balancer. Keep `/apps` behind the application or
a private network; those routes require a request signed with the app secret.

## Single-process limit

The registry is in memory. One Thoth process gives every connection the same
channel and presence state and is the supported deployment for `0.1.0`. Two
processes would each see only their own subscribers, so broadcasts and member
lists would be incomplete.

`ChannelRegistry` is the future shared-state seam. Add a distributed
implementation only when horizontal scaling is needed; until then, give the
single process more resources and set `max_connections` to a measured limit.
