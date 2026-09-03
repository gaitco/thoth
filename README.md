# Thoth Realtime

<p align="center"><img src="assets/icon.svg" width="96" alt="Thoth icon"></p>

Thoth is a self-hosted Pusher Protocol v7 WebSocket and HTTP server for Dart.
It works with the official `pusher-js` client and uses Maat for channel
authorization and signed server broadcasts.

```dart
final app = await Application.configure(basePath: Directory.current.path)
    .withConfig({'thoth': thoth})
    .withProviders([ThothServiceProvider.new])
    .create();

final sesh = Sesh(
  app,
  commands: [ThothStartCommand(), ThothPingCommand()],
);
```

```bash
maat thoth:start --host=0.0.0.0 --port=6001
maat thoth:ping --host=127.0.0.1 --port=6001
```

The first release supports public, private, and presence subscriptions,
server broadcasts, optional client events, ping/pong activity checks, channel
inspection, connection limits, and graceful shutdown. State is held in one
process; run one Thoth instance until a shared registry is added.

See the packaged [WebSockets guide](doc/websockets.md)
for configuration, `pusher-js`, Supervisor, nginx, TLS, and monitoring examples.
