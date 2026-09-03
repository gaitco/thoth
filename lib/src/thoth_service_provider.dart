import 'package:maat/maat.dart';

import 'channel_registry.dart';
import 'config.dart';
import 'thoth_handler.dart';
import 'thoth_server.dart';

class ThothServiceProvider extends ServiceProvider {
  ThothServiceProvider(super.app);

  ThothServer? _server;

  @override
  void register() {
    final settings = Map<String, dynamic>.from(
      (this.app.config.get('thoth') as Map?) ?? const {},
    );
    final credentials = Map<String, dynamic>.from(
      (settings['app'] as Map?) ?? const {},
    );
    final id = (credentials['id'] ?? '') as String;
    final key = (credentials['key'] ?? '') as String;
    final secret = (credentials['secret'] ?? '') as String;
    if (id.isEmpty || key.isEmpty || secret.isEmpty) {
      throw StateError('Thoth app id, key, and secret are required.');
    }

    this.app.instance<ThothConfig>(
      ThothConfig(
        appId: id,
        appKey: key,
        appSecret: secret,
        host: (settings['host'] ?? '0.0.0.0') as String,
        port: (settings['port'] ?? 6001) as int,
        clientEvents: (settings['client_events'] ?? false) as bool,
        activityTimeout: Duration(
          seconds: (settings['activity_timeout'] ?? 120) as int,
        ),
        pongTimeout: Duration(seconds: (settings['pong_timeout'] ?? 30) as int),
        maxConnections: (settings['max_connections'] ?? 0) as int,
      ),
    );
    this.app.singleton<ChannelRegistry>((_) => InMemoryChannelRegistry());
    this.app.singleton<ThothHandler>(
      (_) => ThothHandler(
        this.app.make<ThothConfig>(),
        registry: this.app.make<ChannelRegistry>(),
      ),
    );
    this.app.singleton<ThothServer>(
      (_) => _server = ThothServer(
        this.app.make<ThothConfig>(),
        handler: this.app.make<ThothHandler>(),
      ),
    );
  }

  @override
  Future<void> shutdown() => _server?.close() ?? Future.value();
}
