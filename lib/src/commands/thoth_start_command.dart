import 'dart:async';
import 'dart:io';

import 'package:maat/maat.dart';

import '../thoth_server.dart';

class ThothStartCommand extends Command {
  ThothStartCommand({
    ThothServer Function(Application app)? server,
    Future<void> Function()? waitForShutdown,
  }) : _server = server ?? ((app) => app.make<ThothServer>()),
       _waitForShutdown = waitForShutdown ?? _waitForSignal;

  final ThothServer Function(Application app) _server;
  final Future<void> Function() _waitForShutdown;

  @override
  String get name => 'thoth:start';

  @override
  String get description => 'Start the Thoth WebSocket server';

  @override
  String get signature => '{--host=0.0.0.0} {--port=6001}';

  @override
  Future<int> handle() async {
    final port = int.tryParse(option('port')!);
    if (port == null || port < 0 || port > 65535) {
      error('The port must be between 0 and 65535.');
      return 1;
    }
    final server = _server(this.app);
    try {
      final bound = await server.start(host: option('host'), port: port);
      info('Thoth listening on http://${bound.address.host}:${bound.port}');
      await _waitForShutdown();
      return 0;
    } finally {
      await server.close();
    }
  }

  static Future<void> _waitForSignal() async {
    final done = Completer<void>();
    final subscriptions = [
      for (final signal in [ProcessSignal.sigint, ProcessSignal.sigterm])
        signal.watch().listen((_) {
          if (!done.isCompleted) done.complete();
        }),
    ];
    await done.future;
    for (final subscription in subscriptions) {
      await subscription.cancel();
    }
  }
}
