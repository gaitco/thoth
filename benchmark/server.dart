import 'dart:async';
import 'dart:io';

import 'package:thoth_realtime/thoth_realtime.dart';

Future<void> main() async {
  final environment = Platform.environment;
  final server = ThothServer(
    ThothConfig(
      appId: environment['PUSHER_APP_ID'] ?? 'benchmark',
      appKey: environment['PUSHER_APP_KEY'] ?? 'benchmark-key',
      appSecret: environment['PUSHER_APP_SECRET'] ?? 'benchmark-secret',
      host: environment['PUSHER_HOST'] ?? '127.0.0.1',
      port: int.parse(environment['PUSHER_PORT'] ?? '6001'),
    ),
  );
  final listener = await server.start();
  stdout.writeln('Thoth benchmark server listening on ${listener.port}');

  await Future.any([
    ProcessSignal.sigint.watch().first,
    ProcessSignal.sigterm.watch().first,
  ]);
  await server.close();
}
