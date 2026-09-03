import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:maat/maat.dart';
import 'package:test/test.dart';
import 'package:thoth_realtime/thoth_realtime.dart';

class _User implements Authenticatable {
  const _User();

  @override
  Object get authIdentifier => 7;

  @override
  String get authPassword => '';
}

class _Guard implements Guard {
  @override
  Future<Authenticatable?> user(Request request) async => const _User();
}

bool _hasCommand(String command) {
  try {
    return Process.runSync(command, ['--version']).exitCode == 0;
  } on ProcessException {
    return false;
  }
}

void main() {
  final unavailable = [
    if (!_hasCommand('node')) 'node',
    if (!_hasCommand('npm')) 'npm',
  ];

  test(
    'official pusher-js connects, authorizes, and receives a publish',
    () async {
      const appId = 'compat-id';
      const appKey = 'compat-key';
      const appSecret = 'compat-secret';
      Auth.extend('compat', _Guard.new);
      final app =
          await Application.configure(
                basePath: Directory.current.path,
                environment: const {},
              )
              .withConfig({
                'auth': {
                  'defaults': {'guard': 'compat'},
                },
                'broadcasting': {
                  'default': 'pusher',
                  'connections': {
                    'pusher': {
                      'driver': 'pusher',
                      'app_id': appId,
                      'key': appKey,
                      'secret': appSecret,
                      'host': '127.0.0.1',
                      'port': 0,
                      'scheme': 'http',
                    },
                  },
                },
              })
              .withProviders([BroadcastServiceProvider.new])
              .create();
      Broadcast.channel('compat-private', (_, _) => true);
      Broadcast.channel(
        'compat-presence',
        (_, _) => <String, Object?>{'name': 'Compatibility User'},
      );
      final authServer = await app.serve(host: '127.0.0.1', port: 0);
      final thoth = ThothServer(
        const ThothConfig(appId: appId, appKey: appKey, appSecret: appSecret),
      );
      final socketServer = await thoth.start(host: '127.0.0.1', port: 0);
      addTearDown(() async {
        await thoth.close(force: true);
        await app.shutdown(force: true);
        Application.reset();
      });

      final tool = Directory('tool/pusher-js');
      if (!Directory('${tool.path}/node_modules/pusher-js').existsSync()) {
        final install = await Process.run('npm', [
          'ci',
        ], workingDirectory: tool.path);
        expect(
          install.exitCode,
          0,
          reason: '${install.stdout}\n${install.stderr}',
        );
      }
      final process = await Process.start(
        'node',
        ['compat.mjs'],
        workingDirectory: tool.path,
        environment: {
          ...Platform.environment,
          'THOTH_PORT': '${socketServer.port}',
          'AUTH_PORT': '${authServer.port}',
          'PUSHER_APP_KEY': appKey,
        },
      );
      final output = <String>[];
      final errors = StringBuffer();
      final ready = Completer<void>();
      final stdoutDone = process.stdout
          .transform(utf8.decoder)
          .transform(const LineSplitter())
          .listen((line) {
            output.add(line);
            if (line == 'READY' && !ready.isCompleted) ready.complete();
          })
          .asFuture<void>();
      final stderrDone = process.stderr
          .transform(utf8.decoder)
          .listen(errors.write)
          .asFuture<void>();
      unawaited(
        process.exitCode.then((code) {
          if (!ready.isCompleted) {
            ready.completeError(StateError('pusher-js exited with $code'));
          }
        }),
      );

      await ready.future.timeout(const Duration(seconds: 10));
      await PusherBroadcaster(
        appId: appId,
        key: appKey,
        secret: appSecret,
        host: '127.0.0.1',
        port: socketServer.port,
        scheme: 'http',
      ).broadcast(
        const [
          'compat-public',
          'private-compat-private',
          'presence-compat-presence',
        ],
        'CompatEvent',
        const {'id': 1},
      );

      final exit = await process.exitCode.timeout(const Duration(seconds: 10));
      await Future.wait([stdoutDone, stderrDone]);
      expect(exit, 0, reason: errors.toString());
      expect(jsonDecode(output.last), {
        'public': true,
        'private': true,
        'presence': true,
        'member': '7',
      });
    },
    skip: unavailable.isEmpty
        ? false
        : 'Official compatibility test requires ${unavailable.join(' and ')}.',
    timeout: const Timeout(Duration(seconds: 30)),
  );
}
