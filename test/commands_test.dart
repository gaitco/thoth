import 'dart:io';

import 'package:maat/maat.dart';
import 'package:test/test.dart';
import 'package:thoth/thoth.dart';

const _config = ThothConfig(
  appId: 'app-id',
  appKey: 'app-key',
  appSecret: 'app-secret',
);

class _RecordingServer extends ThothServer {
  _RecordingServer() : super(_config);

  String? host;
  int? port;
  HttpServer? bound;
  bool closed = false;

  @override
  Future<HttpServer> start({String? host, int? port}) async {
    this.host = host;
    this.port = port;
    return bound = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
  }

  @override
  Future<void> close({bool force = false}) async {
    closed = true;
    await bound?.close(force: force);
  }
}

Future<Application> _app([Map<String, dynamic> config = const {}]) =>
    Application.configure(
      basePath: Directory.current.path,
    ).withConfig(config).create();

void main() {
  tearDown(Application.reset);

  test('service provider binds configured Thoth services', () async {
    final app = await Application.configure(basePath: Directory.current.path)
        .withConfig({
          'thoth': {
            'host': '127.0.0.1',
            'port': 6123,
            'app': {
              'id': 'configured-id',
              'key': 'configured-key',
              'secret': 'configured-secret',
            },
            'client_events': true,
            'activity_timeout': 45,
            'pong_timeout': 8,
            'max_connections': 99,
          },
        })
        .withProviders([ThothServiceProvider.new])
        .create();
    addTearDown(app.shutdown);

    final config = app.make<ThothConfig>();
    expect(config.appId, 'configured-id');
    expect(config.host, '127.0.0.1');
    expect(config.port, 6123);
    expect(config.clientEvents, isTrue);
    expect(config.activityTimeout, const Duration(seconds: 45));
    expect(config.pongTimeout, const Duration(seconds: 8));
    expect(config.maxConnections, 99);
    expect(app.make<ChannelRegistry>(), same(app.make<ChannelRegistry>()));
    expect(app.make<ThothHandler>(), same(app.make<ThothHandler>()));
    expect(app.make<ThothServer>(), same(app.make<ThothServer>()));
  });

  test('service provider rejects missing app credentials', () async {
    expect(
      () => Application.configure(basePath: Directory.current.path)
          .withConfig({'thoth': <String, Object?>{}})
          .withProviders([ThothServiceProvider.new])
          .create(),
      throwsA(
        isA<StateError>().having(
          (error) => error.message,
          'message',
          'Thoth app id, key, and secret are required.',
        ),
      ),
    );
  });

  test('thoth:start forwards parsed host and port then closes', () async {
    final app = await _app();
    final server = _RecordingServer();
    final command = ThothStartCommand(
      server: (_) => server,
      waitForShutdown: () async {},
    );
    final exit = await Sesh(
      app,
      commands: [command],
    ).run(['thoth:start', '--host=127.0.0.2', '--port=6123']);

    expect(exit, 0);
    expect(server.host, '127.0.0.2');
    expect(server.port, 6123);
    expect(server.closed, isTrue);
  });

  test('thoth:ping succeeds only when health returns 200', () async {
    final health = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    health.listen((request) {
      request.response
        ..statusCode = request.uri.path == '/health' ? 200 : 404
        ..write('{"status":"ok"}')
        ..close();
    });
    addTearDown(() => health.close(force: true));
    final app = await _app();
    final output = StringBuffer();
    final exit = await Sesh(
      app,
      out: output,
      commands: [ThothPingCommand()],
    ).run(['thoth:ping', '--host=127.0.0.1', '--port=${health.port}']);

    expect(exit, 0);
    expect(output.toString(), contains('healthy'));
  });

  test('thoth:ping reports a concise error for a closed port', () async {
    final socket = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final port = socket.port;
    await socket.close();
    final app = await _app();
    final errors = StringBuffer();
    final exit = await Sesh(
      app,
      err: errors,
      commands: [ThothPingCommand()],
    ).run(['thoth:ping', '--host=127.0.0.1', '--port=$port']);

    expect(exit, isNonZero);
    expect(
      errors.toString(),
      'Thoth is unavailable at http://127.0.0.1:$port/health.\n',
    );
  });
}
