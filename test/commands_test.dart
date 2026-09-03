import 'dart:async';
import 'dart:io';

import 'package:maat/maat.dart';
import 'package:test/test.dart';
import 'package:thoth_realtime/thoth_realtime.dart';
import 'package:web_socket_channel/io.dart';

const _config = ThothConfig(
  appId: 'app-id',
  appKey: 'app-key',
  appSecret: 'app-secret',
);

class _RecordingServer extends ThothServer {
  _RecordingServer([super.config = _config]);

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

class _ObservableServer extends ThothServer {
  _ObservableServer() : super(_config);

  final started = Completer<HttpServer>();

  @override
  Future<HttpServer> start({String? host, int? port}) async {
    final server = await super.start(host: host, port: port);
    started.complete(server);
    return server;
  }
}

Future<Application> _app([Map<String, dynamic> config = const {}]) =>
    Application.configure(
      basePath: Directory.current.path,
    ).withConfig(config).create();

Future<Application> _thothApp({required String host, required int port}) =>
    Application.configure(basePath: Directory.current.path)
        .withConfig({
          'thoth': {
            'host': host,
            'port': port,
            'app': {
              'id': 'configured-id',
              'key': 'configured-key',
              'secret': 'configured-secret',
            },
          },
        })
        .withProviders([ThothServiceProvider.new])
        .create();

Future<int> _freePort() async {
  final socket = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
  final port = socket.port;
  await socket.close();
  return port;
}

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

  test('server refuses to start with missing app credentials', () async {
    final app = await Application.configure(basePath: Directory.current.path)
        .withConfig({'thoth': <String, Object?>{}})
        .withProviders([ThothServiceProvider.new])
        .create();

    expect(
      () => app.make<ThothServer>().start(port: 0),
      throwsA(
        isA<StateError>().having(
          (error) => error.message,
          'message',
          'Thoth app id, key, and secret are required.',
        ),
      ),
    );
  });

  test('thoth:start uses configured host and port without options', () async {
    final port = await _freePort();
    final app = await _thothApp(host: '127.0.0.1', port: port);
    final output = StringBuffer();
    final exit = await Sesh(
      app,
      out: output,
      commands: [ThothStartCommand(waitForShutdown: () async {})],
    ).run(['thoth:start']);

    expect(exit, 0);
    expect(output.toString(), 'Thoth listening on http://127.0.0.1:$port\n');
  });

  test('thoth:start options override configured host and port', () async {
    final configuredPort = await _freePort();
    final overridePort = await _freePort();
    final app = await _thothApp(host: '127.0.0.2', port: configuredPort);
    final server = _RecordingServer(app.make<ThothConfig>());
    final command = ThothStartCommand(
      server: (_) => server,
      waitForShutdown: () async {},
    );
    final exit = await Sesh(
      app,
      commands: [command],
    ).run(['thoth:start', '--host=127.0.0.1', '--port=$overridePort']);

    expect(exit, 0);
    expect(server.host, '127.0.0.1');
    expect(server.port, overridePort);
    expect(server.closed, isTrue);
  });

  test(
    'thoth:start closes an active WebSocket and releases its port',
    () async {
      final app = await _app();
      final server = _ObservableServer();
      final shutdown = Completer<void>();
      final running = Sesh(
        app,
        commands: [
          ThothStartCommand(
            server: (_) => server,
            waitForShutdown: () => shutdown.future,
          ),
        ],
      ).run(['thoth:start', '--host=127.0.0.1', '--port=0']);
      final bound = await server.started.future;
      final port = bound.port;
      final socket = IOWebSocketChannel.connect(
        'ws://127.0.0.1:$port/app/app-key'
        '?protocol=7&client=thoth-test&version=1.0',
      );
      await socket.ready;
      final messages = StreamIterator<Object?>(socket.stream);
      expect(await messages.moveNext(), isTrue);
      addTearDown(() async {
        if (!shutdown.isCompleted) shutdown.complete();
        await socket.sink.close();
        await running;
      });

      shutdown.complete();

      expect(await running.timeout(const Duration(seconds: 1)), 0);
      expect(await messages.moveNext(), isFalse);
      expect(socket.closeCode, 1001);
      final rebound = await HttpServer.bind('127.0.0.1', port);
      await rebound.close(force: true);
    },
  );

  test('thoth:ping uses configured host and port without options', () async {
    final health = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    health.listen((request) {
      request.response
        ..statusCode = request.uri.path == '/health' ? 200 : 404
        ..write('{"status":"ok"}')
        ..close();
    });
    addTearDown(() => health.close(force: true));
    final app = await _thothApp(host: '127.0.0.1', port: health.port);
    final output = StringBuffer();
    final exit = await Sesh(
      app,
      out: output,
      commands: [ThothPingCommand()],
    ).run(['thoth:ping']);

    expect(exit, 0);
    expect(
      output.toString(),
      'Thoth is healthy at http://127.0.0.1:${health.port}/health.\n',
    );
  });

  test('thoth:ping options override configured host and port', () async {
    final health = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    health.listen((request) {
      request.response
        ..statusCode = request.uri.path == '/health' ? 200 : 404
        ..write('{"status":"ok"}')
        ..close();
    });
    addTearDown(() => health.close(force: true));
    final app = await _thothApp(host: '127.0.0.2', port: await _freePort());
    final output = StringBuffer();
    final exit = await Sesh(
      app,
      out: output,
      commands: [ThothPingCommand()],
    ).run(['thoth:ping', '--host=127.0.0.1', '--port=${health.port}']);

    expect(exit, 0);
    expect(
      output.toString(),
      'Thoth is healthy at http://127.0.0.1:${health.port}/health.\n',
    );
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
