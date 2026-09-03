import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:test/test.dart';
import 'package:thoth_realtime/thoth_realtime.dart';
import 'package:web_socket_channel/io.dart';

class _BlockingDisconnectRegistry extends InMemoryChannelRegistry {
  final entered = Completer<void>();
  final release = Completer<void>();

  @override
  Future<void> disconnect(SocketConnection connection) async {
    entered.complete();
    await release.future;
    await super.disconnect(connection);
  }
}

void main() {
  for (final testCase in [
    (
      field: 'app ID',
      config: const ThothConfig(
        appId: '',
        appKey: 'app-key',
        appSecret: 'app-secret',
      ),
    ),
    (
      field: 'app key',
      config: const ThothConfig(
        appId: 'app-id',
        appKey: '',
        appSecret: 'app-secret',
      ),
    ),
    (
      field: 'app secret',
      config: const ThothConfig(
        appId: 'app-id',
        appKey: 'app-key',
        appSecret: '',
      ),
    ),
  ]) {
    test('refuses to start with a missing ${testCase.field}', () async {
      final thoth = ThothServer(testCase.config);
      addTearDown(() => thoth.close(force: true));

      await expectLater(
        thoth.start(host: '127.0.0.1', port: 0),
        throwsA(
          isA<StateError>().having(
            (error) => error.message,
            'message',
            'Thoth app id, key, and secret are required.',
          ),
        ),
      );
    });
  }

  test('starts on an ephemeral port and releases it on close', () async {
    final thoth = ThothServer(
      const ThothConfig(
        appId: 'app-id',
        appKey: 'app-key',
        appSecret: 'app-secret',
      ),
    );
    final server = await thoth.start(host: '127.0.0.1', port: 0);
    final port = server.port;
    final client = HttpClient();
    final response = await (await client.getUrl(
      Uri.parse('http://127.0.0.1:$port/health'),
    )).close();
    expect(response.statusCode, 200);
    expect(await utf8.decoder.bind(response).join(), '{"status":"ok"}');
    client.close();

    await thoth.close();
    await thoth.close();
    final rebound = await HttpServer.bind('127.0.0.1', port);
    await rebound.close(force: true);
  });

  test('forces shutdown after the connection cleanup grace period', () async {
    const config = ThothConfig(
      appId: 'app-id',
      appKey: 'app-key',
      appSecret: 'app-secret',
    );
    final registry = _BlockingDisconnectRegistry();
    final thoth = ThothServer(
      config,
      handler: ThothHandler(config, registry: registry),
      shutdownTimeout: const Duration(milliseconds: 100),
    );
    final server = await thoth.start(host: '127.0.0.1', port: 0);
    final port = server.port;
    final socket = IOWebSocketChannel.connect(
      'ws://127.0.0.1:$port/app/app-key'
      '?protocol=7&client=thoth-test&version=1.0',
    );
    await socket.ready;
    final messages = StreamIterator<Object?>(socket.stream);
    expect(await messages.moveNext(), isTrue);
    addTearDown(() async {
      if (!registry.release.isCompleted) registry.release.complete();
      await socket.sink.close();
    });

    final closing = thoth.close();
    await registry.entered.future.timeout(const Duration(seconds: 1));
    var completed = false;
    unawaited(closing.then((_) => completed = true));
    await Future<void>.delayed(Duration.zero);
    expect(completed, isFalse);
    await closing.timeout(const Duration(seconds: 1));

    expect(await messages.moveNext(), isFalse);
    expect(socket.closeCode, 1001);
    final rebound = await HttpServer.bind('127.0.0.1', port);
    await rebound.close(force: true);
    registry.release.complete();
  });
}
