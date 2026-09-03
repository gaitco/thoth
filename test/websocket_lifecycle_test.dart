import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:maat/maat.dart' show Log, PusherSigner;
import 'package:shelf/shelf_io.dart' as shelf_io;
import 'package:test/test.dart';
import 'package:thoth/thoth.dart';
import 'package:web_socket_channel/io.dart';
import 'package:web_socket_channel/web_socket_channel.dart' show WebSocketSink;

Map<String, Object?> decodeFrame(Object? raw) {
  final frame = Map<String, Object?>.from(jsonDecode(raw! as String) as Map);
  frame['data'] = jsonDecode(frame['data']! as String);
  return frame;
}

class LifecycleSocket {
  LifecycleSocket._(this.channel, this.messages, this.id);

  final IOWebSocketChannel channel;
  final StreamIterator<Object?> messages;
  final String id;

  static Future<LifecycleSocket> connect(int port) async {
    final channel = IOWebSocketChannel.connect(
      'ws://127.0.0.1:$port/app/app-key'
      '?protocol=7&client=thoth-test&version=1.0',
    );
    await channel.ready;
    final messages = StreamIterator<Object?>(channel.stream);
    expect(await messages.moveNext(), isTrue);
    final established = decodeFrame(messages.current);
    final id = (established['data']! as Map)['socket_id'] as String;
    return LifecycleSocket._(channel, messages, id);
  }

  Future<Map<String, Object?>> next() async {
    expect(await messages.moveNext(), isTrue);
    return decodeFrame(messages.current);
  }

  Future<void> subscribe(String channelName) async {
    final signature = PusherSigner(
      'app-secret',
    ).subscriptionSignature(id, channelName);
    channel.sink.add(
      jsonEncode({
        'event': 'pusher:subscribe',
        'data': {
          'channel': channelName,
          if (channelName.startsWith('private-')) 'auth': 'app-key:$signature',
        },
      }),
    );
    expect((await next())['event'], 'pusher_internal:subscription_succeeded');
  }

  void clientEvent(String channelName) => channel.sink.add(
    jsonEncode({
      'event': 'client-typing',
      'channel': channelName,
      'data': {'typing': true},
    }),
  );

  Future<void> close() async {
    await channel.sink.close();
    await messages.cancel();
  }
}

class FailingOnceDisconnectRegistry extends InMemoryChannelRegistry {
  var _shouldFail = true;
  final failed = Completer<void>();

  @override
  Future<void> disconnect(SocketConnection connection) async {
    await super.disconnect(connection);
    if (_shouldFail) {
      _shouldFail = false;
      failed.complete();
      throw StateError('disconnect failed');
    }
  }
}

class FailingSubscribeRegistry extends InMemoryChannelRegistry {
  @override
  Future<void> subscribe(
    SocketConnection connection,
    String channel, {
    Map<String, Object?>? member,
  }) => Future.error(StateError('subscribe failed with app-secret'));
}

class FailingPingConnection extends SocketConnection {
  FailingPingConnection(super.id, super.sink);

  @override
  Future<void> send(PusherFrame frame) {
    if (frame.event == 'pusher:ping') {
      return Future.error(StateError('ping failed with app-secret'));
    }
    return super.send(frame);
  }
}

class FailingTimeoutCloseConnection extends SocketConnection {
  FailingTimeoutCloseConnection(super.id, super.sink);

  @override
  Future<void> close([int? code, String? reason]) {
    if (code == 4201) {
      return Future.error(StateError('timeout close failed with app-secret'));
    }
    return super.close(code, reason);
  }
}

Future<HttpServer> start(ThothConfig config) => shelf_io.serve(
  ThothHandler(config).handler,
  InternetAddress.loopbackIPv4,
  0,
);

const baseConfig = ThothConfig(
  appId: 'app-id',
  appKey: 'app-key',
  appSecret: 'app-secret',
);

void main() {
  test('client events reach private peers but not the sender', () async {
    final server = await start(
      const ThothConfig(
        appId: 'app-id',
        appKey: 'app-key',
        appSecret: 'app-secret',
        clientEvents: true,
      ),
    );
    addTearDown(() => server.close(force: true));
    final sender = await LifecycleSocket.connect(server.port);
    final peer = await LifecycleSocket.connect(server.port);
    addTearDown(sender.close);
    addTearDown(peer.close);
    await sender.subscribe('private-room');
    await peer.subscribe('private-room');

    sender.clientEvent('private-room');

    expect(await peer.next(), {
      'event': 'client-typing',
      'data': {'typing': true},
      'channel': 'private-room',
    });
    final senderResult = await Future.any([
      sender.next().then((_) => 'received'),
      Future.delayed(const Duration(milliseconds: 30), () => 'none'),
    ]);
    expect(senderResult, 'none');
  });

  test('client events are rejected when disabled', () async {
    final server = await start(baseConfig);
    addTearDown(() => server.close(force: true));
    final socket = await LifecycleSocket.connect(server.port);
    addTearDown(socket.close);
    await socket.subscribe('private-room');

    socket.clientEvent('private-room');

    expect((await socket.next())['event'], 'pusher:error');
  });

  test('client events are rejected on public channels', () async {
    final server = await start(
      const ThothConfig(
        appId: 'app-id',
        appKey: 'app-key',
        appSecret: 'app-secret',
        clientEvents: true,
      ),
    );
    addTearDown(() => server.close(force: true));
    final socket = await LifecycleSocket.connect(server.port);
    addTearDown(socket.close);
    await socket.subscribe('room');

    socket.clientEvent('room');

    expect((await socket.next())['event'], 'pusher:error');
  });

  test('client events require membership in the named channel', () async {
    final server = await start(
      const ThothConfig(
        appId: 'app-id',
        appKey: 'app-key',
        appSecret: 'app-secret',
        clientEvents: true,
      ),
    );
    addTearDown(() => server.close(force: true));
    final socket = await LifecycleSocket.connect(server.port);
    addTearDown(socket.close);

    socket.clientEvent('private-room');

    expect((await socket.next())['event'], 'pusher:error');
  });

  test('silent clients receive ping then close with 4201', () async {
    final server = await start(
      const ThothConfig(
        appId: 'app-id',
        appKey: 'app-key',
        appSecret: 'app-secret',
        activityTimeout: Duration(milliseconds: 30),
        pongTimeout: Duration(milliseconds: 30),
      ),
    );
    addTearDown(() => server.close(force: true));
    final socket = await LifecycleSocket.connect(server.port);
    addTearDown(socket.close);

    expect((await socket.next())['event'], 'pusher:ping');
    expect(await socket.messages.moveNext(), isFalse);
    expect(socket.channel.closeCode, 4201);
  });

  test('pusher:pong keeps an active client open', () async {
    final server = await start(
      const ThothConfig(
        appId: 'app-id',
        appKey: 'app-key',
        appSecret: 'app-secret',
        activityTimeout: Duration(milliseconds: 30),
        pongTimeout: Duration(milliseconds: 30),
      ),
    );
    addTearDown(() => server.close(force: true));
    final socket = await LifecycleSocket.connect(server.port);
    addTearDown(socket.close);
    expect((await socket.next())['event'], 'pusher:ping');

    socket.channel.sink.add(jsonEncode({'event': 'pusher:pong', 'data': {}}));

    expect((await socket.next())['event'], 'pusher:ping');
  });

  test('heartbeat send failures are reported and close 1011', () async {
    final previousSink = Log.sink;
    final logs = StringBuffer();
    Log.sink = logs;
    addTearDown(() => Log.sink = previousSink);
    final server = await shelf_io.serve(
      ThothHandler(
        const ThothConfig(
          appId: 'app-id',
          appKey: 'app-key',
          appSecret: 'app-secret',
          activityTimeout: Duration(milliseconds: 10),
        ),
        connectionFactory: (id, WebSocketSink sink) =>
            FailingPingConnection(id, sink),
      ).handler,
      InternetAddress.loopbackIPv4,
      0,
    );
    addTearDown(() => server.close(force: true));
    final socket = await LifecycleSocket.connect(server.port);
    addTearDown(socket.close);

    expect(await socket.messages.moveNext(), isFalse);
    expect(socket.channel.closeCode, 1011);
    expect(logs.toString(), contains('ping failed'));
    expect(logs.toString(), isNot(contains('app-secret')));
  });

  test('pong-timeout close failures are reported and close 1011', () async {
    final previousSink = Log.sink;
    final logs = StringBuffer();
    Log.sink = logs;
    addTearDown(() => Log.sink = previousSink);
    final server = await shelf_io.serve(
      ThothHandler(
        const ThothConfig(
          appId: 'app-id',
          appKey: 'app-key',
          appSecret: 'app-secret',
          activityTimeout: Duration(milliseconds: 10),
          pongTimeout: Duration(milliseconds: 10),
        ),
        connectionFactory: (id, WebSocketSink sink) =>
            FailingTimeoutCloseConnection(id, sink),
      ).handler,
      InternetAddress.loopbackIPv4,
      0,
    );
    addTearDown(() => server.close(force: true));
    final socket = await LifecycleSocket.connect(server.port);
    addTearDown(socket.close);

    expect((await socket.next())['event'], 'pusher:ping');
    expect(await socket.messages.moveNext(), isFalse);
    expect(socket.channel.closeCode, 1011);
    expect(logs.toString(), contains('timeout close failed'));
    expect(logs.toString(), isNot(contains('app-secret')));
  });

  test('connection capacity rejects before admission', () async {
    final server = await start(
      const ThothConfig(
        appId: 'app-id',
        appKey: 'app-key',
        appSecret: 'app-secret',
        maxConnections: 1,
      ),
    );
    addTearDown(() => server.close(force: true));
    final first = await LifecycleSocket.connect(server.port);
    addTearDown(first.close);
    final second = IOWebSocketChannel.connect(
      'ws://127.0.0.1:${server.port}/app/app-key'
      '?protocol=7&client=thoth-test&version=1.0',
    );
    await second.ready;

    await second.stream.drain<void>();

    expect(second.closeCode, 4100);
  });

  test(
    'internal message handling failures are reported and close 1011',
    () async {
      final previousSink = Log.sink;
      final logs = StringBuffer();
      Log.sink = logs;
      addTearDown(() => Log.sink = previousSink);
      final server = await shelf_io.serve(
        ThothHandler(baseConfig, registry: FailingSubscribeRegistry()).handler,
        InternetAddress.loopbackIPv4,
        0,
      );
      addTearDown(() => server.close(force: true));
      final socket = await LifecycleSocket.connect(server.port);
      addTearDown(socket.close);

      socket.channel.sink.add(
        jsonEncode({
          'event': 'pusher:subscribe',
          'data': {'channel': 'tasks'},
        }),
      );

      expect(await socket.messages.moveNext(), isFalse);
      expect(socket.channel.closeCode, 1011);
      expect(logs.toString(), contains('subscribe failed'));
      expect(logs.toString(), isNot(contains('app-secret')));
    },
  );

  test('disconnect cleanup failure releases connection capacity', () async {
    final previousSink = Log.sink;
    final logs = StringBuffer();
    Log.sink = logs;
    addTearDown(() => Log.sink = previousSink);
    final uncaughtErrors = <Object>[];
    final registry = FailingOnceDisconnectRegistry();
    final serverReady = Completer<HttpServer>();
    runZonedGuarded(
      () async => serverReady.complete(
        await shelf_io.serve(
          ThothHandler(
            const ThothConfig(
              appId: 'app-id',
              appKey: 'app-key',
              appSecret: 'app-secret',
              maxConnections: 1,
            ),
            registry: registry,
          ).handler,
          InternetAddress.loopbackIPv4,
          0,
        ),
      ),
      (error, _) => uncaughtErrors.add(error),
    );
    final server = await serverReady.future;
    addTearDown(() => server.close(force: true));
    final first = await LifecycleSocket.connect(server.port);

    await first.close();
    await registry.failed.future;
    await Future<void>.delayed(Duration.zero);

    expect(uncaughtErrors, isEmpty);
    expect(logs.toString(), contains('disconnect failed'));

    final second = await LifecycleSocket.connect(server.port);
    await second.close();
  });
}
