import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:maat/maat.dart';
import 'package:shelf/shelf_io.dart' as shelf_io;
import 'package:test/test.dart';
import 'package:thoth_realtime/thoth_realtime.dart';
import 'package:web_socket_channel/io.dart';

Map<String, Object?> decodeFrame(Object? raw) {
  final outer = Map<String, Object?>.from(jsonDecode(raw! as String) as Map);
  outer['data'] = jsonDecode(outer['data']! as String);
  return outer;
}

Future<Map<String, Object?>> nextFrame(StreamIterator<Object?> messages) async {
  expect(await messages.moveNext(), isTrue);
  return decodeFrame(messages.current);
}

class TestSocket {
  TestSocket(this.channel) : messages = StreamIterator(channel.stream);

  final IOWebSocketChannel channel;
  final StreamIterator<Object?> messages;

  static Future<TestSocket> connect(int port) async {
    final channel = IOWebSocketChannel.connect(
      'ws://127.0.0.1:$port/app/app-key'
      '?protocol=7&client=thoth-test&version=1.0',
    );
    await channel.ready;
    return TestSocket(channel);
  }

  Future<Map<String, Object?>> next() => nextFrame(messages);

  Future<void> close() async {
    await channel.sink.close();
    await messages.cancel();
  }
}

void main() {
  late HttpServer server;

  setUp(() async {
    final handler = ThothHandler(
      const ThothConfig(
        appId: 'app-id',
        appKey: 'app-key',
        appSecret: 'app-secret',
      ),
    );
    server = await shelf_io.serve(
      handler.handler,
      InternetAddress.loopbackIPv4,
      0,
    );
    addTearDown(() => server.close(force: true));
  });

  test('connects and subscribes to a public channel', () async {
    final socket = await TestSocket.connect(server.port);
    addTearDown(socket.close);

    final established = await socket.next();
    expect(established['event'], 'pusher:connection_established');
    expect((established['data']! as Map)['activity_timeout'], 120);
    expect(
      (established['data']! as Map)['socket_id'],
      matches(RegExp(r'^[1-9][0-9]*\.[1-9][0-9]*$')),
    );

    socket.channel.sink.add(
      jsonEncode({
        'event': 'pusher:subscribe',
        'data': {'channel': 'tasks'},
      }),
    );
    final subscribed = await socket.next();
    expect(subscribed['event'], 'pusher_internal:subscription_succeeded');
    expect(subscribed['channel'], 'tasks');
  });

  test('unsubscribe is idempotent', () async {
    final socket = await TestSocket.connect(server.port);
    addTearDown(socket.close);
    await socket.next();
    final subscribe = jsonEncode({
      'event': 'pusher:subscribe',
      'data': {'channel': 'tasks'},
    });
    final unsubscribe = jsonEncode({
      'event': 'pusher:unsubscribe',
      'data': {'channel': 'tasks'},
    });
    socket.channel.sink.add(subscribe);
    expect(
      (await socket.next())['event'],
      'pusher_internal:subscription_succeeded',
    );

    socket.channel.sink
      ..add(unsubscribe)
      ..add(unsubscribe)
      ..add(subscribe);

    expect(
      (await socket.next())['event'],
      'pusher_internal:subscription_succeeded',
    );
  });

  test('WebSocket route matches only /app/{key}', () async {
    final client = HttpClient();
    addTearDown(() => client.close(force: true));
    final response = await (await client.getUrl(
      Uri.parse('http://127.0.0.1:${server.port}/app/app-key/extra'),
    )).close();

    expect(response.statusCode, 404);
  });

  test('accepts a correctly signed private subscription', () async {
    final socket = await TestSocket.connect(server.port);
    addTearDown(socket.close);
    final established = await socket.next();
    final socketId = (established['data']! as Map)['socket_id'] as String;
    const channel = 'private-orders.1';
    final signature = PusherSigner(
      'app-secret',
    ).subscriptionSignature(socketId, channel);

    socket.channel.sink.add(
      jsonEncode({
        'event': 'pusher:subscribe',
        'data': {'channel': channel, 'auth': 'app-key:$signature'},
      }),
    );

    final subscribed = await socket.next();
    expect(subscribed['event'], 'pusher_internal:subscription_succeeded');
    expect(subscribed['channel'], channel);
  });

  test('a bad private signature emits an error without closing', () async {
    final socket = await TestSocket.connect(server.port);
    addTearDown(socket.close);
    await socket.next();

    socket.channel.sink.add(
      jsonEncode({
        'event': 'pusher:subscribe',
        'data': {'channel': 'private-orders.1', 'auth': 'app-key:invalid'},
      }),
    );
    expect(await socket.next(), {
      'event': 'pusher:error',
      'data': {'message': 'Invalid subscription signature'},
    });

    socket.channel.sink.add(
      jsonEncode({
        'event': 'pusher:subscribe',
        'data': {'channel': 'tasks'},
      }),
    );
    expect(
      (await socket.next())['event'],
      'pusher_internal:subscription_succeeded',
    );
  });

  test('malformed and unknown events emit errors without closing', () async {
    final socket = await TestSocket.connect(server.port);
    addTearDown(socket.close);
    await socket.next();

    socket.channel.sink.add('{');
    expect(await socket.next(), {
      'event': 'pusher:error',
      'data': {'message': 'Malformed event'},
    });
    socket.channel.sink.add(jsonEncode({'event': 'unknown', 'data': {}}));
    expect((await socket.next())['event'], 'pusher:error');
    socket.channel.sink.add(
      jsonEncode({
        'event': 'pusher:subscribe',
        'data': {'channel': 'tasks'},
      }),
    );
    expect(
      (await socket.next())['event'],
      'pusher_internal:subscription_succeeded',
    );
  });

  test('presence subscriptions announce member join and final leave', () async {
    final first = await TestSocket.connect(server.port);
    final second = await TestSocket.connect(server.port);
    addTearDown(first.close);
    addTearDown(second.close);
    final firstId =
        ((await first.next())['data']! as Map)['socket_id'] as String;
    final secondId =
        ((await second.next())['data']! as Map)['socket_id'] as String;
    const channel = 'presence-room';
    const firstData = '{"user_id":"7","user_info":{"name":"Ada"}}';
    const secondData = '{"user_id":"8","user_info":{"name":"Lin"}}';
    final signer = PusherSigner('app-secret');

    void subscribe(TestSocket socket, String id, String data) {
      final signature = signer.subscriptionSignature(
        id,
        channel,
        channelData: data,
      );
      socket.channel.sink.add(
        jsonEncode({
          'event': 'pusher:subscribe',
          'data': {
            'channel': channel,
            'auth': 'app-key:$signature',
            'channel_data': data,
          },
        }),
      );
    }

    subscribe(first, firstId, firstData);
    final firstSubscribed = await first.next();
    expect((firstSubscribed['data']! as Map)['presence'], {
      'ids': ['7'],
      'hash': {
        '7': {'name': 'Ada'},
      },
      'count': 1,
    });

    subscribe(second, secondId, secondData);
    final added = await first.next();
    expect(added['event'], 'pusher_internal:member_added');
    expect(added['data'], {
      'user_id': '8',
      'user_info': {'name': 'Lin'},
    });
    final secondSubscribed = await second.next();
    expect((secondSubscribed['data']! as Map)['presence'], {
      'ids': ['7', '8'],
      'hash': {
        '7': {'name': 'Ada'},
        '8': {'name': 'Lin'},
      },
      'count': 2,
    });

    await second.channel.sink.close();
    final removed = await first.next();
    expect(removed['event'], 'pusher_internal:member_removed');
    expect(removed['data'], {'user_id': '8'});
  });

  for (final testCase in [
    (path: '/app/unknown?protocol=7&client=test&version=1', code: 4001),
    (path: '/app/app-key?client=test&version=1', code: 4008),
    (path: '/app/app-key?protocol=8&client=test&version=1', code: 4007),
  ]) {
    test('rejects an invalid connection with ${testCase.code}', () async {
      final socket = IOWebSocketChannel.connect(
        'ws://127.0.0.1:${server.port}${testCase.path}',
      );
      await socket.ready;
      await socket.stream.drain<void>();
      expect(socket.closeCode, testCase.code);
    });
  }
}
