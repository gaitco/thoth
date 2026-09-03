import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:maat/maat.dart' show PusherSigner;
import 'package:shelf/shelf_io.dart' as shelf_io;
import 'package:test/test.dart';
import 'package:thoth_realtime/thoth_realtime.dart';
import 'package:web_socket_channel/io.dart';

const config = ThothConfig(
  appId: 'app-id',
  appKey: 'app-key',
  appSecret: 'app-secret',
);

Map<String, Object?> decodeFrame(Object? raw) {
  final frame = Map<String, Object?>.from(jsonDecode(raw! as String) as Map);
  frame['data'] = jsonDecode(frame['data']! as String);
  return frame;
}

class ApiSocket {
  ApiSocket._(this.channel, this.messages, this.id);

  final IOWebSocketChannel channel;
  final StreamIterator<Object?> messages;
  final String id;

  static Future<ApiSocket> connect(int port) async {
    final channel = IOWebSocketChannel.connect(
      'ws://127.0.0.1:$port/app/app-key'
      '?protocol=7&client=thoth-test&version=1.0',
    );
    await channel.ready;
    final messages = StreamIterator<Object?>(channel.stream);
    expect(await messages.moveNext(), isTrue);
    final established = decodeFrame(messages.current);
    final socket = ApiSocket._(
      channel,
      messages,
      (established['data']! as Map)['socket_id'] as String,
    );
    channel.sink.add(
      jsonEncode({
        'event': 'pusher:subscribe',
        'data': {'channel': 'tasks'},
      }),
    );
    expect(
      (await socket.next())['event'],
      'pusher_internal:subscription_succeeded',
    );
    return socket;
  }

  Future<Map<String, Object?>> next() async {
    expect(await messages.moveNext(), isTrue);
    return decodeFrame(messages.current);
  }

  Future<void> close() async {
    await channel.sink.close();
    await messages.cancel();
  }
}

Future<(int, String)> request(
  int port,
  String method,
  String path, {
  String body = '',
  bool validSignature = true,
  int? timestamp,
}) async {
  final signer = PusherSigner('app-secret');
  final signed = signer.signHttp(
    method: method,
    path: path,
    body: body,
    key: 'app-key',
    timestamp: timestamp ?? DateTime.now().millisecondsSinceEpoch ~/ 1000,
  );
  if (!validSignature) signed['auth_signature'] = 'invalid';
  final uri = Uri(
    scheme: 'http',
    host: '127.0.0.1',
    port: port,
    path: path,
    queryParameters: signed,
  );
  final client = HttpClient();
  try {
    final outgoing = await client.openUrl(method, uri);
    if (body.isNotEmpty) {
      outgoing.headers.contentType = ContentType.json;
      outgoing.write(body);
    }
    final response = await outgoing.close();
    return (response.statusCode, await utf8.decoder.bind(response).join());
  } finally {
    client.close(force: true);
  }
}

void main() {
  late HttpServer server;

  setUp(() async {
    server = await shelf_io.serve(
      ThothHandler(config).handler,
      InternetAddress.loopbackIPv4,
      0,
    );
    addTearDown(() => server.close(force: true));
  });

  test('health is unsigned', () async {
    final client = HttpClient();
    addTearDown(() => client.close(force: true));
    final response = await (await client.getUrl(
      Uri.parse('http://127.0.0.1:${server.port}/health'),
    )).close();

    expect(response.statusCode, 200);
    expect(await utf8.decoder.bind(response).join(), '{"status":"ok"}');
  });

  test(
    'signed HTTP publish reaches subscribers and honors socket_id',
    () async {
      final first = await ApiSocket.connect(server.port);
      final second = await ApiSocket.connect(server.port);
      addTearDown(first.close);
      addTearDown(second.close);
      const path = '/apps/app-id/events';
      final body = jsonEncode({
        'name': 'TaskChanged',
        'channels': ['tasks'],
        'data': jsonEncode({'id': 1}),
      });

      expect((await request(server.port, 'POST', path, body: body)).$1, 200);
      expect(await first.next(), {
        'event': 'TaskChanged',
        'data': {'id': 1},
        'channel': 'tasks',
      });
      await second.next();

      final excludedBody = jsonEncode({
        'name': 'TaskChanged',
        'channels': ['tasks'],
        'data': jsonEncode({'id': 2}),
        'socket_id': first.id,
      });
      expect(
        (await request(server.port, 'POST', path, body: excludedBody)).$1,
        200,
      );
      expect((await second.next())['data'], {'id': 2});

      final sentinelBody = jsonEncode({
        'name': 'TaskChanged',
        'channels': ['tasks'],
        'data': jsonEncode({'id': 3}),
      });
      expect(
        (await request(server.port, 'POST', path, body: sentinelBody)).$1,
        200,
      );
      expect((await first.next())['data'], {'id': 3});
      expect((await second.next())['data'], {'id': 3});
    },
  );

  test(
    'signed API rejects bad signatures and malformed publish bodies',
    () async {
      const path = '/apps/app-id/events';
      const body = '{}';

      expect(
        (await request(
          server.port,
          'POST',
          path,
          body: body,
          validSignature: false,
        )).$1,
        401,
      );
      expect((await request(server.port, 'POST', path, body: body)).$1, 422);
    },
  );

  test(
    'signed API rejects stale requests without exposing the secret',
    () async {
      const path = '/apps/app-id/channels';
      final stale = await request(
        server.port,
        'GET',
        path,
        timestamp: DateTime.now().millisecondsSinceEpoch ~/ 1000 - 601,
      );
      final invalid = await request(
        server.port,
        'GET',
        path,
        validSignature: false,
      );

      expect(stale.$1, 401);
      expect(stale.$2, isNot(contains('app-secret')));
      expect(invalid.$1, 401);
      expect(invalid.$2, isNot(contains('app-secret')));
    },
  );

  test('wrong application ID returns 404 before authentication', () async {
    const path = '/apps/wrong-id/channels';

    expect((await request(server.port, 'GET', path)).$1, 404);
    expect(
      (await request(server.port, 'GET', path, validSignature: false)).$1,
      404,
    );
  });

  test('signed API reports occupied channels', () async {
    final socket = await ApiSocket.connect(server.port);
    addTearDown(socket.close);

    final channels = await request(server.port, 'GET', '/apps/app-id/channels');
    expect(channels.$1, 200);
    expect((jsonDecode(channels.$2) as Map)['channels'], {
      'tasks': <String, Object?>{},
    });

    final channel = await request(
      server.port,
      'GET',
      '/apps/app-id/channels/tasks',
    );
    expect(channel.$1, 200);
    expect(jsonDecode(channel.$2), {'occupied': true, 'subscription_count': 1});

    expect(
      (await request(server.port, 'GET', '/apps/app-id/channels/missing')).$1,
      404,
    );
  });

  test('signed API lists presence users', () async {
    final socket = await ApiSocket.connect(server.port);
    addTearDown(socket.close);
    const channel = 'presence-room';
    const channelData = '{"user_id":"7","user_info":{"name":"Ada"}}';
    final signature = PusherSigner(
      'app-secret',
    ).subscriptionSignature(socket.id, channel, channelData: channelData);
    socket.channel.sink.add(
      jsonEncode({
        'event': 'pusher:subscribe',
        'data': {
          'channel': channel,
          'auth': 'app-key:$signature',
          'channel_data': channelData,
        },
      }),
    );
    expect(
      (await socket.next())['event'],
      'pusher_internal:subscription_succeeded',
    );

    final users = await request(
      server.port,
      'GET',
      '/apps/app-id/channels/$channel/users',
    );

    expect(users.$1, 200);
    expect(jsonDecode(users.$2), {
      'users': [
        {'id': '7'},
      ],
    });
  });
}
