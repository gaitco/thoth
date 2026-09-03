import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:maat/maat.dart' show Log, PusherSigner;
import 'package:shelf/shelf.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

import 'channel_registry.dart';
import 'config.dart';
import 'frame.dart';
import 'socket_connection.dart';

class ThothHandler {
  ThothHandler(
    this.config, {
    ChannelRegistry? registry,
    SocketConnection Function(String id, WebSocketSink sink)? connectionFactory,
  }) : registry = registry ?? InMemoryChannelRegistry(),
       _connectionFactory =
           connectionFactory ?? ((id, sink) => SocketConnection(id, sink)),
       _signer = PusherSigner(config.appSecret);

  final ThothConfig config;
  final ChannelRegistry registry;
  final SocketConnection Function(String id, WebSocketSink sink)
  _connectionFactory;
  final PusherSigner _signer;
  final Random _random = Random.secure();
  final Set<SocketConnection> _connections = {};
  final Set<Future<void>> _serveTasks = {};
  int _connectionCount = 0;
  bool _shuttingDown = false;

  Handler get handler => call;

  FutureOr<Response> call(Request request) {
    final segments = request.url.pathSegments;
    if (request.method == 'GET' &&
        segments.length == 1 &&
        segments.first == 'health') {
      return _json(200, {'status': 'ok'});
    }
    if (segments.isNotEmpty && segments.first == 'apps') {
      return _handleHttp(request, segments);
    }
    if (segments.length != 2 || segments.first != 'app') {
      return Response.notFound('Not found');
    }

    final rejection = _rejection(request, segments.last);
    return _upgrade(request, (socket) {
      if (rejection != null) {
        _runDetached(
          'Failed to reject WebSocket',
          () => socket.close(rejection.$1, rejection.$2),
        );
        return;
      }
      if (_shuttingDown) {
        _runDetached(
          'Failed to close WebSocket during shutdown',
          () => socket.close(WebSocketStatus.goingAway, 'Server shutting down'),
        );
        return;
      }
      _connectionCount++;
      final task = _serve(socket);
      _serveTasks.add(task);
      unawaited(_observe(task));
    });
  }

  Response _upgrade(Request request, void Function(WebSocket) onConnection) {
    if (request.method != 'GET') return Response.notFound('Not found');
    final connection = request.headers['Connection'];
    final upgrade = request.headers['Upgrade'];
    final version = request.headers['Sec-WebSocket-Version'];
    final key = request.headers['Sec-WebSocket-Key'];
    if (connection == null ||
        !connection
            .toLowerCase()
            .split(',')
            .map((token) => token.trim())
            .contains('upgrade') ||
        upgrade?.toLowerCase() != 'websocket') {
      return Response.notFound('Not found');
    }
    if (version == null || key == null || request.protocolVersion != '1.1') {
      return Response(400, body: 'Invalid WebSocket upgrade');
    }
    if (version != '13') return Response.notFound('Not found');
    if (!request.canHijack) {
      throw ArgumentError('WebSocket upgrades require request hijacking.');
    }

    request.hijack((channel) {
      try {
        final transport = channel.sink;
        if (transport is! Socket) {
          throw ArgumentError('WebSocket transport must be a Socket.');
        }
        utf8.encoder
            .startChunkedConversion(transport)
            .add(
              'HTTP/1.1 101 Switching Protocols\r\n'
              'Upgrade: websocket\r\n'
              'Connection: Upgrade\r\n'
              'Sec-WebSocket-Accept: ${WebSocketChannel.signKey(key)}\r\n'
              '\r\n',
            );
        onConnection(WebSocket.fromUpgradedSocket(transport, serverSide: true));
      } catch (error, stackTrace) {
        _report('Failed to upgrade WebSocket', error, stackTrace);
        final transport = channel.sink;
        if (transport is Socket) {
          transport.destroy();
        } else {
          _runDetached('Failed to close WebSocket transport', transport.close);
        }
      }
    });
  }

  Future<void> shutdown() async {
    _shuttingDown = true;
    final connections = _connections.toList();
    for (final connection in connections) {
      try {
        connection.cancelTimers();
      } catch (error, stackTrace) {
        _report('Failed to cancel connection timers', error, stackTrace);
      }
    }
    await Future.wait([
      for (final connection in connections)
        _guard(
          'Failed to close WebSocket during shutdown',
          () => connection.close(
            WebSocketStatus.goingAway,
            'Server shutting down',
          ),
        ),
    ]);
    await Future.wait(_serveTasks.toList());
  }

  Future<Response> _handleHttp(Request request, List<String> segments) async {
    if (segments.length < 3 || segments[1] != config.appId) {
      return _json(404, {'error': 'Not found'});
    }
    final body = await request.readAsString();
    final path = '/${request.url.path}';
    if (!_signer.verifyHttpRequest(
      method: request.method,
      path: path,
      body: body,
      key: config.appKey,
      query: request.url.queryParameters,
    )) {
      return _json(401, {'error': 'Unauthorized'});
    }

    if (request.method == 'POST' &&
        segments.length == 3 &&
        segments[2] == 'events') {
      return _publish(body);
    }
    if (request.method == 'GET' && segments[2] == 'channels') {
      if (segments.length == 3) return _channels();
      final info = registry.info(segments[3]);
      if (info == null) return _json(404, {'error': 'Channel not found'});
      if (segments.length == 4) return _channel(info);
      if (segments.length == 5 && segments[4] == 'users') {
        return _json(200, {
          'users': [
            for (final id in info.members.keys) {'id': id},
          ],
        });
      }
    }
    return _json(404, {'error': 'Not found'});
  }

  Future<Response> _publish(String body) async {
    try {
      final payload = Map<String, Object?>.from(jsonDecode(body) as Map);
      final name = payload['name'];
      final channels = payload['channels'];
      final encodedData = payload['data'];
      final socketId = payload['socket_id'];
      if (name is! String ||
          name.isEmpty ||
          channels is! List ||
          channels.isEmpty ||
          channels.any((channel) => channel is! String || channel.isEmpty) ||
          encodedData is! String ||
          (socketId != null && socketId is! String)) {
        return _json(422, {'error': 'Invalid event'});
      }
      final data = jsonDecode(encodedData);
      await Future.wait([
        for (final channel in channels.cast<String>())
          registry.publish(
            channel,
            name,
            data,
            exceptSocketId: socketId as String?,
          ),
      ]);
      return _json(200, const <String, Object?>{});
    } on Object {
      return _json(422, {'error': 'Invalid event'});
    }
  }

  Response _channels() => _json(200, {
    'channels': {
      for (final info in registry.occupiedChannels)
        info.name: info.members.isEmpty
            ? <String, Object?>{}
            : <String, Object?>{'user_count': info.members.length},
    },
  });

  Response _channel(ChannelInfo info) => _json(200, {
    'occupied': info.occupied,
    'subscription_count': info.subscriptionCount,
    if (info.members.isNotEmpty) 'user_count': info.members.length,
  });

  static Response _json(int status, Object body) => Response(
    status,
    body: jsonEncode(body),
    headers: {'content-type': 'application/json'},
  );

  (int, String)? _rejection(Request request, String key) {
    if (key != config.appKey) return (4001, 'Unknown application key');
    final protocol = request.url.queryParameters['protocol'];
    if (protocol == null) return (4008, 'Protocol is required');
    if (protocol != '7') return (4007, 'Unsupported protocol');
    if (config.maxConnections > 0 &&
        _connectionCount >= config.maxConnections) {
      return (4100, 'Connection limit reached');
    }
    return null;
  }

  Future<void> _serve(WebSocket socket) async {
    SocketConnection? connection;
    try {
      connection = _connectionFactory(_socketId(), _IoWebSocketSink(socket));
      _connections.add(connection);
      await connection.send(
        PusherFrame('pusher:connection_established', {
          'socket_id': connection.id,
          'activity_timeout': config.activityTimeout.inSeconds,
        }),
      );
      _resetActivity(connection);
      await for (final message in socket) {
        _resetActivity(connection);
        await _handleMessage(connection, message);
      }
    } catch (error, stackTrace) {
      final active = connection;
      if (active == null) {
        _report('Failed to create WebSocket connection', error, stackTrace);
      } else {
        await _failConnection(active, error, stackTrace);
      }
    } finally {
      final active = connection;
      try {
        if (active != null) active.cancelTimers();
      } catch (error, stackTrace) {
        _report('Failed to cancel connection timers', error, stackTrace);
      }
      try {
        if (active != null) await registry.disconnect(active);
      } catch (error, stackTrace) {
        _report('Failed to disconnect WebSocket', error, stackTrace);
      } finally {
        if (active != null) _connections.remove(active);
        _connectionCount--;
      }
    }
  }

  Future<void> _handleMessage(
    SocketConnection connection,
    Object? message,
  ) async {
    late final Map<String, Object?> frame;
    try {
      frame = Map<String, Object?>.from(jsonDecode(message! as String) as Map);
    } on FormatException {
      await _error(connection, 'Malformed event');
      return;
    } on TypeError {
      await _error(connection, 'Malformed event');
      return;
    }

    final event = frame['event'];
    if (event == 'pusher:pong') return;
    if (event == 'pusher:ping') {
      await connection.send(
        const PusherFrame('pusher:pong', <String, Object?>{}),
      );
      return;
    }
    if (event == 'pusher:subscribe') {
      late final Map<String, Object?> data;
      try {
        data = _dataMap(frame['data']);
      } on FormatException {
        await _error(connection, 'Malformed event');
        return;
      } on TypeError {
        await _error(connection, 'Malformed event');
        return;
      }
      final channel = data['channel'];
      if (channel is! String || channel.isEmpty) {
        await _error(connection, 'A channel is required');
        return;
      }
      final private = channel.startsWith('private-');
      final presence = channel.startsWith('presence-');
      Map<String, Object?>? member;
      if (private || presence) {
        final channelData = data['channel_data'];
        if (!_validSubscription(
          connection.id,
          channel,
          data['auth'],
          channelData: presence ? channelData : null,
        )) {
          await _error(connection, 'Invalid subscription signature');
          return;
        }
        if (presence) {
          try {
            member = _dataMap(channelData);
          } on FormatException {
            await _error(connection, 'Malformed event');
            return;
          } on TypeError {
            await _error(connection, 'Malformed event');
            return;
          }
          final userId = member['user_id'];
          if (userId is! String || userId.isEmpty) {
            await _error(connection, 'Presence user_id is required');
            return;
          }
        }
      }
      await registry.subscribe(connection, channel, member: member);
      final info = registry.info(channel);
      await connection.send(
        PusherFrame(
          'pusher_internal:subscription_succeeded',
          presence
              ? {
                  'presence': {
                    'ids': info!.members.keys.toList(),
                    'hash': info.members,
                    'count': info.members.length,
                  },
                }
              : const <String, Object?>{},
          channel: channel,
        ),
      );
      return;
    }
    if (event == 'pusher:unsubscribe') {
      late final Object? channel;
      try {
        channel = _dataMap(frame['data'])['channel'];
      } on FormatException {
        await _error(connection, 'Malformed event');
        return;
      } on TypeError {
        await _error(connection, 'Malformed event');
        return;
      }
      if (channel is String) await registry.unsubscribe(connection, channel);
      return;
    }
    if (event is String && event.startsWith('client-')) {
      final channel = frame['channel'];
      if (!config.clientEvents ||
          channel is! String ||
          (!channel.startsWith('private-') &&
              !channel.startsWith('presence-')) ||
          !connection.channels.contains(channel)) {
        await _error(connection, 'Client event is not allowed');
        return;
      }
      await registry.publish(
        channel,
        event,
        frame['data'],
        exceptSocketId: connection.id,
      );
      return;
    }
    await _error(connection, 'Unknown event');
  }

  Future<void> _error(SocketConnection connection, String message) =>
      connection.send(PusherFrame('pusher:error', {'message': message}));

  void _resetActivity(SocketConnection connection) {
    connection.activityTimer?.cancel();
    connection.pongTimer?.cancel();
    connection.activityTimer = Timer(config.activityTimeout, () {
      _runConnectionTask(connection, 'Failed to send heartbeat', () async {
        await connection.send(
          const PusherFrame('pusher:ping', <String, Object?>{}),
        );
        connection.pongTimer = Timer(config.pongTimeout, () {
          _runConnectionTask(
            connection,
            'Failed to close connection after pong timeout',
            () => connection.close(4201, 'Pong timeout'),
          );
        });
      });
    });
  }

  void _runConnectionTask(
    SocketConnection connection,
    String context,
    Future<void> Function() action,
  ) {
    unawaited(() async {
      try {
        await action();
      } catch (error, stackTrace) {
        await _failConnection(connection, error, stackTrace, context: context);
      }
    }());
  }

  Future<void> _failConnection(
    SocketConnection connection,
    Object error,
    StackTrace stackTrace, {
    String context = 'WebSocket connection failed',
  }) async {
    _report(context, error, stackTrace);
    try {
      connection.cancelTimers();
    } catch (timerError, timerStackTrace) {
      _report(
        'Failed to cancel connection timers',
        timerError,
        timerStackTrace,
      );
    }
    await _guard(
      'Failed to close faulty WebSocket',
      () => connection.close(
        WebSocketStatus.internalServerError,
        'Internal server error',
      ),
    );
  }

  void _runDetached(String context, Future<void> Function() action) {
    unawaited(_guard(context, action));
  }

  Future<void> _guard(String context, Future<void> Function() action) async {
    try {
      await action();
    } catch (error, stackTrace) {
      _report(context, error, stackTrace);
    }
  }

  Future<void> _observe(Future<void> task) async {
    try {
      await task;
    } catch (error, stackTrace) {
      _report('Unhandled WebSocket serve task failure', error, stackTrace);
    } finally {
      _serveTasks.remove(task);
    }
  }

  void _report(String context, Object error, StackTrace stackTrace) {
    try {
      Log.error(
        _redact('$context: $error'),
        null,
        StackTrace.fromString(_redact(stackTrace.toString())),
      );
    } catch (_) {
      // Error reporting must not create another detached failure.
    }
  }

  String _redact(String value) => config.appSecret.isEmpty
      ? value
      : value.replaceAll(config.appSecret, '[REDACTED]');

  bool _validSubscription(
    String socketId,
    String channel,
    Object? auth, {
    Object? channelData,
  }) {
    if (auth is! String) return false;
    final separator = auth.indexOf(':');
    if (separator < 1 || auth.substring(0, separator) != config.appKey) {
      return false;
    }
    if (channelData != null && channelData is! String) return false;
    final expected = _signer.subscriptionSignature(
      socketId,
      channel,
      channelData: channelData as String?,
    );
    return _signer.secureEquals(auth.substring(separator + 1), expected);
  }

  static Map<String, Object?> _dataMap(Object? data) {
    final decoded = data is String ? jsonDecode(data) : data;
    return Map<String, Object?>.from(decoded as Map);
  }

  String _socketId() =>
      '${_random.nextInt(0x7ffffffe) + 1}.'
      '${_random.nextInt(0x7ffffffe) + 1}';
}

class _IoWebSocketSink implements WebSocketSink {
  const _IoWebSocketSink(this.socket);

  final WebSocket socket;

  @override
  void add(Object? data) => socket.add(data);

  @override
  void addError(Object error, [StackTrace? stackTrace]) =>
      socket.addError(error, stackTrace);

  @override
  Future<void> addStream(Stream<Object?> stream) async {
    await socket.addStream(stream);
  }

  @override
  Future<void> close([int? closeCode, String? closeReason]) async {
    await socket.close(closeCode, closeReason);
  }

  @override
  Future<void> get done async {
    await socket.done;
  }
}
