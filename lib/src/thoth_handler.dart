import 'dart:async';
import 'dart:convert';
import 'dart:math';

import 'package:maat/maat.dart' show PusherSigner;
import 'package:shelf/shelf.dart';
import 'package:shelf_web_socket/shelf_web_socket.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

import 'channel_registry.dart';
import 'config.dart';
import 'frame.dart';
import 'socket_connection.dart';

class ThothHandler {
  ThothHandler(this.config, {ChannelRegistry? registry})
    : registry = registry ?? InMemoryChannelRegistry(),
      _signer = PusherSigner(config.appSecret);

  final ThothConfig config;
  final ChannelRegistry registry;
  final PusherSigner _signer;
  final Random _random = Random.secure();
  int _connectionCount = 0;

  Handler get handler => call;

  FutureOr<Response> call(Request request) {
    final segments = request.url.pathSegments;
    if (segments.length != 2 || segments.first != 'app') {
      return Response.notFound('Not found');
    }

    final rejection = _rejection(request, segments.last);
    return webSocketHandler((socket, _) {
      if (rejection != null) {
        unawaited(socket.sink.close(rejection.$1, rejection.$2));
        return;
      }
      _connectionCount++;
      unawaited(_serve(socket));
    })(request);
  }

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

  Future<void> _serve(WebSocketChannel socket) async {
    final connection = SocketConnection(_socketId(), socket.sink);
    try {
      await connection.send(
        PusherFrame('pusher:connection_established', {
          'socket_id': connection.id,
          'activity_timeout': config.activityTimeout.inSeconds,
        }),
      );
      await for (final message in socket.stream) {
        await _handleMessage(connection, message);
      }
    } finally {
      await registry.disconnect(connection);
      _connectionCount--;
    }
  }

  Future<void> _handleMessage(
    SocketConnection connection,
    Object? message,
  ) async {
    try {
      final frame = Map<String, Object?>.from(
        jsonDecode(message! as String) as Map,
      );
      final event = frame['event'];
      if (event == 'pusher:subscribe') {
        final data = _dataMap(frame['data']);
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
            member = _dataMap(channelData);
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
        final channel = _dataMap(frame['data'])['channel'];
        if (channel is String) await registry.unsubscribe(connection, channel);
        return;
      }
      await _error(connection, 'Unknown event');
    } on Object {
      await _error(connection, 'Malformed event');
    }
  }

  Future<void> _error(SocketConnection connection, String message) => connection
      .send(PusherFrame('pusher:error', {'code': 4000, 'message': message}));

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
