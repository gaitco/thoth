import 'socket_connection.dart';
import 'frame.dart';

class ChannelInfo {
  const ChannelInfo({
    required this.name,
    required this.subscriptionCount,
    required this.members,
  });

  final String name;
  final int subscriptionCount;
  final Map<String, Map<String, Object?>> members;

  bool get occupied => subscriptionCount > 0;
}

abstract interface class ChannelRegistry {
  Future<void> subscribe(
    SocketConnection connection,
    String channel, {
    Map<String, Object?>? member,
  });

  Future<void> unsubscribe(SocketConnection connection, String channel);

  Future<void> disconnect(SocketConnection connection);

  Future<void> publish(
    String channel,
    String event,
    Object? data, {
    String? exceptSocketId,
  });

  ChannelInfo? info(String channel);

  Iterable<ChannelInfo> get occupiedChannels;
}

class InMemoryChannelRegistry implements ChannelRegistry {
  final Map<String, Set<SocketConnection>> _channels = {};
  final Map<String, Map<String, _PresenceMember>> _presence = {};
  final Map<SocketConnection, Map<String, String>> _memberIds = {};

  @override
  Future<void> subscribe(
    SocketConnection connection,
    String channel, {
    Map<String, Object?>? member,
  }) async {
    final userId = member?['user_id'];
    if (member != null && (userId is! String || userId.isEmpty)) {
      throw ArgumentError.value(userId, 'member.user_id', 'must not be empty');
    }

    final subscribers = _channels.putIfAbsent(channel, () => {});
    if (!subscribers.add(connection)) return;
    connection.channels.add(channel);

    if (member == null) return;
    final members = _presence.putIfAbsent(channel, () => {});
    final state = members.putIfAbsent(
      userId as String,
      () => _PresenceMember(_memberInfo(member)),
    );
    final joined = state.connections.isEmpty;
    state.connections.add(connection);
    _memberIds.putIfAbsent(connection, () => {})[channel] = userId;
    if (joined) {
      await publish(channel, 'pusher_internal:member_added', {
        'user_id': userId,
        'user_info': state.info,
      }, exceptSocketId: connection.id);
    }
  }

  @override
  Future<void> unsubscribe(SocketConnection connection, String channel) async {
    final subscribers = _channels[channel];
    if (subscribers == null || !subscribers.remove(connection)) return;
    connection.channels.remove(channel);
    if (subscribers.isEmpty) _channels.remove(channel);

    final connectionMembers = _memberIds[connection];
    final userId = connectionMembers?.remove(channel);
    if (connectionMembers?.isEmpty ?? false) _memberIds.remove(connection);
    if (userId == null) return;

    final members = _presence[channel];
    final member = members?[userId];
    member?.connections.remove(connection);
    if (member == null || member.connections.isNotEmpty) return;
    members!.remove(userId);
    if (members.isEmpty) _presence.remove(channel);
    await publish(channel, 'pusher_internal:member_removed', {
      'user_id': userId,
    });
  }

  @override
  Future<void> disconnect(SocketConnection connection) async {
    for (final channel in connection.channels.toList()) {
      await unsubscribe(connection, channel);
    }
  }

  @override
  Future<void> publish(
    String channel,
    String event,
    Object? data, {
    String? exceptSocketId,
  }) async {
    final recipients = List<SocketConnection>.of(
      _channels[channel] ?? const {},
    );
    await Future.wait([
      for (final connection in recipients)
        if (connection.id != exceptSocketId)
          connection.send(PusherFrame(event, data, channel: channel)),
    ]);
  }

  @override
  ChannelInfo? info(String channel) {
    final subscribers = _channels[channel];
    if (subscribers == null || subscribers.isEmpty) return null;
    return ChannelInfo(
      name: channel,
      subscriptionCount: subscribers.length,
      members: Map<String, Map<String, Object?>>.unmodifiable({
        for (final entry in (_presence[channel] ?? const {}).entries)
          entry.key: Map<String, Object?>.unmodifiable(entry.value.info),
      }),
    );
  }

  @override
  Iterable<ChannelInfo> get occupiedChannels =>
      _channels.keys.map(info).whereType<ChannelInfo>();

  static Map<String, Object?> _memberInfo(Map<String, Object?> member) {
    final info = member['user_info'];
    return info is Map ? Map<String, Object?>.from(info) : const {};
  }
}

class _PresenceMember {
  _PresenceMember(this.info);

  final Map<String, Object?> info;
  final Set<SocketConnection> connections = {};
}
