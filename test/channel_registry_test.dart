import 'dart:async';
import 'dart:convert';

import 'package:test/test.dart';
import 'package:thoth_realtime/thoth_realtime.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

class RecordingSink implements WebSocketSink {
  final values = <Object?>[];
  final _done = Completer<void>();

  @override
  void add(Object? data) => values.add(data);

  @override
  void addError(Object error, [StackTrace? stackTrace]) => throw error; // coverage:ignore-line

  @override
  Future<void> addStream(Stream<Object?> stream) async =>
      values.addAll(await stream.toList());

  @override
  Future<void> close([int? closeCode, String? closeReason]) async {
    if (!_done.isCompleted) _done.complete();
  }

  @override
  Future<void> get done => _done.future;
}

class ThrowingMemberRemovedSink extends RecordingSink {
  @override
  void add(Object? data) {
    if (data is String && data.contains('pusher_internal:member_removed')) {
      throw StateError('member removal failed');
    }
    super.add(data);
  }
}

SocketConnection connection(String id, RecordingSink sink) =>
    SocketConnection(id, sink);

void main() {
  test('publish reaches subscribers except the excluded socket', () async {
    final registry = InMemoryChannelRegistry();
    final firstSink = RecordingSink();
    final secondSink = RecordingSink();
    final first = connection('1.1', firstSink);
    final second = connection('2.2', secondSink);
    await registry.subscribe(first, 'tasks');
    await registry.subscribe(second, 'tasks');

    await registry.publish('tasks', 'TaskChanged', {
      'id': 1,
    }, exceptSocketId: '1.1');

    expect(firstSink.values, isEmpty);
    expect(jsonDecode(secondSink.values.single as String), {
      'event': 'TaskChanged',
      'data': '{"id":1}',
      'channel': 'tasks',
    });
  });

  test('duplicate subscription is idempotent', () async {
    final registry = InMemoryChannelRegistry();
    final socket = connection('1.1', RecordingSink());

    await registry.subscribe(socket, 'tasks');
    await registry.subscribe(socket, 'tasks');

    expect(registry.info('tasks')?.subscriptionCount, 1);
  });

  test('disconnect removes empty channels', () async {
    final registry = InMemoryChannelRegistry();
    final socket = connection('1.1', RecordingSink());
    await registry.subscribe(socket, 'tasks');

    await registry.disconnect(socket);

    expect(registry.info('tasks'), isNull);
  });

  test('presence member remains until its final connection leaves', () async {
    final registry = InMemoryChannelRegistry();
    final first = connection('1.1', RecordingSink());
    final second = connection('2.2', RecordingSink());
    const member = {
      'user_id': '7',
      'user_info': {'name': 'Ada'},
    };
    await registry.subscribe(first, 'presence-room', member: member);
    await registry.subscribe(second, 'presence-room', member: member);

    await registry.disconnect(first);
    expect(registry.info('presence-room')?.members.keys, ['7']);

    await registry.disconnect(second);
    expect(registry.info('presence-room'), isNull);
  });

  test(
    'disconnect removes every membership when a presence departure fails',
    () async {
      final registry = InMemoryChannelRegistry();
      final leaving = connection('1.1', RecordingSink());
      final remaining = connection('2.2', ThrowingMemberRemovedSink());
      const leavingMember = {
        'user_id': '1',
        'user_info': {'name': 'Ada'},
      };
      const remainingMember = {
        'user_id': '2',
        'user_info': {'name': 'Grace'},
      };
      await registry.subscribe(
        leaving,
        'presence-first',
        member: leavingMember,
      );
      await registry.subscribe(
        remaining,
        'presence-first',
        member: remainingMember,
      );
      await registry.subscribe(
        leaving,
        'presence-second',
        member: leavingMember,
      );

      await expectLater(
        registry.disconnect(leaving),
        throwsA(isA<StateError>()),
      );

      expect(registry.info('presence-first')?.members.keys, ['2']);
      expect(registry.info('presence-second'), isNull);
      expect(leaving.channels, isEmpty);
    },
  );
}
