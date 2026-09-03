import 'dart:async';

import 'package:web_socket_channel/web_socket_channel.dart';

import 'frame.dart';

class SocketConnection {
  SocketConnection(this.id, this.sink);

  final String id;
  final WebSocketSink sink;
  final Set<String> channels = {};
  Timer? activityTimer;
  Timer? pongTimer;

  Future<void> send(PusherFrame frame) async => sink.add(frame.encode());

  Future<void> close([int? code, String? reason]) => sink.close(code, reason);

  void cancelTimers() {
    activityTimer?.cancel();
    pongTimer?.cancel();
  }
}
