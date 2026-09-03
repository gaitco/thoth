import 'dart:convert';

class PusherFrame {
  const PusherFrame(this.event, this.data, {this.channel});

  final String event;
  final Object? data;
  final String? channel;

  String encode() => jsonEncode({
    'event': event,
    'data': jsonEncode(data),
    if (channel != null) 'channel': channel,
  });
}
