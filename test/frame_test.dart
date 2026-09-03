import 'dart:convert';

import 'package:test/test.dart';
import 'package:thoth/thoth.dart';

void main() {
  test('protocol frame encodes data as a JSON string', () {
    final encoded = PusherFrame('pusher:connection_established', {
      'socket_id': '123.456',
      'activity_timeout': 120,
    }).encode();

    expect(jsonDecode(encoded), {
      'event': 'pusher:connection_established',
      'data': '{"socket_id":"123.456","activity_timeout":120}',
    });
  });
}
