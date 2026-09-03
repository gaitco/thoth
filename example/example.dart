import 'package:thoth_realtime/thoth_realtime.dart';

void main() {
  final frame = PusherFrame('task.updated', {
    'id': 1,
    'completed': true,
  }, channel: 'tasks');

  print(frame.encode());
}
