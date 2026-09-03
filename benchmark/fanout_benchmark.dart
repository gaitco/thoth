import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:maat/maat.dart';

const _channel = 'benchmark';
const _event = 'BenchmarkEvent';

Future<void> main() async {
  final environment = Platform.environment;
  final host = environment['PUSHER_HOST'] ?? '127.0.0.1';
  final port = int.parse(environment['PUSHER_PORT'] ?? '6001');
  final appId = environment['PUSHER_APP_ID'] ?? 'benchmark';
  final key = environment['PUSHER_APP_KEY'] ?? 'benchmark-key';
  final secret = environment['PUSHER_APP_SECRET'] ?? 'benchmark-secret';
  final clientCount = int.parse(environment['CLIENTS'] ?? '100');
  final eventCount = int.parse(environment['EVENTS'] ?? '50');
  final warmups = int.parse(environment['WARMUPS'] ?? '3');
  final batchSize = int.parse(environment['CONNECT_BATCH'] ?? '200');
  final hold = Duration(milliseconds: int.parse(environment['HOLD_MS'] ?? '0'));

  final receipts = <int, Set<int>>{};
  final completions = <int, Completer<void>>{};
  void receive(int client, int sequence) {
    final received = receipts[sequence];
    if (received == null || !received.add(client)) return;
    if (received.length == clientCount) completions[sequence]!.complete();
  }

  final sockets = <_BenchmarkSocket>[];
  final connectWatch = Stopwatch()..start();
  try {
    for (var start = 0; start < clientCount; start += batchSize) {
      final end = (start + batchSize).clamp(0, clientCount);
      sockets.addAll(
        await Future.wait([
          for (var index = start; index < end; index++)
            _BenchmarkSocket.connect(host, port, key, index, receive),
        ]),
      );
    }
    connectWatch.stop();
    if (hold > Duration.zero) await Future<void>.delayed(hold);

    final publisher = _Publisher(host, port, appId, key, secret);
    final latencies = <int>[];
    final runWatch = Stopwatch()..start();
    for (var sequence = -warmups; sequence < eventCount; sequence++) {
      receipts[sequence] = <int>{};
      completions[sequence] = Completer<void>();
      final watch = Stopwatch()..start();
      await publisher.publish(sequence);
      await completions[sequence]!.future.timeout(const Duration(seconds: 30));
      watch.stop();
      if (sequence >= 0) latencies.add(watch.elapsedMicroseconds);
      receipts.remove(sequence);
      completions.remove(sequence);
    }
    runWatch.stop();
    publisher.close();

    latencies.sort();
    final measuredMicros = latencies.fold<int>(0, (sum, value) => sum + value);
    final measuredSeconds = measuredMicros / Duration.microsecondsPerSecond;
    stdout.writeln(
      jsonEncode({
        'clients': clientCount,
        'events': eventCount,
        'deliveries': clientCount * eventCount,
        'connect_ms': connectWatch.elapsedMicroseconds / 1000,
        'fanout_messages_per_second':
            clientCount * eventCount / measuredSeconds,
        'p50_ms': _percentile(latencies, 0.50) / 1000,
        'p95_ms': _percentile(latencies, 0.95) / 1000,
        'p99_ms': _percentile(latencies, 0.99) / 1000,
        'wall_ms': runWatch.elapsedMicroseconds / 1000,
      }),
    );
  } finally {
    await Future.wait([for (final socket in sockets) socket.close()]);
  }
}

int _percentile(List<int> values, double percentile) {
  final index = ((values.length - 1) * percentile).round();
  return values[index];
}

class _BenchmarkSocket {
  _BenchmarkSocket(this._socket, this._subscription);

  final WebSocket _socket;
  final StreamSubscription<Object?> _subscription;

  static Future<_BenchmarkSocket> connect(
    String host,
    int port,
    String key,
    int index,
    void Function(int client, int sequence) receive,
  ) async {
    final uri = Uri(
      scheme: 'ws',
      host: host,
      port: port,
      pathSegments: ['app', key],
      queryParameters: const {
        'protocol': '7',
        'client': 'maat-benchmark',
        'version': '0.1',
        'flash': 'false',
      },
    );
    final socket = await WebSocket.connect(uri.toString());
    final ready = Completer<void>();
    late final StreamSubscription<Object?> subscription;
    subscription = socket.listen(
      (message) {
        final frame = Map<String, Object?>.from(
          jsonDecode(message! as String) as Map,
        );
        final event = frame['event'];
        if (event == 'pusher:connection_established') {
          socket.add(
            jsonEncode({
              'event': 'pusher:subscribe',
              'data': {'channel': _channel},
            }),
          );
        } else if (event == 'pusher_internal:subscription_succeeded') {
          if (!ready.isCompleted) ready.complete();
        } else if (event == 'pusher:ping') {
          socket.add(jsonEncode({'event': 'pusher:pong', 'data': {}}));
        } else if (event == _event) {
          final data = frame['data'];
          final payload = Map<String, Object?>.from(
            jsonDecode(data! as String) as Map,
          );
          receive(index, payload['sequence']! as int);
        }
      },
      onError: (Object error, StackTrace stackTrace) {
        if (!ready.isCompleted) ready.completeError(error, stackTrace);
      },
      onDone: () {
        if (!ready.isCompleted) {
          ready.completeError(
            StateError('WebSocket closed before subscribing'),
          );
        }
      },
    );
    await ready.future.timeout(const Duration(seconds: 30));
    return _BenchmarkSocket(socket, subscription);
  }

  Future<void> close() async {
    await _socket.close();
    await _subscription.cancel();
  }
}

class _Publisher {
  _Publisher(this.host, this.port, this.appId, this.key, String secret)
    : _signer = PusherSigner(secret);

  final String host;
  final int port;
  final String appId;
  final String key;
  final PusherSigner _signer;
  final HttpClient _client = HttpClient();

  Future<void> publish(int sequence) async {
    final body = jsonEncode({
      'name': _event,
      'channels': [_channel],
      'data': jsonEncode({'sequence': sequence}),
    });
    final path = '/apps/$appId/events';
    final query = _signer.signHttp(
      method: 'POST',
      path: path,
      body: body,
      key: key,
      timestamp: DateTime.now().millisecondsSinceEpoch ~/ 1000,
    );
    final request = await _client.postUrl(
      Uri(
        scheme: 'http',
        host: host,
        port: port,
        path: path,
        queryParameters: query,
      ),
    );
    request.headers.contentType = ContentType.json;
    final bytes = utf8.encode(body);
    request.contentLength = bytes.length;
    request.add(bytes);
    final response = await request.close();
    await response.drain<void>();
    if (response.statusCode < 200 || response.statusCode >= 300) {
      throw StateError('Publish failed with HTTP ${response.statusCode}');
    }
  }

  void close() => _client.close(force: true);
}
