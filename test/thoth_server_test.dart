import 'dart:convert';
import 'dart:io';

import 'package:test/test.dart';
import 'package:thoth/thoth.dart';

void main() {
  test('starts on an ephemeral port and releases it on close', () async {
    final thoth = ThothServer(
      const ThothConfig(
        appId: 'app-id',
        appKey: 'app-key',
        appSecret: 'app-secret',
      ),
    );
    final server = await thoth.start(host: '127.0.0.1', port: 0);
    final port = server.port;
    final client = HttpClient();
    final response = await (await client.getUrl(
      Uri.parse('http://127.0.0.1:$port/health'),
    )).close();
    expect(response.statusCode, 200);
    expect(await utf8.decoder.bind(response).join(), '{"status":"ok"}');
    client.close();

    await thoth.close();
    await thoth.close();
    final rebound = await HttpServer.bind('127.0.0.1', port);
    await rebound.close(force: true);
  });
}
