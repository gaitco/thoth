import 'dart:io';

import 'package:maat/maat.dart';

import '../config.dart';

class ThothPingCommand extends Command {
  @override
  String get name => 'thoth:ping';

  @override
  String get description => 'Check whether Thoth is healthy';

  @override
  String get signature => '{--host=} {--port=}';

  @override
  Future<int> handle() async {
    final hostOption = option('host');
    final portOption = option('port');
    final config = hostOption == null || portOption == null
        ? this.app.make<ThothConfig>()
        : null;
    final host = hostOption ?? config!.host;
    final port = portOption == null ? config!.port : int.tryParse(portOption);
    if (port == null || port < 1 || port > 65535) {
      error('The port must be between 1 and 65535.');
      return 1;
    }
    final uri = Uri(scheme: 'http', host: host, port: port, path: '/health');
    final client = HttpClient()..connectionTimeout = const Duration(seconds: 3);
    try {
      final response =
          await (await client.getUrl(uri).timeout(const Duration(seconds: 3)))
              .close()
              .timeout(const Duration(seconds: 3));
      if (response.statusCode != HttpStatus.ok) {
        error('Thoth is unavailable at $uri.');
        return 1;
      }
      info('Thoth is healthy at $uri.');
      return 0;
    } on Object {
      error('Thoth is unavailable at $uri.');
      return 1;
    } finally {
      client.close(force: true);
    }
  }
}
