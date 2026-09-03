import 'dart:io';

import 'package:shelf/shelf_io.dart' as shelf_io;

import 'config.dart';
import 'thoth_handler.dart';

class ThothServer {
  ThothServer(this.config, {ThothHandler? handler})
    : handler = handler ?? ThothHandler(config);

  final ThothConfig config;
  final ThothHandler handler;
  HttpServer? _server;
  Future<void>? _closing;

  Future<HttpServer> start({String? host, int? port}) async {
    if (_server != null) throw StateError('Thoth is already running');
    return _server = await shelf_io.serve(
      handler.handler,
      host ?? config.host,
      port ?? config.port,
    );
  }

  Future<void> close({bool force = false}) =>
      _closing ??= _server?.close(force: force) ?? Future.value();
}
