import 'dart:async';
import 'dart:io';

import 'package:shelf/shelf_io.dart' as shelf_io;

import 'config.dart';
import 'thoth_handler.dart';

class ThothServer {
  ThothServer(
    this.config, {
    ThothHandler? handler,
    this.shutdownTimeout = const Duration(seconds: 5),
  }) : handler = handler ?? ThothHandler(config);

  final ThothConfig config;
  final ThothHandler handler;
  final Duration shutdownTimeout;
  HttpServer? _server;
  Future<void>? _closing;

  Future<HttpServer> start({String? host, int? port}) async {
    if (_server != null) throw StateError('Thoth is already running');
    if (config.appId.isEmpty ||
        config.appKey.isEmpty ||
        config.appSecret.isEmpty) {
      throw StateError('Thoth app id, key, and secret are required.');
    }
    return _server = await shelf_io.serve(
      handler.handler,
      host ?? config.host,
      port ?? config.port,
    );
  }

  Future<void> close({bool force = false}) => _closing ??= _close(force);

  Future<void> _close(bool force) async {
    final server = _server;
    if (server == null) return;
    if (force) {
      unawaited(handler.shutdown());
      await server.close(force: true);
      return;
    }

    final listenerClosed = server.close(force: false);
    final connectionsClosed = handler.shutdown();
    try {
      await Future.wait([
        listenerClosed,
        connectionsClosed,
      ]).timeout(shutdownTimeout);
    } on TimeoutException {
      await server.close(force: true);
    }
  }
}
