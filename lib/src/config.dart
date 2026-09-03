class ThothConfig {
  const ThothConfig({
    required this.appId,
    required this.appKey,
    required this.appSecret,
    this.host = '0.0.0.0',
    this.port = 6001,
    this.clientEvents = false,
    this.activityTimeout = const Duration(seconds: 120),
    this.pongTimeout = const Duration(seconds: 30),
    this.maxConnections = 0,
  });

  final String appId;
  final String appKey;
  final String appSecret;
  final String host;
  final int port;
  final bool clientEvents;
  final Duration activityTimeout;
  final Duration pongTimeout;
  final int maxConnections;
}
