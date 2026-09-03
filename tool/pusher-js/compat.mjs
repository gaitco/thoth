import Pusher from 'pusher-js';

const port = Number(process.env.THOTH_PORT);
const authPort = Number(process.env.AUTH_PORT);
const key = process.env.PUSHER_APP_KEY;

const pusher = new Pusher(key, {
  cluster: 'mt1',
  wsHost: '127.0.0.1',
  wsPort: port,
  forceTLS: false,
  enabledTransports: ['ws'],
  disableStats: true,
  channelAuthorization: {
    endpoint: `http://127.0.0.1:${authPort}/broadcasting/auth`,
    transport: 'ajax',
  },
});

function subscribed(channel) {
  return new Promise((resolve, reject) => {
    channel.bind('pusher:subscription_succeeded', resolve);
    channel.bind('pusher:subscription_error', reject);
  });
}

function nextEvent(channel) {
  return new Promise((resolve) => channel.bind('CompatEvent', resolve));
}

async function verify() {
  const publicChannel = pusher.subscribe('compat-public');
  const privateChannel = pusher.subscribe('private-compat-private');
  const presenceChannel = pusher.subscribe('presence-compat-presence');
  const events = [
    nextEvent(publicChannel),
    nextEvent(privateChannel),
    nextEvent(presenceChannel),
  ];
  const [, , members] = await Promise.all([
    subscribed(publicChannel),
    subscribed(privateChannel),
    subscribed(presenceChannel),
  ]);
  if (members.me?.id !== '7') throw new Error('presence member was not authorized');
  console.log('READY');
  const payloads = await Promise.all(events);
  if (payloads.some((payload) => payload.id !== 1)) {
    throw new Error('published payload did not round-trip');
  }
  console.log(JSON.stringify({
    public: true,
    private: true,
    presence: true,
    member: members.me.id,
  }));
}

try {
  let timeout;
  await Promise.race([
    verify(),
    new Promise((_, reject) =>
      timeout = setTimeout(
        () => reject(new Error('pusher-js compatibility timed out')),
        10_000,
      ),
    ),
  ]);
  clearTimeout(timeout);
  pusher.disconnect();
} catch (error) {
  pusher.disconnect();
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
}
