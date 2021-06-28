/* global Package */
Package.describe({
    name: 'socialize:server-presence',
    summary: 'Scalable server presence',
    version: '1.0.4',
    git: 'https://github.com/copleykj/socialize-server-presence.git',
});

Package.onUse(function _(api) {
    api.versionsFrom(['1.10.2', '2.3']);

    api.use(['ecmascript', 'mongo'], 'server');

    api.mainModule('server-presence.js', 'server');
});
