// Required modules
var Etcd = require('node-etcd');
var Hapi = require('hapi');
var moment = require('moment');
var config = require('./config.json');
var _ = require('lodash');

// Connect to etcd
var etcd = new Etcd(config.etcd);

// A list of listeners for client keys
var listeners = {};

// Store info about clients
var clients = {};

// Create a server with a host and port
var server = new Hapi.Server();
server.connection({
    port: process.env.PORT || 8080
});

// Healthcheck route
server.route({
    method: 'GET',
    path:'/v1/healthcheck',
    handler: function(request, reply) {
        return reply('OK');
    }
});

// Status route
server.route({
    method: 'GET',
    path: '/v1/status',
    handler: function(request, reply) {
        return reply(clients);
    }
});

// Used by clients to post their rate limit usage and recieve the global status
server.route({
    method: 'POST',
    path:'/v1/frontends/{frontend}/clients/{key}',
    handler: handlePostRequest
});

// Start the server
server.start();

// Handler for incoming post requests
function handlePostRequest(request, reply) {
    var frontend = request.params.frontend,
        key = request.params.key;

    // Generate the client object key
    var client_key = request.params.frontend + ':' + request.params.key;

    // Get the current second
    var second = moment().seconds();

    // Get the usage for the client
    var usage;

    // Check for valid frontend
    if (typeof(request.params.frontend) !== 'string') {
        return reply({
            error: 'No frontend field in params!'
        }).code(400);
    }

    // Check for valid key
    if (typeof(request.params.key) !== 'string') {
        return reply({
            error: 'No key field in params!'
        }).code(400);
    }

    // Check for a valid payload
    if (typeof(request.payload.usage) !== 'string') {
        return reply({
            error: 'No usage field in payload!'
        }).code(400);
    } else {
        usage = parseInt(request.payload.usage, 10);
    }

    // Check for a listener for this client to see if it's data changes in etcd
    if (typeof(listeners[client_key]) !== 'function') {
        listeners[client_key] = etcd.watcher('/vulcand/frontends/' + frontend + '/middlewares/gatekeeper');
        listeners[client_key].on('change', function(err, result) {
            try {
                // Parse the etcd data and extract the rate
                var value = JSON.parse(result.node.value);
            } catch (e) {
                return reply({
                    error: 'Error parsing etcd data'
                }).code(500);
            }

            // Retrieve the rate from the etcd data
            var rate = value.Middleware.Keys[request.params.key].Rate;

            // Does the client already exist, if so just update the rate
            if (typeof(clients[client_key]) === 'object') {
                clients[client_key].rate = rate;
            // Otherwise create the client with some default values
            } else {
                clients[client_key] = {
                    rate: rate,
                    used: 0,
                    remaining: rate,
                    window: _.range(60).map(function() {
                        return {
                            value: 0,
                            last_updated: moment().unix()
                        };
                    })
                };
            }

            return reply(processClientRequest(key, frontend, client_key, second, usage));
        });
    } else {
        return reply(processClientRequest(key, frontend, client_key, second, usage));
    }
}

// Process the client request
function processClientRequest(frontend, client_key, second, usage) {
    // Check for a client object
    if (typeof(clients[client_key]) !== 'object') {
        etcd.get('/vulcand/frontends/' + frontend + '/middlewares/gatekeeper', function(err, result) {
            try {
                // Parse the etcd data and extract the rate
                var value = JSON.parse(result.node.value);
            } catch (e) {
                return reply({
                    error: 'Error parsing etcd data'
                }).code(500);
            }

            // Retrieve the rate from the etcd data
            var rate = value.Middleware.Keys[key].Rate;

            clients[client_key] = {
                rate: rate,
                used: 0,
                remaining: rate,
                window: _.range(60).map(function() {
                    return {
                        value: 0,
                        last_updated: moment().unix()
                    };
                })
            };

            return incrementRates(client_key, second, usage);
        });
    } else {
        return incrementRates(client_key, second, usage);
    }
}

// Increment the rates for the given client for this second
function incrementRates(client_key, second, usage) {
    // Reset any values older than 60 seconds
    clients[client_key].window = clients[client_key].window.map(function(value) {
        if (value.last_updated <= (moment().unix() - 60)) {
            return {
                value: 0,
                last_updated: moment().unix()
            };
        }

        return value;
    });

    // Increment the current second in the window
    clients[client_key].window[second].value += usage;
    clients[client_key].window[second].last_updated = moment().unix();

    // Calculate how many requests they have remaining
    var used = 0;

    // Add every value up
    clients[client_key].window.forEach(function(second) {
        used += second.value;
    });

    // Substract from the clients rate allowance
    var remaining = clients[client_key].rate - used;

    // Don't allow the remaining rate to be below 0
    clients[client_key].remaining = remaining > 0 ? remaining : 0;

    // Add the amount used
    clients[client_key].used = used;

    return {
        rate: clients[client_key].rate,
        remaining: clients[client_key].remaining
    };
}
