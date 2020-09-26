using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Strombus.Redis;
using Strombus.ServerShared;
using Microsoft.AspNetCore.Builder;
using Newtonsoft.Json;
using System.Text;

namespace Strombus.EventService
{
    public class EventServiceMiddleware
    {
        private readonly RequestDelegate _next;

        private const string APIPATH_ACCOUNTS = "accounts";
        private const string APIPATH_EVENTS = "events";
        private const string APIPATH_SUBSCRIPTIONS = "subscriptions";
        private const string APIPATH_NOTIFICATIONS = "notifications";

        private const int MAX_PAYLOAD_SIZE_CREATE_SUBSCRIPTION = 32768;
        private const int MAX_PAYLOAD_SIZE_SUBSCRIBE_EVENT = 32768;
        private const int MAX_PAYLOAD_SIZE_UNSUBSCRIBE_EVENT = 32768;

        private RedisClient _redisClient;

        private const string REDIS_PREFIX_SUBSCRIPTION = "subscription";
        private const string REDIS_PREFIX_SEPARATOR = ":";

        public EventServiceMiddleware(RequestDelegate next)
        {
            // start up our EventDispatcher
            Task.Run(async () => await EventDispatcher.DispatchEventsAsync());

            _next = next;
        }

        public async Task Invoke(HttpContext context)
        {
            // ensure that we have a connection to the Redis server
            // NOTE: we could do this in our constructor, but we have intentionally avoided putting any code there which could block or throw an exception.  Instead we lazily load the redis client here.
            if (_redisClient == null)
            {
                // NOTE: this call will attempt to create the connection if it does not already exists (and will "block" in an async-friendly fashion)
                _redisClient = await Singletons.GetRedisClientAsync();
            }

            // process non-websocket HTTP requests
            if (context.WebSockets.IsWebSocketRequest == false)
            {
                string path = context.Request.Path.ToString();
                // for the event server: pull out the optional scope portion of the uri
                string scope = null;
                if (path.IndexOf('(') >= 0)
                {
                    scope = path.Substring(path.IndexOf('(') + 1);
                    if (scope.Length >= 1 && scope.Substring(scope.Length - 1, 1) == ")")
                    {
                        scope = scope.Substring(0, scope.Length - 1);
                        // scope was formatted appropriately; remove the scope suffix from the path
                        path = path.Substring(0, path.IndexOf('('));
                    }
                    else
                    {
                        // scope was not formatted properly
                        scope = null;
                    }
                }
                //
                List<string> pathHierarchy = new List<string>(path.Split(new char[] { '/' }));
                // if the last entry is a forward-slash, remove it now; the trailing forward-slash is optional and should be ignored
                if (pathHierarchy.Count > 0 && pathHierarchy[pathHierarchy.Count - 1] == string.Empty)
                {
                    pathHierarchy.RemoveAt(pathHierarchy.Count - 1);
                }
                int pathHierarchyLength = pathHierarchy.Count;
                int pathHierarchyIndex = 0;

                /* VALID PATH OPTIONS: (+ for implemented, - for not yet implemented)
                 * - /accounts/<account_id>/subscriptions
                 * - /accounts/<account_id>/subscriptions/<subscription_id>/events
                 * - /accounts/<account_id>/subscriptions/<subscription_id>/events/<event_id>
                 * - /accounts/<account_id>/subscriptions/<subscription_id>/notifications
                 */

                string accountId = null;

                // the first element should be empty (i.e. the leading slash)
                if (pathHierarchy[0] != null && pathHierarchy[0] == string.Empty)
                {
                    // skip the leading slash
                    pathHierarchyIndex += 1;

                    // determine the request type from the remainder of the hierarchy
                    int pathHierarchyElementsRemaining = pathHierarchyLength - pathHierarchyIndex;

                    // get our account ID, if specified in the path
                    if ((pathHierarchyLength >= pathHierarchyIndex + 2) && (pathHierarchy[pathHierarchyIndex] == APIPATH_ACCOUNTS))
                    {
                        accountId = pathHierarchy[pathHierarchyIndex + 1];
                        pathHierarchyIndex += 2;
                    }
                    if (accountId == null)
                    {
                        // NOTE: we might infer the account ID from the supplied authentication credentials
                        //if (oauth2Token != null)
                        //{
                        //    accountId = oauth2Token?.AccountId;
                        //}
                    }

                    if (pathHierarchyElementsRemaining == 1 && pathHierarchy[pathHierarchyIndex + 0].ToLowerInvariant() == APIPATH_SUBSCRIPTIONS)
                    {
                        // PATH: /accounts/<account_id>/subscriptions
                        string[] supportedMethods = new string[] { "POST" };
                        switch (context.Request.Method.ToUpperInvariant())
                        {
                            case "OPTIONS":
                                HttpHelper.SetHttpResponseNoContent_ForOptionsMethod(context, supportedMethods);
                                return;
                            case "POST":
                                await CreateSubscriptionAsync(context, accountId).ConfigureAwait(false);
                                return;
                            default:
                                HttpHelper.SetHttpResponseMethodNotAllowed(context, supportedMethods);
                                return;
                        }
                    }
                    else if (pathHierarchyElementsRemaining == 2 && pathHierarchy[pathHierarchyIndex + 0].ToLowerInvariant() == APIPATH_SUBSCRIPTIONS &&
                                                                    pathHierarchy[pathHierarchyIndex + 1] != string.Empty)
                    {
                        // PATH: /v1/accounts/<account_id>/subscriptions/<subscription_id>
                        string subscriptionId = pathHierarchy[pathHierarchyIndex + 1];
                        //
                        string[] supportedMethods = new string[] { "DELETE" };
                        switch (context.Request.Method.ToUpperInvariant())
                        {
                            case "OPTIONS":
                                HttpHelper.SetHttpResponseNoContent_ForOptionsMethod(context, supportedMethods);
                                return;
                            case "DELETE":
                                await DeleteSubscriptionAsync(context, accountId, subscriptionId).ConfigureAwait(false);
                                return;
                            default:
                                HttpHelper.SetHttpResponseMethodNotAllowed(context, supportedMethods);
                                return;
                        }
                    }
                    else if (pathHierarchyElementsRemaining == 3 && pathHierarchy[pathHierarchyIndex + 0].ToLowerInvariant() == APIPATH_SUBSCRIPTIONS &&
                                                                   pathHierarchy[pathHierarchyIndex + 1] != string.Empty &&
                                                                   pathHierarchy[pathHierarchyIndex + 2].ToLowerInvariant() == APIPATH_EVENTS)

                    {
                        // PATH: /v1/accounts/<account_id>/subscriptions/<subscription_id>/events
                        string subscriptionId = pathHierarchy[pathHierarchyIndex + 1];
                        //
                        string[] supportedMethods = new string[] { "DELETE", "GET", "POST" };
                        switch (context.Request.Method.ToUpperInvariant())
                        {
                            case "OPTIONS":
                                HttpHelper.SetHttpResponseNoContent_ForOptionsMethod(context, supportedMethods);
                                return;
                            case "DELETE":
                                await DeleteEventsAsync(context, accountId, subscriptionId).ConfigureAwait(false);
                                return;
                            case "GET":
                                await GetEventsAsync(context, accountId, subscriptionId).ConfigureAwait(false);
                                return;
                            case "POST":
                                await SubscribeEventAsync(context, accountId, subscriptionId).ConfigureAwait(false);
                                return;
                            default:
                                HttpHelper.SetHttpResponseMethodNotAllowed(context, supportedMethods);
                                return;
                        }
                    }
                    else if (pathHierarchyElementsRemaining == 4 && pathHierarchy[pathHierarchyIndex + 0].ToLowerInvariant() == APIPATH_SUBSCRIPTIONS &&
                                                                   pathHierarchy[pathHierarchyIndex + 1] != string.Empty &&
                                                                   pathHierarchy[pathHierarchyIndex + 2].ToLowerInvariant() == APIPATH_EVENTS &&
                                                                   pathHierarchy[pathHierarchyIndex + 3] != string.Empty)
                    {
                        // PATH: /v1/accounts/<account_id>/subscriptions/<subscription_id>/events/<event_name>(optional_scope)
                        string subscriptionId = pathHierarchy[pathHierarchyIndex + 1];
                        string eventName = pathHierarchy[pathHierarchyIndex + 3];
                        //
                        string[] supportedMethods = new string[] { "DELETE", "GET" };
                        switch (context.Request.Method.ToUpperInvariant())
                        {
                            case "OPTIONS":
                                HttpHelper.SetHttpResponseNoContent_ForOptionsMethod(context, supportedMethods);
                                return;
                            case "DELETE":
                                await UnsubscribeEventAsync(context, accountId, subscriptionId, eventName, scope).ConfigureAwait(false);
                                return;
                            case "GET":
                                await GetEventAsync(context, accountId, subscriptionId, eventName, scope).ConfigureAwait(false);
                                return;
                            default:
                                HttpHelper.SetHttpResponseMethodNotAllowed(context, supportedMethods);
                                return;
                        }
                    }
                    else if (pathHierarchyElementsRemaining == 3 && pathHierarchy[pathHierarchyIndex + 0].ToLowerInvariant() == APIPATH_SUBSCRIPTIONS &&
                                                                   pathHierarchy[pathHierarchyIndex + 1] != string.Empty &&
                                                                   pathHierarchy[pathHierarchyIndex + 2].ToLowerInvariant() == APIPATH_NOTIFICATIONS)

                    {
                        // PATH: /v1/accounts/<account_id>/subscriptions/<subscription_id>/notifications
                        string subscriptionId = pathHierarchy[pathHierarchyIndex + 1];
                        //
                        string[] supportedMethods = new string[] { "GET" };
                        switch (context.Request.Method.ToUpperInvariant())
                        {
                            case "OPTIONS":
                                HttpHelper.SetHttpResponseNoContent_ForOptionsMethod(context, supportedMethods);
                                return;
                            case "GET":
                                await GetNotificationsAsync(context, accountId, subscriptionId).ConfigureAwait(false);
                                return;
                            default:
                                HttpHelper.SetHttpResponseMethodNotAllowed(context, supportedMethods);
                                return;
                        }
                    }
                    // else if...
                }
            }
            await _next(context).ConfigureAwait(false);
        }

        private struct AddSubscriptionEventMultiRequest
        {
            public SubscriptionEvent[] items;
        }
        private struct CreateSubscriptionRequest
        {
            public AddSubscriptionEventMultiRequest events;
        }
        private async Task CreateSubscriptionAsync(HttpContext context, string accountId)
        {
            if (await HttpHelper.VerifyAcceptHeaderIsJson(context).ConfigureAwait(false) == false)
                return;

            /* extract the (optional) account-id and server number from our hostname
             * if an account-id is provided, we will mandate that the accountId matches (and that the token's accountId matches)
             * we will also validate that the server hostname is a principal service host, to keep things clean for clients if we multi-home both principal and secondary services on one server */
            var serverDetails = ParsingHelper.ExtractServerDetailsFromHostname(context.Request.Headers["Host"]);
            // if the requested server is not parseable or otherwise blatantly invalid, return an error
            if (serverDetails == null || string.IsNullOrWhiteSpace(serverDetails?.ServerType) || serverDetails?.ServerId == null)
            {
                HttpHelper.SetHttpResponseInternalServerError(context);
                return;
            }
            // record the account id that this server hostname is restricted to, if any
            string restrictToAccountId = serverDetails?.AccountId;
            // build the full serverId
            string serverId = serverDetails?.ToAccountIdServerTypeServiceIdString();

            // verify that our server acts as a server for the specific accountId (and specific serverId, if one was provided in the hostname)
            var verifyAccountResult = await RedisHelper.VerifyServerIdAsync(_redisClient, "event", accountId, false, serverId);
            if (verifyAccountResult == false)
            {
                /* redirect to the root api server "https://<accountId>-api.example.com" instead */
                // NOTE: we use a temporary redirect here, as some scenarios where our server is being called with an invalid accountId/serverId are temporary
                HttpHelper.SetHttpResponseTemporaryRedirect(context, ParsingHelper.RewriteUrlWithServiceHostname(context, "api", accountId));
                return;
            }

            // make sure that the request is not too large
            if (context.Request.ContentLength > MAX_PAYLOAD_SIZE_CREATE_SUBSCRIPTION)
            {
                HttpHelper.SetHttpResponsePayloadTooLarge(context);
                return;
            }

            // retrieve the request payload
            byte[] buffer = new byte[context.Request.ContentLength ?? 0];
            int bytesRead = await context.Request.Body.ReadAsync(buffer, 0, buffer.Length).ConfigureAwait(false);
            CreateSubscriptionRequest? request = null;
            if (bytesRead > 0)
            {
                // deserialize the (JSON) request payload
                try
                {
                    request = JsonConvert.DeserializeObject<CreateSubscriptionRequest>(Encoding.UTF8.GetString(buffer, 0, bytesRead));
                }
                catch (JsonException)
                {
                    // if the request was malformed, return a generic error.
                    HttpHelper.SetHttpResponseBadRequest(context);
                    return;
                }
            }
            // parse the request
            List<SubscriptionEvent> requestedEvents = null; 
            if (request != null && request?.events != null && request?.events.items != null)
            {
                requestedEvents = new List<SubscriptionEvent>(request.Value.events.items);
            }

            Subscription subscription = Subscription.NewSubscription(serverId, clientId, accountId, userId);

            await subscription.SaveSubscriptionAsync();

            if (requestedEvents != null)
            {
                foreach (SubscriptionEvent requestedEvent in requestedEvents)
                {
                    await SubscriptionEvent.AddEventToSubscriptionAsync(accountId, subscription.SubscriptionId, requestedEvent.name, requestedEvent.scope);
                }
            }
        }

        private async Task DeleteSubscriptionAsync(HttpContext context, string accountId, string subscriptionId)
        {
            throw new NotImplementedException();
        }

        private async Task DeleteEventsAsync(HttpContext context, string accountId, string subscriptionId)
        {
            throw new NotImplementedException();
        }

        private async Task GetEventsAsync(HttpContext context, string accountId, string subscriptionId)
        {
            throw new NotImplementedException();
        }

        private async Task SubscribeEventAsync(HttpContext context, string accountId, string subscriptionId)
        {
            if (await HttpHelper.VerifyContentTypeHeaderIsJson(context).ConfigureAwait(false) == false)
                return;
            if (await HttpHelper.VerifyAcceptHeaderIsJson(context).ConfigureAwait(false) == false)
                return;

            /* extract the (optional) account-id and server number from our hostname
             * if an account-id is provided, we will mandate that the accountId matches (and that the token's accountId matches)
             * we will also validate that the server hostname is a principal service host, to keep things clean for clients if we multi-home multiple services on one server */
            var serverDetails = ParsingHelper.ExtractServerDetailsFromHostname(context.Request.Headers["Host"]);
            // if the requested server is not parseable or otherwise blatantly invalid, return an error
            if (serverDetails == null || string.IsNullOrWhiteSpace(serverDetails?.ServerType) || serverDetails?.ServerId == null)
            {
                HttpHelper.SetHttpResponseInternalServerError(context);
                return;
            }
            // record the account id that this server hostname is restricted to, if any
            string restrictToAccountId = serverDetails?.AccountId;
            // build the full serverId
            string serverId = serverDetails?.ToAccountIdServerTypeServiceIdString();

            // verify that our server acts as a server for the specific accountId (and specific serverId, if one was provided in the hostname)
            var verifyAccountResult = await RedisHelper.VerifyServerIdAsync(_redisClient, "event", accountId, false, serverId);
            if (verifyAccountResult == false)
            {
                /* redirect to the root api server "https://<accountId>-api.example.com" instead */
                // NOTE: we use a temporary redirect here, as some scenarios where our server is being called with an invalid accountId/serverId are temporary
                HttpHelper.SetHttpResponseTemporaryRedirect(context, ParsingHelper.RewriteUrlWithServiceHostname(context, "api", accountId));
                return;
            }

            // make sure that the request is not too large
            if (context.Request.ContentLength > MAX_PAYLOAD_SIZE_SUBSCRIBE_EVENT)
            {
                HttpHelper.SetHttpResponsePayloadTooLarge(context);
                return;
            }

            // retrieve the request payload
            byte[] buffer = new byte[context.Request.ContentLength ?? 0];
            int bytesRead = await context.Request.Body.ReadAsync(buffer, 0, buffer.Length).ConfigureAwait(false);
            if (bytesRead == 0)
            {
                throw new Exception(); 
            }

            SubscriptionEvent request = null;
            // deserialize the (JSON) request payload
            try
            {
                request = JsonConvert.DeserializeObject<SubscriptionEvent>(Encoding.UTF8.GetString(buffer, 0, bytesRead));
            }
            catch (JsonException)
            {
                // if the request was malformed, return a generic error.
                HttpHelper.SetHttpResponseBadRequest(context);
                return;
            }

            await SubscriptionEvent.AddEventToSubscriptionAsync(accountId, subscriptionId, request.name, request.scope);
        }

        private async Task UnsubscribeEventAsync(HttpContext context, string accountId, string subscriptionId, string eventName, string eventScope)
        {
            /* extract the (optional) account-id and server number from our hostname
             * if an account-id is provided, we will mandate that the accountId matches (and that the token's accountId matches)
             * we will also validate that the server hostname is a "principal service" host, to keep things clean for clients if we multi-home both principal and secondary services on one server */
            var serverDetails = ParsingHelper.ExtractServerDetailsFromHostname(context.Request.Headers["Host"]);
            // if the requested server is not parseable or otherwise blatantly invalid, return an error
            if (serverDetails == null || string.IsNullOrWhiteSpace(serverDetails?.ServerType) || serverDetails?.ServerId == null)
            {
                HttpHelper.SetHttpResponseInternalServerError(context);
                return;
            }
            // record the account id that this server hostname is restricted to, if any
            string restrictToAccountId = serverDetails?.AccountId;
            // build the full serverId
            string serverId = serverDetails?.ToServerTypeServerIdIdentifierString();

            // verify that our server acts as a server for the specific accountId (and specific serverId, if one was provided in the hostname)
            var verifyAccountResult = await RedisHelper.VerifyServerIdAsync(_redisClient, "event", accountId, false, serverId);
            if (verifyAccountResult == false)
            {
                /* redirect to the root api server "https://<accountId>-api.example.com" instead */
                // NOTE: we use a temporary redirect here, as some scenarios where our server is being called with an invalid accountId/serverId are temporary
                HttpHelper.SetHttpResponseTemporaryRedirect(context, ParsingHelper.RewriteUrlWithServiceHostname(context, "api", accountId));
                return;
            }

            // make sure that the request is not too large
            if (context.Request.ContentLength > MAX_PAYLOAD_SIZE_UNSUBSCRIBE_EVENT)
            {
                HttpHelper.SetHttpResponsePayloadTooLarge(context);
                return;
            }

            await SubscriptionEvent.RemoveEventFromSubscriptionAsync(accountId, subscriptionId, eventName, eventScope);
        }

        private async Task GetEventAsync(HttpContext context, string accountId, string subscriptionId, string eventName, string scope)
        {
            throw new NotImplementedException();
        }

        const int MAX_GET_NOTIFICATIONS_COUNT = 100;

        struct NotificationResult
        {
            public long id;
            public string data;
        }
        // this function will retrieve notifications from the specified subscription
        /* NOTES:
         * 1. if the request is an AJAX request, and if one or more notifications are available, the call will return immediately.
         * 2. if the request is an AJAX request, and if no notifications are currently available, the call will long-poll-wait up to "timeout_ms" before returning.
         * 3. if the request is an SSE (EventSource) request, the call will immediately return any available notifications and will then wait indefinitely. */
        private async Task GetNotificationsAsync(HttpContext context, string accountId, string subscriptionId)
        {
            // determine our request type (AJAX/long-polling vs. SSE, etc.)
            GetNotificationType getNotificationType = GetNotificationType.LongPolling; // default to long polling
            //
            string acceptHeader = context.Request.Headers["Accept"];
            if (acceptHeader != null && acceptHeader == "text/event-stream")
            {
                getNotificationType = GetNotificationType.ServerSentEvent;
            }

            // get our 'from' value, if provided; if not provided, default to one.  NOTE: our notification IDs are one-based.
            long? idRangeFrom = null;
            long? numNotificationsToGet = null;
            switch (getNotificationType)
            {
                case GetNotificationType.LongPolling:
                    idRangeFrom = RetrieveQueryParameterAsLong(context, "from");
                    numNotificationsToGet = RetrieveQueryParameterAsLong(context, "count") ?? MAX_GET_NOTIFICATIONS_COUNT;
                    break;
                case GetNotificationType.ServerSentEvent:
                    long? lastEventId = RetrieveHeaderValueAsLong(context, "Last-Event-ID");
                    // if we were supplied with a lastEventId, add one to this value to determine our "from" value
                    if (lastEventId != null)
                    {
                        idRangeFrom = lastEventId + 1;
                    }
                    break;
            }
            // if the from value is out of range, return an error
            if (idRangeFrom != null && (idRangeFrom < 0 || idRangeFrom > Math.Pow(2, 53)))
            {
                HttpHelper.SetHttpResponseBadRequest(context);
                return;
            }
            // if the count is out of range, return an error
            if (numNotificationsToGet != null && (numNotificationsToGet < 0 || numNotificationsToGet > Math.Pow(2, 53)))
            {
                HttpHelper.SetHttpResponseBadRequest(context);
                return;
            }
            // if the count is greater than our per-call maximum, reduce the count to our per-count maximujm
            if (numNotificationsToGet > MAX_GET_NOTIFICATIONS_COUNT)
            {
                numNotificationsToGet = MAX_GET_NOTIFICATIONS_COUNT;
            }
            // if the caller did not specify a "from" value, default to id #1
            if (idRangeFrom == null)
            {
                idRangeFrom = 1;
            }

            // we will base all of our keys off of a base subscription key
            string subscriptionKeyBase = REDIS_PREFIX_SUBSCRIPTION + REDIS_PREFIX_SEPARATOR + accountId.ToString() + "/" + subscriptionId.ToString();

            /* check if our subscription has been deleted; if so, then we should return HTTP 404 Not Found */
            if (await _redisClient.ExistsAsync(subscriptionKeyBase).ConfigureAwait(false) == 0)
            {
                HttpHelper.SetHttpResponseNotFound(context);
                return;
            }

            // generate Lua script (which will assign our HTTP request to the subscription and close out any previously-waiting connection)
            // NOTE: this is necessary so that we don't have two competing HTTP requests blocking for the same notifications--especially because
            //       some of our logic intentionally deletes "old" notifications after they have been confirmed as received by the HTTP caller.
            StringBuilder luaBuilder = new StringBuilder();
            List<string> arguments = new List<string>();
            int iArgument = 1;
            // retrieve the subscription's current connectionId; store it in a temporary variable named "oldConnectionId".
            // NOTE: HGET returns nil if the field doesn't exist--and Lua converts the returned "nil" value to "boolean false".
            luaBuilder.Append("local oldConnectionId = redis.call(\"HGET\", KEYS[1], \"connection-id\")\n");
            // increment the connectionId so that the previous connection is no longer the current connection; store the new value (our connectionId) in a variable named "connectionId".
            luaBuilder.Append("local connectionId = redis.call(\"HINCRBY\", KEYS[1], \"connection-id\", 1)\n");
            // return our new connectionId; if the oldConnectionId existed, return it as a second value
            // NOTE: 
            luaBuilder.Append("if oldConnectionId == false then\n");
            luaBuilder.Append("  return {connectionId}");
            luaBuilder.Append("else\n");
            // NOTE: we haev to convert oldConnectionId from a "Redis Bulk Reply" to a "Redis Number", or else we'd get back a byte array
            luaBuilder.Append("  return {connectionId, tonumber(oldConnectionId)}");
            luaBuilder.Append("end\n");
            //
            List<string> keys = new List<string>();
            keys.Add(subscriptionKeyBase);
            object[] connectionsAsObjectArray = await _redisClient.EvalAsync<string, string, object[]>(luaBuilder.ToString(), keys.ToArray(), arguments.ToArray()).ConfigureAwait(false);
            long connectionId = (long)connectionsAsObjectArray[0];
            long? oldConnectionId = null;
            if (connectionsAsObjectArray.Length > 1)
            {
                oldConnectionId = (long)connectionsAsObjectArray[1];
            }

            // if an old connection existed, close it out now (by pushing an empty value into its waitkey and, for good measure, expiring the waitkey)
            if (oldConnectionId != null)
            {
                string oldConnectionWaitKey = subscriptionKeyBase + "#waitkey-" + oldConnectionId.ToString();
                // NOTE: we push a value into the key so that the previous connection will trigger, see that it's not the current connection, and terminate
                await _redisClient.ListPushRightAsync(oldConnectionWaitKey, new string[] { string.Empty });
                // NOTE: we also put a timeout on the previous connection's waitkey so that, if no connection was actively listening, the key will be auto-deleted
                // NOTE: we use a fixed expiration of ten seconds.  We actually do not need any timeout--but we want to make sure that a caller who is currently blocked gets a chance to
                //       fetch the empty value we just enqueued...while automatically cleaning up afterwards.  ten seconds should be far more than enough time for a previous caller to 
                //       start blocking on its waitkey and dequeue the empty value we pushed into its queue.
                await _redisClient.ExpireAsync(oldConnectionWaitKey, 10);
            }

            // purge all notifications older than 'from-1'
            // NOTE: we do _not_ remove the "from-1" value; this helps us detect error cases in which we have purged notifications and the caller requests
            //       already-purged notifications.  Basically, we always keep one older notification around so that, if it is the only item in the notifications
            //       queue, we know that no notifications have been lost.  If notifications are lost (for a polled/websocket subscription), we auto-delete
            //       the subscription and close down its waiting callers so that the caller can re-sync and ensure data consistency.
            await _redisClient.SortedSetRemoveRangyByScoreAsync(subscriptionKeyBase + "#notifications", Double.NegativeInfinity, idRangeFrom.Value - 1, true, false);

            // NOTE: if our idRangeFrom value is not one (i.e. we are not looking for the first event), make sure that notification "idRangeFrom - 1" exists; 
            //       if it does not, then assume that we are out of sync and return a "no content" error.  This will let the client know something is wrong.
            // NOTE: there is one exception to this; if the "idRangeFrom" entry exists, then we're also fine.
            // NOTE: when clients request a higher-number "from" id, we purge older entries.  So clients must be careful to only request later IDs when they
            //       they have already retrieved earlier IDs.  This beahvior should happen normally and by SSE, etc. default--but is important to remember.
            if (idRangeFrom > 1)
            {
                var checkIdRangeResults = await _redisClient.SortedSetRangyByScoreAsync<string, string>(subscriptionKeyBase + "#notifications", idRangeFrom.Value - 1, idRangeFrom.Value, true, true);
                if (checkIdRangeResults == null || checkIdRangeResults.Count == 0)
                {
                    switch (getNotificationType)
                    {
                        case GetNotificationType.LongPolling:
                        case GetNotificationType.ServerSentEvent:
                            HttpHelper.SetHttpResponseNoContent(context);
                            return;
                    }
                }
            }

            // if the request is an SSE request, send an HTTP OK response and the required headers; we'll then send notifications as they become available
            if (getNotificationType == GetNotificationType.ServerSentEvent)
            {
                HttpHelper.SetHttpResponseOk(context);
                context.Response.Headers["Content-Type"] = "text/event-stream";
                context.Response.Headers["Cache-Control"] = "no-cache";
            }

            RedisClient blockingRedisClient = null;
            try
            {
                /* NOTES:
                 * 1. If this is a long-polling connection, we will exit the following loop after just one request.
                 * 2. For SSE and similar connections, we will continue getting more notifications until the connection is closed. */
                while (true)
                {
                    // create a list to store our accumulated notifications (which we can then return to the caller)
                    List<NotificationResult> notifications = new List<NotificationResult>();

                    // count down the number of notifications we still need to get; if this is not an AJAX call this number will be refreshed every loop iteration
                    long remainingNotificationsToGet = numNotificationsToGet ?? MAX_GET_NOTIFICATIONS_COUNT;

                    // retrieve up to 'remainingNumNotificationsToGet' notifications from the pre-existing notifications set (i.e. previously-fetched-and-queued notifications)
                    var retrievedNotifications = await RetrieveNotificationsFromSubscription(subscriptionKeyBase, idRangeFrom.Value, remainingNotificationsToGet);
                    remainingNotificationsToGet -= retrievedNotifications.Count();
                    // add the retrieved notifications to our accumulated list variable
                    notifications.AddRange(retrievedNotifications);

                    // if the #notifications set did not contain enough notifications, try pulling more notifications in from the pending (incoming priority) notification sets
                    if (remainingNotificationsToGet > 0)
                    {
                        var moveNotificationResult = await MoveNotificationsFromSubscriptionIncomingQueues(subscriptionKeyBase, connectionId, remainingNotificationsToGet);
                        if (moveNotificationResult == null)
                        {
                            // we are no longer the active connection for this subscription; abort immediately
                            if (getNotificationType != GetNotificationType.ServerSentEvent)
                            {
                                HttpHelper.SetHttpResponseNoContent(context);
                            }
                            return;
                        }
                        else if (moveNotificationResult.Value.Count > 0)
                        {
                            // again...retrieve up to 'remainingNumNotificationsToGet' notifications from the pre-existing notifications set
                            retrievedNotifications = await RetrieveNotificationsFromSubscription(subscriptionKeyBase, moveNotificationResult.Value.FirstNotificationId, moveNotificationResult.Value.Count);
                            remainingNotificationsToGet -= retrievedNotifications.Count();
                            // add the retrieved notifications to our accumulated list
                            notifications.AddRange(retrievedNotifications);
                        }
                    }

                    // if we have retrieved any notifications, update our idRangeFrom value
                    if (notifications.Count > 0)
                    {
                        idRangeFrom = notifications[notifications.Count - 1].id + 1;
                    }

                    switch (getNotificationType)
                    {
                        case GetNotificationType.LongPolling:
                            // if we successfully retrieved any notifications, return now. 
                            if (notifications.Count > 0)
                            {
                                // set our headers
                                HttpHelper.SetHttpResponseOk(context);
                                context.Response.Headers["Content-Type"] = "application/json";
                                context.Response.Headers["Cache-Control"] = "no-cache";
                                // write out our events
                                StringBuilder longPollingOutput = new StringBuilder();
                                longPollingOutput.Append("{items:[");
                                foreach (NotificationResult notification in notifications)
                                {
                                    // add the eventId to the notification
                                    string notificationJson = InsertEventIdIntoNotificationJson(notification.data, notification.id.ToString());
                                    longPollingOutput.Append(notificationJson + ",");
                                }
                                longPollingOutput.Remove(longPollingOutput.Length - 1, 1); // remove the trailing comma
                                longPollingOutput.Append("]}");
                                await context.Response.WriteAsync(longPollingOutput.ToString());
                                return;
                            }
                            break;
                        case GetNotificationType.ServerSentEvent:
                            {
                                // if we have retrieved at least one notification, transmit it now; otherwise, block on a dedicated redis connection until 
                                // our subscription receives a new notification
                                if (notifications.Count > 0)
                                {
                                    StringBuilder sseEventOutput = new StringBuilder();
                                    foreach (NotificationResult notification in notifications)
                                    {
                                        string eventData;

                                        sseEventOutput.Append("id:" + notification.id.ToString() + "\n");
                                        // remove the event name from the notification
                                        var removeEventResult = RemoveEventFromNotificationJson(notification.data);

                                        // if an event name exists in the notification, remove it from the notification JSON and write it out before the event
                                        if (removeEventResult != null)
                                        {
                                            StringBuilder eventDataBuilder = new StringBuilder();
                                            eventDataBuilder.Append(notification.data.Substring(0, removeEventResult.Value.FirstCharToRemoveIndex));
                                            // NOTE: we are simply omitting the "eventname" key/value pair here
                                            eventDataBuilder.Append(notification.data.Substring(removeEventResult.Value.FirstCharToRemoveIndex + removeEventResult.Value.RemoveCharCount));
                                            eventData = eventDataBuilder.ToString();
                                            sseEventOutput.Append("event:" + removeEventResult.Value.Event + "\n");
                                        }
                                        else
                                        {
                                            eventData = notification.data;
                                        }

                                        // NOTE: some notification.data payloads may end with a blank line; in those circumstances, make sure we include an extra blank line at the end of our SSE transmission
                                        //       if previousLineEndedWithLineFeed is set to true, then a blank "final line" will be transmitted (to preserve the line feed on the previous line)
                                        bool previousLineEndedWithLineFeed = false;
                                        while ((eventData.Length > 0) || (previousLineEndedWithLineFeed == true))
                                        {
                                            // NOTE: we will treat all three possible line feed sequences identically.
                                            string eventDataLine;
                                            if (eventData.IndexOf("\r\n") >= 0)
                                            {
                                                eventDataLine = eventData.Substring(0, eventData.IndexOf("\r\n"));
                                                eventData = eventData.Substring(eventData.IndexOf("\r\n") + 2);
                                                previousLineEndedWithLineFeed = true;
                                            }
                                            else if (eventData.IndexOf("\r") >= 0)
                                            {
                                                eventDataLine = eventData.Substring(0, eventData.IndexOf("\r"));
                                                eventData = eventData.Substring(eventData.IndexOf("\r") + 1);
                                                previousLineEndedWithLineFeed = true;
                                            }
                                            else if (eventData.IndexOf("\n") >= 0)
                                            {
                                                eventDataLine = eventData.Substring(0, eventData.IndexOf("\n"));
                                                eventData = eventData.Substring(eventData.IndexOf("\n") + 1);
                                                previousLineEndedWithLineFeed = true;
                                            }
                                            else
                                            {
                                                eventDataLine = eventData;
                                                eventData = string.Empty;
                                                previousLineEndedWithLineFeed = false;
                                            }
                                            sseEventOutput.Append("data:" + eventDataLine + "\n");
                                        }
                                        sseEventOutput.Append("\n"); // send a blank line to fire the just-transmitted event on the client
                                        await context.Response.WriteAsync(sseEventOutput.ToString());
                                    }
                                }
                                else
                                {
                                    // we have no events to transmit
                                }
                                //await context.Response.Body.FlushAsync();
                            }
                            break;
                    }

                    // determine our timeout, based on the caller's mechanism (SSE = 15 sec, FF = 15 sec, AJAX = 25 sec, Websocket = ???)
                    long timeoutInSeconds;
                    switch (getNotificationType)
                    {
                        case GetNotificationType.ServerSentEvent:
                            timeoutInSeconds = 15;
                            break;
                        case GetNotificationType.LongPolling:
                        default:
                            timeoutInSeconds = 25;
                            break;
                    }

                    // create a *new* connection to Redis and block on our waitkey; if we cannot make a connection (overload?), log the error.  And return HTTP 500
                    // if this is an AJAX call...or simply close the connection for any other type of call.
                    if (blockingRedisClient == null)
                    {
                        try
                        {
                            blockingRedisClient = await Singletons.CreateNewRedisClientAsync();
                        }
                        catch
                        {
                            if (getNotificationType == GetNotificationType.LongPolling)
                            {
                                HttpHelper.SetHttpResponseInternalServerError(context);
                            }
                            return;
                        }
                    }

                    string waitKey = subscriptionKeyBase + "#waitkey-" + connectionId.ToString();
                    KeyValuePair<string, string> result = await blockingRedisClient.ListPopLeftBlockingAsync<string, string>(new string[] { waitKey }, timeoutInSeconds);

                    if (result.Key == null)
                    {
                        // timeout
                        switch (getNotificationType)
                        {
                            /* NOTE: SSE: we will send a "comment" line every time the waitqueue times out, if we have no data.
                             *       AJAX: we will return the call every time the waitqueue times out, whether or not we have data
                             *       Websocket: just go back to waiting again; the timeout mechanism is just here to "clean us up".
                             *       ALL SCENARIOS: delete our waitkey before we exit */
                            case GetNotificationType.LongPolling:
                                {
                                    // end the connection, letting the caller know that were no events during the timeout period
                                    HttpHelper.SetHttpResponseNoContent(context);
                                    return;
                                }
                            case GetNotificationType.ServerSentEvent:
                                {
                                    // send a keepalive "comment"
                                    await context.Response.WriteAsync(":\n");
                                }
                                break;
                        }
                    }
                    else
                    {
                        // wait key was triggered...either because a new connection has taken over or because we have a new notification to dequeue
                        // in either circumstance, we attempt to retrieve notifications; if we are no longer the active connection then we abort.
                        var moveNotificationResult = await MoveNotificationsFromSubscriptionIncomingQueues(subscriptionKeyBase, connectionId, remainingNotificationsToGet);
                        if (moveNotificationResult == null)
                        {
                            // we are no longer the current connection; abort.
                            return;
                        }
                        else if (moveNotificationResult.Value.Count > 0)
                        {
                            // we are still the current connection and we have retrieved more notifications
                            // optional: idRangeFrom = moveNotificationResult.Value.FirstNotificationId;
                        }
                    }
                }
            }
            finally
            {
                if (blockingRedisClient != null)
                {
                    blockingRedisClient.Dispose();
                }
            }
        }

        enum GetNotificationType
        {
            LongPolling = 0,
            ServerSentEvent = 1,
        }

        // this function is used to insert the event "id" field into the notification json
        string InsertEventIdIntoNotificationJson(string json, string eventId)
        {
            if (json == null) return null;
            if (json.IndexOf("{") != 0) return null;

            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.Append(json.Substring(0, 1));
            resultBuilder.Append("id:" + eventId + ",");
            resultBuilder.Append(json.Substring(1));
            return resultBuilder.ToString();
        }

        struct RemoveEventFromNotificationJsonResult
        {
            public string Event;
            public int FirstCharToRemoveIndex;
            public int RemoveCharCount;
        }
        // this function is used to remove the "event" field from the notification json; the returned value consists of the event and the characters to snip from the JSON
        // NOTE: this function returns null if the json is invalid or no event is found
        // NOTE: this function assumes that no other keys will end in "event" before the "event" key that we're looking for.
        RemoveEventFromNotificationJsonResult? RemoveEventFromNotificationJson(string json)
        {
            if (json == null) return null;
            if (json.IndexOf("{") != 0) return null;

            string eventKey = "event:";

            int beginEventPos = json.ToLowerInvariant().IndexOf(eventKey);
            if (beginEventPos < 0) return null;

            // find the entire event
            char? quoteChar = null;
            int beginQuotePos = -1;
            int endQuotePos = -1;
            for (int i = beginEventPos + eventKey.Length; i < json.Length; i++)
            {
                if (quoteChar == null)
                {
                    if (json[i] == '\"' || json[i] == '\'')
                    {
                        beginQuotePos = i;
                        quoteChar = json[i];
                    }
                }
                else
                {
                    if (json[i] == quoteChar)
                    {
                        endQuotePos = i;
                        break;
                    }
                }
            }

            // if we could not find the end of the event, return null.
            if (endQuotePos == -1) return null;

            // now, consume the comma (unless we reach the end of the json first)
            int endEventPos = endQuotePos;
            for (int i = endQuotePos + 1; i < json.Length; i++)
            {
                if (json[i] == ',')
                {
                    endEventPos = i;
                    break;
                }
            }

            // finally, parse out the event name and return the character sequence to remove
            RemoveEventFromNotificationJsonResult result = new RemoveEventFromNotificationJsonResult()
            {
                Event = json.Substring(beginQuotePos + 1, endQuotePos - beginQuotePos - 1),
                FirstCharToRemoveIndex = beginEventPos,
                RemoveCharCount = endEventPos - beginEventPos + 1
            };
            return result;
        }

        async Task<List<NotificationResult>> RetrieveNotificationsFromSubscription(string subscriptionKey, long from, long count)
        {
            List<NotificationResult> notifications = new List<NotificationResult>();

            // retrieve up to "count" notifications (with ids already assigned) now; if we don't have the full "count" then we will look for notifications
            // in the incoming notification priority sets next.
            List<byte[]> notificationsWithScores = await _redisClient.SortedSetRangyByScoreAsync<string, byte[]>(subscriptionKey + "#notifications", (double)from, double.PositiveInfinity, true, true, true, 0, (double)count);

            // the resulting list consists of the event ID and the event data; the event is prepended by an 8-byte unique id (receive timestamp) which we will remove before returning the results
            int i = 0;
            while (i < notificationsWithScores.Count - 1)
            {
                NotificationResult notification = new NotificationResult();
                // retrieve notification data (skipping the first 8 bytes)
                notification.data = new string(Encoding.UTF8.GetChars(notificationsWithScores[i], 8, notificationsWithScores[i].Length - 8));
                // retrieve notification id
                bool retrieveIdSuccess = long.TryParse(new string(System.Text.Encoding.UTF8.GetChars(notificationsWithScores[i + 1])), out notification.id);
                if (retrieveIdSuccess == false)
                {
                    // TODO: throw more specific "parsing event" error
                    throw new Exception(); 
                }

                notifications.Add(notification);
                count--;
                i += 2;
            }

            return notifications;
        }

        struct MoveNotificationsFromSubscriptionIncomingQueuesResult
        {
            public long FirstNotificationId;
            public long Count;
        }
        // subscriptionKey = the base subscription key (e.g. subscription:accountname/123)
        // connectionId = the connection ID of the current connection
        // count = the maximum number of notifications to move from the incoming priority queues
        async Task<MoveNotificationsFromSubscriptionIncomingQueuesResult?> MoveNotificationsFromSubscriptionIncomingQueues(string subscriptionKey, long connectionId, long count)
        {
            MoveNotificationsFromSubscriptionIncomingQueuesResult? result = null;

            // generate Lua script (which will fetch incoming notifications from the incomingpriority queues)
            // NOTE: this set of operations must complete atomically so that we are guaranteed to be the connection owner; if we did not do these
            //       operations atomically, a new connection could take ownership of the subscription plus notification corruption (duplicates, etc.)
            var luaBuilder = new StringBuilder();
            var arguments = new List<string>();
            int iArgument = 1;
            // CRITICAL: doubly-verify that our connectionId has not changed between the time we set it and now
            luaBuilder.Append("local verifyConnectionId = redis.call(\"HGET\", KEYS[1], \"connection-id\")\n");
            luaBuilder.Append("if verifyConnectionId ~= ARGV[" + iArgument.ToString() + "] then\n");
            luaBuilder.Append("  return 0\n");
            luaBuilder.Append("end\n");
            arguments.Add(connectionId.ToString());
            iArgument++;
            // if the connectionId has not changed, iterate through all seven priority queues in order, removing up to "remainingNotificationsToGet"
            // notifications in total; we then add those events into the subscription's notifications set
            luaBuilder.Append("local numNotifications = 0\n");
            luaBuilder.Append("local numRemainingNotifications = tonumber(ARGV[" + iArgument.ToString() + "])\n");
            luaBuilder.Append("local firstNotificationId = 0\n");
            luaBuilder.Append("local notificationId\n");
            arguments.Add(count.ToString());
            iArgument++;
            // iterate through each priority queue (starting at the highest priority)
            luaBuilder.Append("for iPriority=7,0,-1 do\n");
            // if we have already fetched our full list of notifications, break.
            luaBuilder.Append("  if numRemainingNotifications < 0 then break end\n");
            // pull in a list of notifications from this incomingpriority queue
            luaBuilder.Append("  local notifications = redis.call(\"ZRANGE\", KEYS[3 + iPriority], 0, numRemainingNotifications-1)\n");
            luaBuilder.Append("  for iNotification = 1, #notifications do\n"); // NOTE: #notifications means "length of array"
                                                                               // for each "dequeued" notification: create the next notificationId, and add the notification (with id) to the subscription's #notifications set
            luaBuilder.Append("    notificationId = redis.call(\"HINCRBY\", KEYS[1], \"last-notification-id\", 1)\n");
            luaBuilder.Append("    if firstNotificationId == 0 then\n");
            luaBuilder.Append("      firstNotificationId = notificationId\n");
            luaBuilder.Append("    end\n");
            luaBuilder.Append("    redis.call(\"ZADD\", KEYS[2], notificationId, notifications[iNotification])\n");
            luaBuilder.Append("  end\n");
            // and delete the newly-dequeued notifications from the current "incomingpriority" set
            luaBuilder.Append("  numNotifications = numNotifications + #notifications\n");
            luaBuilder.Append("  numRemainingNotifications = numRemainingNotifications - #notifications\n");
            luaBuilder.Append("    if #notifications > 0 then\n");
            luaBuilder.Append("      redis.call(\"ZREMRANGEBYRANK\", KEYS[3 + iPriority], 0, #notifications)\n");
            luaBuilder.Append("    end\n");
            luaBuilder.Append("end\n");
            //
            // return the first notification id (of the notifications we just added) and the total count (or 0 if no notifications were found)
            luaBuilder.Append("return {firstNotificationId, numNotifications}\n");

            var keys = new List<string>();
            keys.Add(subscriptionKey);
            keys.Add(subscriptionKey + "#notifications");
            for (int iPriority = 0; iPriority <= 7; iPriority++)
            {
                keys.Add(subscriptionKey + "#incoming-notifications" + iPriority.ToString());
            }
            // this function will return 0 if we have lost our connection...or an array of two values if success ({firstNotificationId, count})
            object priorityFetchResult = await _redisClient.EvalAsync<string, string, object>(luaBuilder.ToString(), keys.ToArray(), arguments.ToArray()).ConfigureAwait(false);
            if (priorityFetchResult != null && priorityFetchResult.GetType() == typeof(long))
            {
                if ((long)priorityFetchResult == 0)
                {
                    // we are no longer the active subscription
                    return null;
                }
                else
                {
                    // unknown response; consider throwing an error
                }
            }
            else if (priorityFetchResult != null && priorityFetchResult.GetType() == typeof(object[]))
            {
                object[] priorityFetchResultArray = (object[])priorityFetchResult;
                result = new MoveNotificationsFromSubscriptionIncomingQueuesResult()
                {
                    FirstNotificationId = (long)priorityFetchResultArray[0],
                    Count = (long)priorityFetchResultArray[1]
                };
            }

            return result;
        }

        private long? RetrieveHeaderValueAsLong(HttpContext context, string name)
        {
            long result;
            if (long.TryParse(context.Request.Headers[name], out result))
            {
                // if the header existed and could be parsed as an int, return its value now.
                return result;
            }
            else
            {
                // if the header existed but could not be parsed (or was null--not-existent--and could therefore not be parsed), return null.
                return null;
            }
        }

        private long? RetrieveQueryParameterAsLong(HttpContext context, string name)
        {
            long result;
            if (long.TryParse(context.Request.Query[name], out result))
            {
                // if the param existed and could be parsed as an int, return its value now.
                return result;
            }
            else
            {
                // if the query existed but could not be parsed (or was null--not-existent--and could therefore not be parsed), return null.
                return null;
            }
        }
    }

    public static class BuilderExtensions
    {
        public static IApplicationBuilder UseEventService(this IApplicationBuilder app)
        {
            return app.UseMiddleware<EventServiceMiddleware>();
        }
    }
}
