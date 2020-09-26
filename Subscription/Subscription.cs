using Strombus.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json.Serialization;
using Newtonsoft.Json;
using System.Text;
using Strombus.ServerShared;

namespace Strombus.EventService
{
    public class Subscription
    {
        static RedisClient _redisClient = null;

        private const string REDIS_PREFIX_EVENT_SERVICE = "event-service";
        private const string REDIS_PREFIX_SUBSCRIPTION = "subscription";
        private const string REDIS_PREFIX_SEPARATOR = ":";
        //
        private const string REDIS_ASTERISK = "*";
        private const string REDIS_SLASH = "/";
        private const char REDIS_SLASH_AS_CHAR = '/';
        //
        private const string REDIS_SUFFIX_EVENTS = "events";
        private const string REDIS_SUFFIX_INCOMING_NOTIFICATIONS = "incoming_notifications";
        private const string REDIS_SUFFIX_NOTIFICATIONS = "notifications";
        private const string REDIS_SUFFIX_SUBSCRIPTIONS = "subscriptions";
        private const string REDIS_SUFFIX_SEPARATOR = "#";

        private const string EVENT_SERVICE_NAME = "event";

        bool _objectIsNew = true;

        private string _serverId = null;
        private string _clientId = null;
        private string _accountId = null;
        private string _userId = null; // optional
        private string _subscriptionId = null;

        private Subscription()
        {
        }

        public static Subscription NewSubscription(string serverId, string clientId, string accountId, string userId)
        {
            Subscription result = new Subscription()
            {
                _objectIsNew = true,
                _serverId = serverId,
                _clientId = clientId,
                _accountId = accountId,
                _userId = userId
            };
            return result;
        }

        public static async Task<Subscription> LoadSubscriptionAsync(string accountId, string subscriptionId)
        {
            if (_redisClient == null)
            {
                _redisClient = await Singletons.GetRedisClientAsync();
            }

            string fullyQualifiedSubscriptionKey = REDIS_PREFIX_SUBSCRIPTION + REDIS_PREFIX_SEPARATOR + (accountId != null ? accountId.ToLowerInvariant() : REDIS_ASTERISK) + REDIS_SLASH + subscriptionId.ToLowerInvariant();
            bool subscriptionExists = (await _redisClient.ExistsAsync(new string[] { fullyQualifiedSubscriptionKey }) > 0);
            if (subscriptionExists)
            { 
                Dictionary<string, string> subscriptionDictionary = await _redisClient.HashGetAllASync<string, string, string>(fullyQualifiedSubscriptionKey);

                // extract serverId from subscriptionId
                string serverId = ParsingHelper.ExtractServerDetailsFromAccountServerIdIdentifier(subscriptionId).Value.ToServerTypeServerIdIdentifierString();
                //
                string clientId = subscriptionDictionary.ContainsKey("client-id") ? subscriptionDictionary["client-id"] : null;
                //
                string userId = subscriptionDictionary.ContainsKey("user-id") ? subscriptionDictionary["user-id"] : null;

                Subscription resultSubscription = new Subscription();
                // account-id
                resultSubscription._accountId = accountId;
                // subscription-id
                resultSubscription._subscriptionId = subscriptionId;
                // server-id
                resultSubscription._serverId = serverId;
                // client-id
                resultSubscription._clientId = clientId;
                // user-id
                resultSubscription._userId = userId;

                return resultSubscription;
            }

            // subscription (or its owner) could not be found
            return null;
        }

        public async Task DeleteSubscriptionAsync()
        {
            if (_redisClient == null)
            {
                _redisClient = await Singletons.GetRedisClientAsync();
            }

            //int RESULT_KEY_CONFLICT = -1;

            // generate Lua script (which we will use to commit all changes--or the new record--in an atomic transaction)
            StringBuilder luaBuilder = new StringBuilder();
            List<string> arguments = new List<string>();
            int iArgument = 1;
            // if the subscription has already been deleted, return success
            luaBuilder.Append(
                "if redis.call(\"EXISTS\", KEYS[1]) == 0 then\n" +
                "  return {1, \"\"}\n" +
                "end\n");
            // read the connection-id from the subscription; we will delete this connection's waitkey in a follow-up call
            luaBuilder.Append("local connection_id = redis.call(\"HGET\", KEYS[1], \"connection-id\")\n");
            // delete the subscription itself
            luaBuilder.Append("redis.call(\"DEL\", KEYS[1])\n");
            // delete the subscription's event registrations
            luaBuilder.Append("redis.call(\"DEL\", KEYS[2])\n");
            // delete the subscription's notifications list
            luaBuilder.Append("redis.call(\"DEL\", KEYS[3])\n");
            // delete the subscription's incoming notifications priority queues
            for (int iPriority = 0; iPriority < 8; iPriority++)
            {
                luaBuilder.Append("redis.call(\"DEL\", KEYS[" + (4 + iPriority).ToString() + "])\n");
            }
            //
            luaBuilder.Append("return {1, connection-id}\n");

            object[] luaResult = null;
            List<string> keys = new List<string>();
            string subscriptionKeyBase = REDIS_PREFIX_SUBSCRIPTION + REDIS_PREFIX_SEPARATOR + (_accountId != null ? _accountId : REDIS_ASTERISK) + REDIS_SLASH + _subscriptionId; ;
            keys.Add(subscriptionKeyBase);
            keys.Add(subscriptionKeyBase + REDIS_SUFFIX_SEPARATOR + REDIS_SUFFIX_EVENTS);
            keys.Add(subscriptionKeyBase + REDIS_SUFFIX_SEPARATOR + REDIS_SUFFIX_NOTIFICATIONS);
            for (int iPriority = 0; iPriority < 8; iPriority++)
            {
                keys.Add(subscriptionKeyBase + REDIS_SUFFIX_SEPARATOR + REDIS_SUFFIX_INCOMING_NOTIFICATIONS + iPriority.ToString());
            }
            luaResult = await _redisClient.EvalAsync<string, string, object[]>(luaBuilder.ToString(), keys.ToArray(), arguments.ToArray()).ConfigureAwait(false);

            long? resultValue = (luaResult != null && luaResult.Length >= 1 ? (long)luaResult[0] : (long?)null);
            if (resultValue != null && resultValue == 1)
            {
                if (luaResult.Length >= 2 && string.IsNullOrWhiteSpace((string)luaResult[1]) == false)
                {
                    string oldConnectionId = (string)luaResult[1];

                    // success; dispose of the current connection's waitkey, if one existed (by pushing a dummy value into it and then expiring it in ten seconds)
                    string oldConnectionWaitKey = subscriptionKeyBase + "#waitkey-" + oldConnectionId.ToString();
                    // NOTE: we push a value into the key so that the previous connection will trigger, see that it's not the current connection, and terminate
                    await _redisClient.ListPushRightAsync(oldConnectionWaitKey, new string[] { string.Empty });
                    // NOTE: we also put a timeout on the previous connection's waitkey so that, if no connection was actively listening, the key will be auto-deleted
                    // NOTE: we use a fixed expiration of ten seconds.  We actually do not need any timeout--but we want to make sure that a caller who is currently blocked gets a chance to
                    //       fetch the empty value we just enqueued...while automatically cleaning up afterwards.  ten seconds should be far more than enough time for a previous caller to 
                    //       start blocking on its waitkey and dequeue the empty value we pushed into its queue.
                    await _redisClient.ExpireAsync(oldConnectionWaitKey, 10);
                }

                // finally, reset our server-assigned values
                _subscriptionId = null;
            }
            else
            {
                // unknown error
                throw new Exception("Critical Redis error!");
            }
        }

        public async Task SaveSubscriptionAsync()
        {
            if (_redisClient == null)
            {
                _redisClient = await Singletons.GetRedisClientAsync();
            }

            // create our script-builder object placeholders; we re-use these below
            StringBuilder luaBuilder;
            List<string> arguments;
            int iArgument;
            List<string> keys;

            /* if the subscription is a new subscription, allocate the subscription-id now */
            if (_objectIsNew)
            {
                // allocate a new subscriptionId for this subscription on this account; we use an atomic lua script to do this so that the first subscriptionId is zero for consistency (since HINCRBY would set the first entry to one)
                // NOTE: we allocate the subscriptionId in a unique script because Lua-on-Redis requires that we pass all keys pre-built--so we can't allocate the subscriptionId and create its key(s) in the same script
                // generate Lua script
                luaBuilder = new StringBuilder();
                arguments = new List<string>();
                iArgument = 1;
                //
                // default our subscription_id to -1 (as a marker indicating that the subscription_id is not valid)
                luaBuilder.Append("local subscription_id = -1\n");
                // create or increment the last-subscription-id for this account
                luaBuilder.Append(
                    "  if redis.call(\"HEXISTS\", KEYS[1], \"last-subscription-id\") == 0 then\n" +
                    // if no subscription has been added to this account for this server_id, set the subscription_id to zero and store it.
                    "    redis.call(\"HSET\", KEYS[1], \"last-subscription-id\", 0)\n" +
                    "    subscription_id = 0\n" +
                    "  else\n" +
                    // otherwise...increment the last-subscription-id and return the new value as subscription_id.
                    "    subscription_id = redis.call(\"HINCRBY\", KEYS[1], \"last-subscription-id\", 1)\n" +
                    "  end\n" +
                    "  return subscription_id\n"
                    );
                //
                keys = new List<string>();
                keys.Add(REDIS_PREFIX_EVENT_SERVICE + REDIS_PREFIX_SEPARATOR + _accountId + REDIS_SLASH + _serverId);
                long newSubscriptionLuaResult = await _redisClient.EvalAsync<string, string, long>(luaBuilder.ToString(), keys.ToArray(), arguments.ToArray()).ConfigureAwait(false);
                //
                // obtain the subscription_id (if subscription_id == -1 then we had an error)
                if (newSubscriptionLuaResult >= 0)
                {
                    _subscriptionId = _serverId + "-" + newSubscriptionLuaResult.ToString();
                }
                else
                {
                    // subscriptionId could not be allocated
                    throw new Exception("Critical Redis error!");
                }
            }

            int RESULT_SUCCESS = 0;
            int RESULT_KEY_CONFLICT = -1;
            int RESULT_DATA_CORRUPTION = -2;

            // generate Lua script (which we will use to commit all changes--or the new record--in an atomic transaction)
            luaBuilder = new StringBuilder();
            arguments = new List<string>();
            iArgument = 1;
            if (_objectIsNew)
            {
                // for new subscriptions: if a subscription with this accountId+'/'+subscriptionId already exists, return RESULT_KEY_CONFLICT.
                luaBuilder.Append(
                    "if redis.call(\"EXISTS\", KEYS[1]) == 1 then\n" +
                    "  return " + RESULT_KEY_CONFLICT.ToString() + "\n" +
                    "end\n");
            }
            //
            luaBuilder.Append("redis.call(\"HSET\", KEYS[1], \"client-id\", ARGV[" + iArgument.ToString() + "])\n");
            arguments.Add(_clientId);
            iArgument++;
            //
            if (_userId != null)
            {
                luaBuilder.Append("redis.call(\"HSET\", KEYS[1], \"user-id\", ARGV[" + iArgument.ToString() + "])\n");
                arguments.Add(_userId);
                iArgument++;
            }
            //
            luaBuilder.Append("return " + RESULT_SUCCESS.ToString() + "\n"); // 

            keys = new List<string>();
            string subscriptionKeyBase = REDIS_PREFIX_SUBSCRIPTION + REDIS_PREFIX_SEPARATOR + (_accountId != null ? _accountId : REDIS_ASTERISK) + REDIS_SLASH + _subscriptionId;
            keys.Add(subscriptionKeyBase);
            long luaResult = await _redisClient.EvalAsync<string, string, long>(luaBuilder.ToString(), keys.ToArray(), arguments.ToArray()).ConfigureAwait(false);
            if (luaResult == RESULT_SUCCESS)
            {
                // success

                // add the subscription to the set of all subscriptions on this fullServerId
                string subscriptionsSetKey = REDIS_PREFIX_EVENT_SERVICE + REDIS_PREFIX_SEPARATOR + (_accountId != null ? _accountId : REDIS_ASTERISK) + REDIS_SLASH + _serverId + REDIS_SUFFIX_SEPARATOR + REDIS_SUFFIX_SUBSCRIPTIONS;
                await _redisClient.SetAddAsync(subscriptionsSetKey, new string[] { _subscriptionId });
            }
            else if (luaResult == RESULT_KEY_CONFLICT)
            {
                // key name conflict
                // TODO: consider returning an error to the caller
            }
            else if (luaResult == RESULT_DATA_CORRUPTION)
            {
                // data corruption
                throw new Exception("Critical Redis error!");
            }
            else
            {
                // unknown error
                throw new Exception("Critical Redis error!");
            }
        }

        [JsonIgnore]
        public string AccountId
        {
            get
            {
                return _accountId;
            }
        }

        [JsonIgnore]
        public string ClientId
        {
            get
            {
                return _clientId;
            }
        }

        [JsonIgnore]
        public string UserId
        {
            get
            {
                return _userId;
            }
        }

        [JsonProperty("id")]
        public string SubscriptionId
        {
            get
            {
                return _subscriptionId;
            }
        }

    }
}
