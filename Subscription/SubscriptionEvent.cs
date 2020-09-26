using Strombus.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Strombus.EventService
{
    public class SubscriptionEvent
    {
        static RedisClient _redisClient = null;

        private const string REDIS_PREFIX_EVENT = "event";
        private const string REDIS_PREFIX_SUBSCRIPTION = "subscription";
        private const string REDIS_PREFIX_SEPARATOR = ":";
        //
        private const string REDIS_ASTERISK = "*";
        private const string REDIS_SLASH = "/";
        private const char REDIS_SLASH_AS_CHAR = '/';
        //
        private const string REDIS_SUFFIX_EVENTS = "events";
        private const string REDIS_SUFFIX_SUBSCRIPTIONS = "subscriptions";
        private const string REDIS_SUFFIX_SEPARATOR = "#";

        public string name; // pre-defined name of event
        public string scope; // path which limits the scope of the event subscription (i.e. only raise this event if it happens within this scope)
        public string href; // uri of SubscriptionEvent

        public static async Task<bool> AddEventToSubscriptionAsync(string accountId, string subscriptionId, string eventName, string eventScope)
        {
            if (_redisClient == null)
            {
                _redisClient = await Singletons.GetRedisClientAsync();
            }

            // convert the event name and event scope to all lower-case characters
            if (eventName != null) eventName = eventName.ToLowerInvariant();
            if (eventScope != null) eventScope = eventScope.ToLowerInvariant();

            // sanity-check: eventScope must belong to the account; if the scope is null or empty then set it to scope to the account itself
            if (string.IsNullOrWhiteSpace(accountId) == false)
            {
                // accountId is present
                string baseAccountScope = "/accounts/" + accountId;

                if (eventScope != null)
                {
                    if (eventScope.IndexOf(baseAccountScope) != 0)
                    {
                        return false;
                    }
                }
                else
                {
                    eventScope = baseAccountScope;
                }
            }

            // NOTE: we do this entire operation in one atomic Lua script to ensure that we all the collections we touch stay intact
            // NOTE: if the subscription is already subscribed to this event...then this Lua function will effectively have no effect

            // create our script-builder object placeholders
            StringBuilder luaBuilder;
            List<string> arguments;
            int iArgument;
            List<string> keys;

            int RESULT_SUCCESS = 0;
            int RESULT_SUBSCRIPTION_MISSING = -4;

            // generate Lua script
            luaBuilder = new StringBuilder();
            arguments = new List<string>();
            iArgument = 1;
            // if the subscription has already been deleted, return false
            luaBuilder.Append(
                "if redis.call(\"EXISTS\", KEYS[1]) == 0 then\n" +
                "  return " + RESULT_SUBSCRIPTION_MISSING.ToString() + "\n"+
                "end\n");
            //
            luaBuilder.Append("redis.call(\"SADD\", KEYS[2], ARGV[" + iArgument.ToString() + "])\n");
            arguments.Add(accountId + REDIS_SLASH + subscriptionId);
            iArgument++;
            //
            luaBuilder.Append("redis.call(\"SADD\", KEYS[3], ARGV[" + iArgument.ToString() + "])\n");
            arguments.Add(eventName + "[scope@" + eventScope + "]");
            iArgument++;
            //
            luaBuilder.Append("return " + RESULT_SUCCESS.ToString() + "\n");

            keys = new List<string>();
            keys.Add(REDIS_PREFIX_SUBSCRIPTION + REDIS_PREFIX_SEPARATOR + accountId + REDIS_SLASH + subscriptionId);
            keys.Add(REDIS_PREFIX_EVENT + REDIS_PREFIX_SEPARATOR + eventName + "[scope@" + eventScope + "]" + REDIS_SUFFIX_SEPARATOR + REDIS_SUFFIX_SUBSCRIPTIONS);
            keys.Add(REDIS_PREFIX_SUBSCRIPTION + REDIS_PREFIX_SEPARATOR + accountId + REDIS_SLASH + subscriptionId + REDIS_SUFFIX_SEPARATOR + REDIS_SUFFIX_EVENTS);
            long luaResult = await _redisClient.EvalAsync<string, string, long>(luaBuilder.ToString(), keys.ToArray(), arguments.ToArray()).ConfigureAwait(false);

            if (luaResult == RESULT_SUCCESS)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public static async Task RemoveEventFromSubscriptionAsync(string accountId, string subscriptionId, string eventName, string eventScope)
        {
            if (_redisClient == null)
            {
                _redisClient = await Singletons.GetRedisClientAsync();
            }

            // convert the event scope to all lower-case characters
            eventScope = eventScope.ToLowerInvariant();

            // sanity-check: eventScope must belong to the account; if the scope is null or empty then set it to scope to the account itself
            if (string.IsNullOrWhiteSpace(accountId) == false)
            {
                // accountId is present
                string baseAccountScope = "/accounts/" + accountId;

                if (eventScope != null)
                {
                    if (eventScope.IndexOf(baseAccountScope) != 0)
                    {
                        return; // return false;
                    }
                }
                else
                {
                    eventScope = baseAccountScope;
                }
            }

            // NOTE: we do this entire operation in one atomic Lua script to ensure that we all the collections we touch stay intact
            // NOTE: if the subscription is already subscribed to this event...then this Lua function will effectively have no effect

            // create our script-builder object placeholders
            StringBuilder luaBuilder;
            List<string> arguments;
            int iArgument;
            List<string> keys;

            int RESULT_SUCCESS = 0;

            // generate Lua script
            luaBuilder = new StringBuilder();
            arguments = new List<string>();
            iArgument = 1;
            //
            luaBuilder.Append("redis.call(\"SREM\", KEYS[1], ARGV[" + iArgument.ToString() + "])\n");
            arguments.Add(accountId + REDIS_SLASH + subscriptionId);
            iArgument++;
            //
            luaBuilder.Append("redis.call(\"SREM\", KEYS[2], ARGV[" + iArgument.ToString() + "])\n");
            arguments.Add(eventName + "[scope@" + eventScope + "]");
            iArgument++;
            //
            luaBuilder.Append("return " + RESULT_SUCCESS.ToString() + "\n");

            keys = new List<string>();
            keys.Add(REDIS_PREFIX_EVENT + REDIS_PREFIX_SEPARATOR + eventName + "[scope@" + eventScope + "]" + REDIS_SUFFIX_SEPARATOR + REDIS_SUFFIX_SUBSCRIPTIONS);
            keys.Add(REDIS_PREFIX_SUBSCRIPTION + REDIS_PREFIX_SEPARATOR + accountId + REDIS_SLASH + subscriptionId + REDIS_SUFFIX_SEPARATOR + REDIS_SUFFIX_EVENTS);
            long luaResult = await _redisClient.EvalAsync<string, string, long>(luaBuilder.ToString(), keys.ToArray(), arguments.ToArray()).ConfigureAwait(false);
        }
    }
}
