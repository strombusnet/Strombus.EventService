using Strombus.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Strombus.EventService
{
    public class EventDispatcher
    {
        static RedisClient _redisClient;

        public static async Task DispatchEventsAsync()
        {
            // connect to Redis
            _redisClient = await Singletons.CreateNewRedisClientAsync();

            // create a list of all eventPriorityKeys
            List<string> eventPriorityKeyList = new List<string>();
            for (int i = 7; i >= 0; i--)
            {
                eventPriorityKeyList.Add("service:*/event#incoming-notifications" + i.ToString());
            };
            string[] eventPriorityKeys = eventPriorityKeyList.ToArray();

            while (true)
            {
                // wait for a new event to be enqueued; then dequeue it (always dequeuing from the highest-priority event queue first)
                var queuedEvent = await _redisClient.ListPopLeftBlockingAsync<string, byte[]>(eventPriorityKeys, null);

                // retrieve our event priority
                string keyName = queuedEvent.Key;
                int priority = int.Parse(keyName.Substring("service:*/event#incoming-notifications".Length));

                // parse out the event creation timestamp and the resource path of the event source
                // NOTE: the event value is: [8-byte unix timestamp in milliseconds][resourceuri#eventname][1 byte of 0x00][event json]
                byte[] eventAsBytes = queuedEvent.Value;
                long createdTimestamp = BitConverter.ToInt64(eventAsBytes, 0);
                int resourceStartPos = 8;
                int resourceLength = 0;
                for (int i = resourceStartPos; i < eventAsBytes.Length; i++)
                {
                    if (eventAsBytes[i] == 0x00)
                    {
                        resourceLength = i - resourceStartPos;
                    }
                }
                string resourcePathAndEvent = Encoding.UTF8.GetString(eventAsBytes, resourceStartPos, resourceLength);
                int jsonStartPos = resourceStartPos + resourceLength + 1;
                string jsonEncodedEvent = Encoding.UTF8.GetString(eventAsBytes, jsonStartPos, eventAsBytes.Length - jsonStartPos);

                // split the resourcePath and its eventName
                if (resourcePathAndEvent.IndexOf("#") < 0) continue; // safety check: if the resource is not validly formatted, skip to the next event
                string resourcePath = resourcePathAndEvent.Substring(0, resourcePathAndEvent.IndexOf("#"));
                string eventName = resourcePathAndEvent.Substring(resourcePathAndEvent.IndexOf("#") + 1);

                // calculate the root resource for the account (to make sure we don't "bubble" the event handler any higher than the account's root)
                if (resourcePath.IndexOf("/accounts/") != 0) continue; // sanity and security check: resource must start with the accounts tag
                if (resourcePath.Length <= "/accounts/".Length) continue; // sanity and security check: resource must contain an account id
                //int accountStartPos = "/accounts/".Length;
                //int rootAccountResourcePos = resourcePath.IndexOf("/", accountStartPos);
                //if (rootAccountResourcePos < 0) continue; // sanity and security check: resource must contain an account id

                // find all subscriptions which are subscribed to receive this event
                List<string> listeningSubscriptions = new List<string>();
                List<string> resourcePathList = new List<string>();
                string tempResourcePath = resourcePath;
                // bubble up the resource tree, to find all parent resources which could also be subscribed to this event
                while (tempResourcePath.Length > "/accounts/".Length)
                {
                    resourcePathList.Add(tempResourcePath);
                    int parentPathElementPos = tempResourcePath.LastIndexOf('/' /*, tempResourcePath.Length - 2 */);
                    tempResourcePath = tempResourcePath.Substring(0, parentPathElementPos);
                }
                // search all event-subscription keys to determine which subscriptions are subscribed to receive this event
                foreach (string resourcePathElement in resourcePathList)
                {
                    List<string> subscriptionIds = await _redisClient.SetMembersAsync<string, string>("event:" + eventName + "[scope@" + resourcePathElement + "]" + "#" + "subscriptions");
                    foreach (string subscriptionId in subscriptionIds)
                    {
                        //string subscriptionId = subscriptionIdAndTag.Substring(0, subscriptionId.IndexOf(" "));
                        //string tag = subscriptionId.Substring(subscriptionIdAndTag.IndexOf(" ") + 1);

                        // establish our subscription's base key
                        string subscriptionBaseKey = "subscription:" + subscriptionId;
                        long currentTimestamp;
                        long subMillisecondIndex;
                        StringBuilder luaBuilder;
                        List<string> arguments;
                        int iArgument;
                        List<string> keys;

                        // NOTE: we put this code in a loop for the rare-to-impossible case that we experience more than 1 million events/subscription/millisecond
                        while (true)
                        {
                            // prefix the current timestamp to the event json
                            currentTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                            /* now, we make sure that the current timestamp is unique: 
                             *   - the following lua function passes our UnixTimeMilliseconds timestamp to Redis,
                             *   - then compares the timestamp to "subscription:account_id/subscription_id" field "last-incoming-notification-time"
                             *   - if the timestamp is different than the existing timestamp: stores the new timestamp in memory,
                             *     sets the field "last-incoming-notification-time-incrementer" to 0 and returns that "incrementer" value "0".
                             *   - if the timestamp is the same then we instead increment the field "last-incoming-notification-time-incrementer" and return its new value.
                            */
                            // NOTE: this function is written in Lua because this operation must happen atomically
                            // if the subscription cannot be found, the function immediately returns 0 (and does not inadvertently recreate the key)
                            luaBuilder = new StringBuilder();
                            arguments = new List<string>();
                            iArgument = 1;
                            /* switch replication to "commands replication" instead of "script replication" for this script (required because we obtain a non-static piece of data, the timestamp, in this script and sometimes then write it out as data */
                            // NOTE: this feature requires Redis 3.2.0 or newer
                            luaBuilder.Append("redis.replicate_commands()\n");
                            //
                            // check and make sure that the subscription still exists (for that edge case) and, if not then return "-1" as error. 
                            luaBuilder.Append("if redis.call(\"EXISTS\", KEYS[1]) == 0 then\n");
                            luaBuilder.Append("  return {-1, -1}\n");
                            luaBuilder.Append("end\n");

                            luaBuilder.Append("local currentTime = tonumber(ARGV[" + iArgument.ToString() + "])\n");
                            arguments.Add(currentTimestamp.ToString());
                            iArgument++;
                            luaBuilder.Append("local currentTimeArray = redis.call(\"TIME\")\n");
                            luaBuilder.Append("local currentTime = (currentTimeArray[1] * 1000) + math.floor(currentTimeArray[2] / 1000)\n");
                            //
                            luaBuilder.Append("local returnValue = 0\n");
                            // get current timestamp
                            luaBuilder.Append("local previousTime = tonumber(redis.call(\"HGET\", KEYS[1], \"last-incoming-notification-time\"))\n");
                            // if the currentTime is newer than the previousTime, then overwrite "last-incoming-notification-time", etc.
                            luaBuilder.Append("if (previousTime == false) or (currentTime > previousTime) then\n");
                            luaBuilder.Append("  redis.call(\"HSET\", KEYS[1], \"last-incoming-notification-time\", currentTime)\n");
                            luaBuilder.Append("  redis.call(\"HSET\", KEYS[1], \"last-incoming-notification-time-incrementer\", 0)\n");
                            luaBuilder.Append("  returnValue = 0\n");
                            luaBuilder.Append("else\n");
                            luaBuilder.Append("  returnValue = redis.call(\"HINCRBY\", KEYS[1], \"last-incoming-notification-time-incrementer\", 1)\n");
                            luaBuilder.Append("end\n");
                            // return our returnValue (the incrementer)
                            luaBuilder.Append("return {currentTime, returnValue}\n");

                            keys = new List<string>();
                            keys.Add(subscriptionBaseKey);

                            // NOTE: if the returned incrementer value is >= 1 million, it's too large and we need to wait until the next millisecond and try again.
                            //       (practically speaking, anywhere close to 1 million events per millisecond should never ever ever happen in our software)
                            object[] returnObjects = await _redisClient.EvalAsync<string, string, object[]>(luaBuilder.ToString(), keys.ToArray(), arguments.ToArray()).ConfigureAwait(false);
                            currentTimestamp = (long)returnObjects[0];
                            subMillisecondIndex = (long)returnObjects[1];

                            if (subMillisecondIndex < 1000000)
                                break;
                        }

                        byte[] currentTimeAsByteArray = BitConverter.GetBytes((currentTimestamp * 1000000) + subMillisecondIndex);
                        byte[] jsonAsByteArray = Encoding.UTF8.GetBytes(jsonEncodedEvent);
                        byte[] subscriptionEventValueAsByteArray = new byte[currentTimeAsByteArray.Length + jsonAsByteArray.Length];
                        Array.Copy(currentTimeAsByteArray, 0, subscriptionEventValueAsByteArray, 0, currentTimeAsByteArray.Length);
                        Array.Copy(jsonAsByteArray, 0, subscriptionEventValueAsByteArray, currentTimeAsByteArray.Length, jsonAsByteArray.Length);

                        // now queue this event into each subscription's respective incoming notification queue, check for the current waitkey and
                        // if the waikey is empty we should add a dummy value so that the subscriber dequeues the new incoming event
                        //
                        // NOTE: this function is written in Lua because this operation must happen atomically
                        // if the subscription cannot be found, the function immediately fails so that the event key is not inadvertenly created
                        luaBuilder = new StringBuilder();
                        List<byte[]> argumentsAsByteArrays = new List<byte[]>();
                        iArgument = 1;
                        // check and make sure that the subscription still exists (for that edge case) and, if not then return "-1" as error. 
                        luaBuilder.Append("if redis.call(\"EXISTS\", KEYS[1]) == 0 then\n");
                        luaBuilder.Append("  return {-1, -1}\n"); // return -1 if the subscription doesn't exist (so we can short-circuit our verification logic
                        luaBuilder.Append("end\n");
                        // add the event to the appropriate incoming event queue
                        luaBuilder.Append("local addCount = redis.call(\"ZADD\", KEYS[2], ARGV[" + iArgument.ToString() + "], ARGV[" + (iArgument + 1).ToString() + "])\n");
                        argumentsAsByteArrays.Add(_redisClient.Encoding.GetBytes(createdTimestamp.ToString()));
                        iArgument++;
                        argumentsAsByteArrays.Add(subscriptionEventValueAsByteArray);
                        iArgument++;
                        // check for the current subscription waitkey; if a waitkey exists and is empty then add a dummy value so that it dequeues the new incoming event
                        luaBuilder.Append("local connectionId = redis.call(\"HGET\", KEYS[1], \"connection-id\")\n");
                        luaBuilder.Append("if connectionId == false then\n"); // null values are if-evaluated as false; we'll change this to -1 to mean "no connection"
                        luaBuilder.Append("  connectionId = -1\n");
                        luaBuilder.Append("end\n");
                        luaBuilder.Append("return {addCount, tonumber(connectionId)}\n");

                        keys = new List<string>();
                        keys.Add(subscriptionBaseKey);
                        keys.Add(subscriptionBaseKey + "#incoming-notifications" + priority.ToString());

                        object[] addCountAndConnectionIdArray = await _redisClient.EvalAsync<string, byte[], object[]>(luaBuilder.ToString(), keys.ToArray(), argumentsAsByteArrays.ToArray()).ConfigureAwait(false);
                        //object[] addCountAndConnectionIdArray = (object[])addCountAndConnectionId;

                        bool subscriptionAndWaitKeyExist = true;
                        var addCount = (long)addCountAndConnectionIdArray[0];
                        if (addCount == -1)
                        {
                            // this is no subscription
                            subscriptionAndWaitKeyExist = false;
                        }
                        else if (addCount == 0)
                        {
                            // we failed to add the event; error!
                        }
                        long connectionId = (long)addCountAndConnectionIdArray[1];
                        if (connectionId == -1)
                        {
                            // there is no connectionId
                            subscriptionAndWaitKeyExist = false;
                        }
                        else
                        {
                            connectionId = (long)addCountAndConnectionIdArray[1];
                        }
                        // if our subscription exists and a wait key exists, check to see if the wait key has any elements; if not then add one
                        if (subscriptionAndWaitKeyExist)
                        {
                            luaBuilder = new StringBuilder();
                            arguments = new List<string>();
                            iArgument = 1;
                            //
                            luaBuilder.Append("local connectionId = ARGV[" + iArgument.ToString() + "]\n");
                            arguments.Add(connectionId.ToString());
                            iArgument++;
                            // check to make sure the connection-id has not changed
                            luaBuilder.Append("local verifyConnectionId = redis.call(\"HGET\", KEYS[1], ARGV[" + iArgument.ToString() + "])\n");
                            arguments.Add("connection-id");
                            iArgument++;
                            luaBuilder.Append("if connectionId == verifyConnectionId then\n");
                            // if the connection matches, then check to see if the wait key exists; if the wait key does not exist, push it a blank element
                            luaBuilder.Append("  if redis.call(\"EXISTS\", KEYS[2]) == 0 then\n");
                            luaBuilder.Append("    redis.call(\"RPUSH\", KEYS[2], ARGV[" + iArgument.ToString() + "])\n");
                            arguments.Add("");
                            iArgument++;
                            luaBuilder.Append("  end\n");
                            luaBuilder.Append("end\n");

                            keys = new List<string>();
                            keys.Add(subscriptionBaseKey);
                            keys.Add(subscriptionBaseKey + "#waitkey-" + connectionId.ToString());

                            await _redisClient.EvalAsync<string, string, object>(luaBuilder.ToString(), keys.ToArray(), arguments.ToArray()).ConfigureAwait(false);
                        }
                    }
                }
            }
        }
    }
}
