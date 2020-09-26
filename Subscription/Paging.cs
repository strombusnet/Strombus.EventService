using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Strombus.EventService
{
    public class Paging
    {
        public struct PagingInfo
        {
            public string previous;
            public string next;
        }

        public struct SubscriptionEventsPagingContainer
        {
            public PagingInfo? paging;
            public SubscriptionEvent[] items;
        }
    }
}
