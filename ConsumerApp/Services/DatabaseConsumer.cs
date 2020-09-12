using System.Collections.Generic;
using System.Threading;
using ConsumerApp.Interfaces;

namespace ConsumerApp.Services
{
    public class DatabaseConsumer : Consumer
    {
        public override void Run(string brokerList, List<string> topics, CancellationToken cancellationToken){ }
    }
}