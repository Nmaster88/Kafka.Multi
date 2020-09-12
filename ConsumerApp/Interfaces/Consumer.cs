using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using ConsumerApp.DataAccess;
using ConsumerApp.Dtos;

namespace ConsumerApp.Interfaces
{
    public abstract class Consumer
    {
        public virtual void Run(string brokerList, List<string> topics, CancellationToken cancellationToken){ 
        }
    }
}
