using System.Threading.Tasks;
using Confluent.Kafka;

namespace ProducerApp.Interfaces
{
    public abstract class Producer
    {
        public virtual async Task Run(ProducerConfig config, string kafkaTopic){
            await Task.Run(() =>  {  }); 
        }
    }
}
