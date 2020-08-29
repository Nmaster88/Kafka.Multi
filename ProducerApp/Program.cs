using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Confluent.Kafka;
using ProducerApp.Dtos;
using System.Text.Json;
using System.Collections.Generic;
using ProducerApp.Types;
using NDesk.Options;
using ProducerApp.Interfaces;
using ProducerApp.Services;

namespace ProducerApp
{
    class Program
    {
        public static async Task Produce(ProducerConfig config, string kafkaTopic, string mode)
        {
            Producer producer;

            //Modes of the producer
            //single - each line is write in the console
            //multi - produces a lot of messages
            if(String.IsNullOrEmpty(mode))
            {
                mode = ProducerTypes.multi.ToString();
            }
            else
            {
                ProducerTypes producerType;
                if(Enum.TryParse(mode, out producerType))
                {
                    if(Enum.IsDefined(typeof(ProducerTypes),mode))
                    {
                        mode = producerType.ToString();
                    }
                    else
                    {
                        Console.WriteLine("The producer type passed as argument doesn't exits. mode defaults to multi");
                        mode = ProducerTypes.multi.ToString();
                    }
                }
                else
                {
                    mode = ProducerTypes.multi.ToString();
                }
            }

            if(mode == ProducerTypes.single.ToString())
            {
                producer = new SingleProducer();
            }
            else if(mode == ProducerTypes.multi.ToString())
            {
                producer = new MultiSepProducer();
            } 
            else
            {
                producer = new MultiSepProducer();
            } 

            await producer.Run(config,kafkaTopic);       
        }

        static void ShowHelp (OptionSet p)
        {
            Console.WriteLine ("Options:");
            p.WriteOptionDescriptions (Console.Out);
        }

        public static async Task Main(string[] args)
        {
            #region " kafka configuration "
                // The Kafka endpoint address
                string kafkaEndpoint = "127.0.0.1:9092";

                // The Kafka topic we'll be using
                string kafkaTopic = "new-message-topic";

                // Create the producer configuration
                var config = new ProducerConfig { BootstrapServers = kafkaEndpoint, EnableDeliveryReports = false, QueueBufferingMaxMessages = 200000 };
            #endregion

            #region " options passed as arguments "
            bool show_help = false;
            bool show_version = false;
            string mode = "";

            var p = new OptionSet () {
                { "v|version", "The version of kafka producer test application.",
                v => show_version = v != null },
                { "m|mode=", "Choose the mode that this application will use. It can be Single or Multi.",
                v => mode = v },
                { "h|help",  "Show this message and exit", 
                v => show_help = v != null },
            };

            try {
                p.Parse (args);
            }
            catch (OptionException e) {
                Console.Write ("kafka: ");
                Console.WriteLine (e.Message);
                Console.WriteLine ("Try `--help' for more information.");
                return;
            }

            if (show_help) {
                ShowHelp (p);
                return;
            }

            if (show_version) {
                Console.WriteLine ("Version 1.0.0 for producer and consumer test application of Kafka confluent.");
                return;
            }

            #endregion

            await Produce(config, kafkaTopic, mode);          
        }

    }
}
