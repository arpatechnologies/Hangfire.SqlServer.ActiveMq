// Copyright (c) 2020 Mirco Tamburini

using System;
using System.Data.Common;
using System.Linq;
using System.Threading;
using Apache.NMS;
using Hangfire.SqlServer;
using Hangfire.Storage;

namespace Hangfire.SqlServer.ActiveMq
{
    internal class QueueStorageJobQueue : IPersistentJobQueue
    {
        private readonly IConnection _client;
        private readonly QueueStorageOptions _options;

        //private static readonly Hangfire.Logging.ILog Logger = Hangfire.Logging.LogProvider.For<RabbitMqJobQueue>();

        public QueueStorageJobQueue(IConnection client, QueueStorageOptions options)
        {
            if (client == null) throw new ArgumentNullException("client");
            if (options == null) throw new ArgumentNullException("options");

            _client = client;
            _options = options;
        }

        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException("queues");
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", "queues");

            if (!_client.IsStarted)
                _client.Start();

            ITextMessage message;

            using (ISession session = _client.CreateSession())
            {
                try
                {
                    var cloudQueues = queues.Select(queue => session.GetQueue(queue)).ToArray();
                    var currentQueueIndex = 0;

                    do
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        var destination = cloudQueues[currentQueueIndex];
                        using (IMessageConsumer consumer = session.CreateConsumer(destination))
                        {
                            message = consumer.Receive(_options.VisibilityTimeout) as ITextMessage;
                           
                            if (message == null)
                            {
                                if (currentQueueIndex == cloudQueues.Length - 1)
                                {
                                    cancellationToken.WaitHandle.WaitOne(_options.QueuePollInterval);
                                    cancellationToken.ThrowIfCancellationRequested();
                                }
                                currentQueueIndex = (currentQueueIndex + 1) % queues.Length;
                            }
                        }

                    } while (message == null);

                    return new QueueStorageFetchedJob(_client, cloudQueues[currentQueueIndex].QueueName, message);
                }
                catch (Exception ex)
                {
                    //_log.Error(ex);
                }
                finally
                {
                    session.Close();
                }
            }

            return null;
        }

        public void Enqueue(DbConnection connection, DbTransaction transaction, string queue, string jobId)
        {
            if (!_client.IsStarted)
                _client.Start();

            using (ISession session = _client.CreateSession())
            {
                try
                {
                    IDestination destination = null;
                    destination = session.GetQueue(queue);

                    IMessageProducer producer = session.CreateProducer(destination);
                    producer.DeliveryMode = MsgDeliveryMode.Persistent;

                    IMessage objMessage = null;
                    objMessage = producer.CreateTextMessage(jobId);

                    producer.Send(objMessage);
                    producer.Close();
                }
                catch (Exception ex)
                {
                    //_log.Error(ex);
                }
                finally
                {
                    session.Close();
                }
            }
        }

        
    }
}