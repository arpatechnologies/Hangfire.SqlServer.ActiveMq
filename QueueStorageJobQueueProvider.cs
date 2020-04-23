// Copyright (c) 2020 Mirco Tamburini

using System;
using System.Data;
using Apache.NMS;
using Hangfire.SqlServer;


namespace Hangfire.SqlServer.ActiveMq
{
    internal class QueueStorageJobQueueProvider : IPersistentJobQueueProvider
    {
        private readonly IConnection _client;
        private readonly QueueStorageOptions _options;
        private readonly string[] _queues;

        public QueueStorageJobQueueProvider(
            IConnection client, 
            QueueStorageOptions options, 
            string[] queues)
        {
            if (client == null) throw new ArgumentNullException("client");
            if (options == null) throw new ArgumentNullException("options");
            if (queues == null) throw new ArgumentNullException("queues");

            _client = client;
            _options = options;
            _queues = queues;

            CreateQueuesIfNotExists();
        }

        public IPersistentJobQueue GetJobQueue()
        {
            return new QueueStorageJobQueue(_client, _options);
        }
        
        public IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi()
        {
            return new QueueStorageMonitoringApi(_client, _queues);
        }

        private void CreateQueuesIfNotExists()
        {
            if (!_client.IsStarted)
                _client.Start();

            using (ISession session = _client.CreateSession())
            {
                try
                {
                    foreach (var queue in _queues)
                    {
                        IDestination destination = null;
                        destination = session.GetQueue(queue);
                    }
         
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
