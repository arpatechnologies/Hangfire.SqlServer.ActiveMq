// Copyright (c) 2020 Mirco Tamburini

using System;
using Apache.NMS;
using Hangfire.SqlServer;
using Hangfire.States;


namespace Hangfire.SqlServer.ActiveMq
{
    public static class QueueStorageSqlServerStorageExtensions
    {
        public static SqlServerStorage UseActiveMq(this SqlServerStorage storage, string activeMqUri, QueueStorageOptions options, params string[] queues)
        {
            if (storage == null) throw new ArgumentNullException("storage");
            if (queues == null) throw new ArgumentNullException("queues");
            if (queues.Length == 0) throw new ArgumentException("No queue(s) specified for RabbitMQ provider.", "queues");
            if (string.IsNullOrEmpty(activeMqUri)) throw new ArgumentNullException("activeMqUri is null");

            if (options == null)
            {
                options = new QueueStorageOptions();
            }

            var factory = new Apache.NMS.ActiveMQ.ConnectionFactory(activeMqUri);
            IConnection client = factory.CreateConnection();

            var provider = new QueueStorageJobQueueProvider(client, options, queues);
            storage.QueueProviders.Add(provider, queues);

            return storage;
        }

        public static SqlServerStorage UseActiveMq(this SqlServerStorage storage, string activeMqUri, params string[] queues)
        {
            return UseActiveMq(storage,  activeMqUri, null, queues);
        }
    }
}
