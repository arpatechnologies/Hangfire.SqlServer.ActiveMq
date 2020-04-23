// Copyright (c) 2020 Mirco Tamburini

using System;
using System.Collections.Generic;
using System.Linq;
using Apache.NMS;
using Hangfire.SqlServer;

namespace Hangfire.SqlServer.ActiveMq
{
    internal class QueueStorageMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private readonly IConnection _client;
        private readonly string[] _queues;

        public QueueStorageMonitoringApi(IConnection client, string[] queues)
        {
            if (client == null) throw new ArgumentNullException("client");
            if (queues == null) throw new ArgumentNullException("queues");

            _client = client;
            _queues = queues;
        }

        public IEnumerable<string> GetQueues()
        {
            return _queues;
        }


        /// <summary>
        /// Bisogna andare in browsing di tutti i messaggi..... con Apache NMS
        /// </summary>
        /// <param name="queue"></param>
        /// <returns></returns>
        private IEnumerable<long> GetEnqueuedJobIds(string queue)
        {
            List<long> jobIds = new List<long>();

            if (!_client.IsStarted)
                _client.Start();

            using (ISession session = _client.CreateSession())
            {
                if (!_client.IsStarted)
                    _client.Start();

                try
                {
                    IDestination destination = session.GetQueue(queue);
                    using (IMessageConsumer consumer = session.CreateConsumer(destination))
                    {
                        var q = session.GetQueue(queue);
                        var b = session.CreateBrowser(q);
                        var msgs = b.GetEnumerator();
                        while (msgs.MoveNext())
                        {
                            ITextMessage message = msgs.Current as ITextMessage;
                            jobIds.Add(Convert.ToInt64(message.Text));
                        }
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

            return jobIds;
        }


        public IEnumerable<long> GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            return GetEnqueuedJobIds(queue).Skip(from).Take(perPage);
        }

        public IEnumerable<long> GetFetchedJobIds(string queue, int from, int perPage)
        {
            return Enumerable.Empty<long>();
        }

        public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            var jobIds = GetEnqueuedJobIds(queue);

            return new EnqueuedAndFetchedCountDto
            {
                EnqueuedCount = jobIds.Count(),
                FetchedCount = null
            };
        }

       
    }
}
