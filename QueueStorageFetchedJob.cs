// Copyright (c) 2020 Mirco Tamburini

using System;
using Apache.NMS;
using Hangfire.Storage;

namespace Hangfire.SqlServer.ActiveMq
{
    internal class QueueStorageFetchedJob : IFetchedJob
    {
        private readonly IConnection _client;
        private readonly ITextMessage _message;
        private readonly string _queueName;

        public QueueStorageFetchedJob(IConnection client,string queueName, ITextMessage message)
        {
            if (client == null) throw new ArgumentNullException("client");
            if (message == null) throw new ArgumentNullException("message");
            if (queueName == null) throw new ArgumentNullException("queue");

            _client = client;
            _queueName = queueName;
            _message = message;

            JobId = _message.Text;
        }

        public string JobId { get; private set; }

        public void RemoveFromQueue()
        {
            if (!_client.IsStarted)
                _client.Start();

            using (ISession session = _client.CreateSession(AcknowledgementMode.AutoAcknowledge))
            {
                using (IDestination destination = session.GetQueue(_queueName))
                {
                    using (IMessageConsumer consumer = session.CreateConsumer(destination, $"JMSMessageID LIKE '%{_message.NMSMessageId}%'"))
                    {
                        //connection.Start();
                        var message = consumer.Receive(new TimeSpan(0, 0, 1));
                        consumer.Close();
                        //connection.Close();
                        if (message == null)
                        {
                            throw new Exception($"Message '{_message.NMSMessageId}' not found on queue '{_queueName}'");
                        }
                    }
                }
            }
        }

        public void Requeue()
        {

            if (!_client.IsStarted)
                _client.Start();

            using (ISession session = _client.CreateSession())
            {
                try
                {
                    IDestination destination = null;
                    destination = session.GetQueue(_queueName);

                    IMessageProducer producer = session.CreateProducer(destination);
                    producer.DeliveryMode = MsgDeliveryMode.Persistent;

                    IMessage objMessage = null;
                    objMessage = producer.CreateTextMessage(_message.Text);

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
    

        void IDisposable.Dispose()
        {

        }
    }
}