// Copyright (c) 2020 Mirco Tamburini

using System;

namespace Hangfire.SqlServer.ActiveMq
{
    public class QueueStorageOptions
    {
        public QueueStorageOptions()
        {
            VisibilityTimeout = TimeSpan.FromMinutes(30);
            QueuePollInterval = TimeSpan.FromSeconds(5);
        }

        public TimeSpan VisibilityTimeout { get; set; }
        public TimeSpan QueuePollInterval { get; set; }
    }
}