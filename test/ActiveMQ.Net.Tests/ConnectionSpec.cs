﻿using System.Threading;
using System.Threading.Tasks;
using ActiveMQ.Net.Tests.Utils;
using Amqp.Handler;
using Xunit;
using Xunit.Abstractions;

namespace ActiveMQ.Net.Tests
{
    public class ConnectionSpec : ActiveMQNetSpec
    {
        public ConnectionSpec(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task New_connection_should_implicitly_open_new_session()
        {
            var endpoint = GetUniqueEndpoint();
            var sessionOpened = new ManualResetEvent(false);

            var testHandler = new TestHandler(@event =>
            {
                switch (@event.Id)
                {
                    case EventId.SessionRemoteOpen:
                        sessionOpened.Set();
                        break;
                }
            });

            using var host = CreateOpenedContainerHost(endpoint, testHandler);

            await using var connection = await CreateConnection(endpoint);

            Assert.True(sessionOpened.WaitOne());
        }

        [Fact]
        public async Task Should_create_and_close_connection()
        {
            var endpoint = GetUniqueEndpoint();
            var connectionOpened = new ManualResetEvent(false);
            var connectionClosed = new ManualResetEvent(false);

            var testHandler = new TestHandler(@event =>
            {
                switch (@event.Id)
                {
                    case EventId.ConnectionRemoteOpen:
                        connectionOpened.Set();
                        break;
                    case EventId.ConnectionRemoteClose:
                        connectionClosed.Set();
                        break;
                }
            });

            using var host = CreateOpenedContainerHost(endpoint, testHandler);

            var connection = await CreateConnection(endpoint);
            await connection.DisposeAsync();

            Assert.True(connectionOpened.WaitOne());
            Assert.True(connectionClosed.WaitOne());
        }
    }
}