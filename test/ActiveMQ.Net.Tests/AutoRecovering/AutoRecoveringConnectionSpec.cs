﻿using System;
using System.Threading;
using System.Threading.Tasks;
using ActiveMQ.Net.Tests.Utils;
using Amqp.Framing;
using Amqp.Handler;
using Xunit;

namespace ActiveMQ.Net.Tests.AutoRecovering
{
    public class AutoRecoveringConnectionSpec : ActiveMQNetSpec
    {
        [Fact]
        public async Task Should_reconnect_when_broker_is_available_after_outage_is_over()
        {
            var address = GetUniqueAddress();
            var connectionOpened = new ManualResetEvent(false);

            var testHandler = new TestHandler(@event =>
            {
                switch (@event.Id)
                {
                    case EventId.ConnectionRemoteOpen:
                        connectionOpened.Set();
                        break;
                }
            });

            var host1 = CreateOpenedContainerHost(address, testHandler);

            var connection = await CreateConnection(address);
            Assert.NotNull(connection);
            Assert.True(connectionOpened.WaitOne(TimeSpan.FromSeconds(1)));

            host1.Dispose();

            connectionOpened.Reset();
            using var host2 = CreateOpenedContainerHost(address, testHandler);

            Assert.True(connectionOpened.WaitOne(TimeSpan.FromSeconds(1)));
        }

        [Fact]
        public async Task Should_not_try_to_reconnect_when_connection_explicitly_closed()
        {
            var address = GetUniqueAddress();
            var connectionOpened = new ManualResetEvent(false);

            var testHandler = new TestHandler(@event =>
            {
                switch (@event.Id)
                {
                    case EventId.ConnectionRemoteOpen:
                        connectionOpened.Set();
                        break;
                }
            });

            using var host = CreateOpenedContainerHost(address, testHandler);

            var connection = await CreateConnection(address);
            Assert.NotNull(connection);
            Assert.True(connectionOpened.WaitOne(TimeSpan.FromSeconds(1)));

            connectionOpened.Reset();
            await connection.DisposeAsync();

            Assert.False(connectionOpened.WaitOne(TimeSpan.FromMilliseconds(50)));
        }
        
        [Fact]
        public async Task Should_recreate_producers_on_connection_recovery()
        {
            var address = GetUniqueAddress();
            var producersAttached = new CountdownEvent(2);
            var testHandler = new TestHandler(@event =>
            {
                switch (@event.Id)
                {
                    case EventId.LinkRemoteOpen when @event.Context is Attach attach && attach.Role:
                        producersAttached.Signal();
                        break;
                }
            });

            var host1 = CreateOpenedContainerHost(address, testHandler);

            var connection = await CreateConnection(address);
            connection.CreateProducer("a1");
            connection.CreateProducer("a2");

            Assert.True(producersAttached.Wait(TimeSpan.FromSeconds(1)));
            producersAttached.Reset();

            host1.Dispose();

            using var host2 = CreateOpenedContainerHost(address, testHandler);

            Assert.True(producersAttached.Wait(TimeSpan.FromSeconds(1)));
        }
    }
}