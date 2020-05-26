using System;
using System.Threading;
using System.Threading.Tasks;
using ActiveMQ.Artemis.Client.Exceptions;
using ActiveMQ.Artemis.Client.UnitTests.Utils;
using Amqp.Handler;
using Xunit;
using Xunit.Abstractions;

namespace ActiveMQ.Artemis.Client.UnitTests
{
    public class ConnectionSpec : ActiveMQNetSpec
    {
        public ConnectionSpec(ITestOutputHelper output) : base(output)
        {
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

        [Fact]
        public async Task Throws_on_attempt_to_create_producer_using_disposed_connection()
        {
            using var host = CreateOpenedContainerHost();

            var connection = await CreateConnection(host.Endpoint);
            await connection.DisposeAsync();

            await Assert.ThrowsAsync<ObjectDisposedException>(() => connection.CreateProducerAsync("a1", AddressRoutingType.Anycast));
        }

        [Fact]
        public async Task Throws_on_attempt_to_create_anonymous_producer_using_disposed_connection()
        {
            using var host = CreateOpenedContainerHost();

            var connection = await CreateConnection(host.Endpoint);
            await connection.DisposeAsync();

            await Assert.ThrowsAsync<ObjectDisposedException>(() => connection.CreateAnonymousProducer());
        }

        [Fact]
        public async Task Throws_on_attempt_to_create_consumer_using_disposed_connection()
        {
            using var host = CreateOpenedContainerHost();

            var connection = await CreateConnection(host.Endpoint);
            await connection.DisposeAsync();

            await Assert.ThrowsAsync<ObjectDisposedException>(() => connection.CreateConsumerAsync("a1", QueueRoutingType.Anycast));
        }

        [Fact]
        public async Task Throws_on_attempt_to_create_producer_using_closed_connection()
        {
            using var host = CreateOpenedContainerHost();
            var connection = await CreateConnectionWithoutAutomaticRecovery(host.Endpoint);
            await DisposeHostAndWaitUntilConnectionNotified(host, connection);

            await Assert.ThrowsAsync<ConnectionClosedException>(() => connection.CreateProducerAsync("a1", AddressRoutingType.Anycast));
        }
        
        [Fact]
        public async Task Throws_on_attempt_to_create_anonymous_producer_using_closed_connection()
        {
            using var host = CreateOpenedContainerHost();
            var connection = await CreateConnectionWithoutAutomaticRecovery(host.Endpoint);
            await DisposeHostAndWaitUntilConnectionNotified(host, connection);

            await Assert.ThrowsAsync<ConnectionClosedException>(() => connection.CreateAnonymousProducer());
        }
        
        [Fact]
        public async Task Throws_on_attempt_to_create_consumer_using_closed_connection()
        {
            using var host = CreateOpenedContainerHost();
            var connection = await CreateConnectionWithoutAutomaticRecovery(host.Endpoint);
            await DisposeHostAndWaitUntilConnectionNotified(host, connection);

            await Assert.ThrowsAsync<ConnectionClosedException>(() => connection.CreateConsumerAsync("a1", QueueRoutingType.Anycast));
        }
    }
}