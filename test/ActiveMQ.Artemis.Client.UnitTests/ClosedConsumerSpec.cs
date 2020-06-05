﻿using System;
using System.Threading.Tasks;
using ActiveMQ.Artemis.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace ActiveMQ.Artemis.Client.UnitTests
{
    public class DisposedConsumerSpec : ActiveMQNetSpec
    {
        public DisposedConsumerSpec(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Throws_on_attempt_to_receive_message_using_disposed_consumer()
        {
            using var host = CreateOpenedContainerHost();
            var messageSource = host.CreateMessageSource("a1");

            await using var connection = await CreateConnection(host.Endpoint);
            var consumer = await connection.CreateConsumerAsync("a1", RoutingType.Anycast);

            messageSource.Enqueue(new Message("foo"));

            await consumer.DisposeAsync();

            await Assert.ThrowsAsync<ObjectDisposedException>(async () => await consumer.ReceiveAsync(CancellationToken));
        }

        [Fact]
        public async Task Throws_on_attempt_to_receive_message_when_connection_disconnected()
        {
            using var host = CreateOpenedContainerHost();
            var messageSource = host.CreateMessageSource("a1");

            var connection = await CreateConnectionWithoutAutomaticRecovery(host.Endpoint);
            var consumer = await connection.CreateConsumerAsync("a1", RoutingType.Anycast);

            messageSource.Enqueue(new Message("foo"));

            await DisposeHostAndWaitUntilConnectionNotified(host, connection);

            await Assert.ThrowsAsync<ConsumerClosedException>(async () => await consumer.ReceiveAsync(CancellationToken));
        }

        [Fact]
        public async Task Throws_on_attempt_to_receive_message_when_connection_disposed()
        {
            using var host = CreateOpenedContainerHost();
            var messageSource = host.CreateMessageSource("a1");

            var connection = await CreateConnectionWithoutAutomaticRecovery(host.Endpoint);
            var consumer = await connection.CreateConsumerAsync("a1", RoutingType.Anycast);

            messageSource.Enqueue(new Message("foo"));

            await connection.DisposeAsync();

            await Assert.ThrowsAsync<ConsumerClosedException>(async () => await consumer.ReceiveAsync(CancellationToken));
        }

        [Fact]
        public async Task Throws_on_attempt_to_accept_message_using_disposed_consumer()
        {
            using var host = CreateOpenedContainerHost();
            var messageSource = host.CreateMessageSource("a1");

            await using var connection = await CreateConnection(host.Endpoint);
            var consumer = await connection.CreateConsumerAsync("a1", RoutingType.Anycast);

            messageSource.Enqueue(new Message("foo"));

            var msg = await consumer.ReceiveAsync(CancellationToken);

            await consumer.DisposeAsync();

            await Assert.ThrowsAsync<ObjectDisposedException>(async () => await consumer.AcceptAsync(msg));
        }


        [Fact]
        public async Task Throws_on_attempt_to_accept_message_when_when_connection_disconnected()
        {
            using var host = CreateOpenedContainerHost();
            var messageSource = host.CreateMessageSource("a1");

            await using var connection = await CreateConnection(host.Endpoint);
            var consumer = await connection.CreateConsumerAsync("a1", RoutingType.Anycast);

            messageSource.Enqueue(new Message("foo"));

            var msg = await consumer.ReceiveAsync(CancellationToken);

            await DisposeHostAndWaitUntilConnectionNotified(host, connection);

            await Assert.ThrowsAsync<ConsumerClosedException>(async () => await consumer.AcceptAsync(msg));
        }

        [Fact]
        public async Task Throws_on_attempt_to_accept_message_when_connection_disposed()
        {
            using var host = CreateOpenedContainerHost();
            var messageSource = host.CreateMessageSource("a1");

            await using var connection = await CreateConnection(host.Endpoint);
            var consumer = await connection.CreateConsumerAsync("a1", RoutingType.Anycast);

            messageSource.Enqueue(new Message("foo"));

            var msg = await consumer.ReceiveAsync(CancellationToken);

            await connection.DisposeAsync();

            await Assert.ThrowsAsync<ConsumerClosedException>(async () => await consumer.AcceptAsync(msg));
        }

        [Fact]
        public async Task Throws_on_attempt_to_reject_message_using_disposed_consumer()
        {
            using var host = CreateOpenedContainerHost();
            var messageSource = host.CreateMessageSource("a1");

            await using var connection = await CreateConnection(host.Endpoint);
            var consumer = await connection.CreateConsumerAsync("a1", RoutingType.Anycast);

            messageSource.Enqueue(new Message("foo"));

            var msg = await consumer.ReceiveAsync(CancellationToken);

            await consumer.DisposeAsync();

            Assert.Throws<ObjectDisposedException>(() => consumer.Reject(msg));
        }


        [Fact]
        public async Task Throws_on_attempt_to_reject_message_when_when_connection_disconnected()
        {
            using var host = CreateOpenedContainerHost();
            var messageSource = host.CreateMessageSource("a1");

            await using var connection = await CreateConnection(host.Endpoint);
            var consumer = await connection.CreateConsumerAsync("a1", RoutingType.Anycast);

            messageSource.Enqueue(new Message("foo"));

            var msg = await consumer.ReceiveAsync(CancellationToken);

            await DisposeHostAndWaitUntilConnectionNotified(host, connection);

            Assert.Throws<ConsumerClosedException>(() => consumer.Reject(msg));
        }

        [Fact]
        public async Task Throws_on_attempt_to_reject_message_when_connection_disposed()
        {
            using var host = CreateOpenedContainerHost();
            var messageSource = host.CreateMessageSource("a1");

            await using var connection = await CreateConnection(host.Endpoint);
            var consumer = await connection.CreateConsumerAsync("a1", RoutingType.Anycast);

            messageSource.Enqueue(new Message("foo"));

            var msg = await consumer.ReceiveAsync(CancellationToken);

            await connection.DisposeAsync();

            Assert.Throws<ConsumerClosedException>(() => consumer.Reject(msg));
        }
    }
}