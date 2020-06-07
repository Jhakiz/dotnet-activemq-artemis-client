﻿using System;
using System.Threading;
using System.Threading.Tasks;
using ActiveMQ.Artemis.Client.Exceptions;
using ActiveMQ.Artemis.Client.Transactions;
using Microsoft.Extensions.Logging;
using Nito.AsyncEx;

namespace ActiveMQ.Artemis.Client.AutoRecovering
{
    internal class AutoRecoveringConsumer : IConsumer, IRecoverable
    {
        private readonly ILogger<AutoRecoveringConsumer> _logger;
        private readonly ConsumerConfiguration _configuration;
        private readonly AsyncManualResetEvent _manualResetEvent = new AsyncManualResetEvent(true);
        private bool _closed;
        private Exception _failureCause;
        private IConsumer _consumer;

        public AutoRecoveringConsumer(ILoggerFactory loggerFactory, ConsumerConfiguration configuration)
        {
            _logger = loggerFactory.CreateLogger<AutoRecoveringConsumer>();
            _configuration = configuration;
        }

        public async ValueTask<Message> ReceiveAsync(CancellationToken cancellationToken = default)
        {
            while (true)
            {
                CheckState();

                try
                {
                    return await _consumer.ReceiveAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (ConsumerClosedException)
                {
                    CheckState();

                    Log.RetryingReceiveAsync(_logger);

                    Suspend();
                    RecoveryRequested?.Invoke();
                    await _manualResetEvent.WaitAsync(cancellationToken).ConfigureAwait(false);
                }
            }
        }

        public ValueTask AcceptAsync(Message message, Transaction transaction, CancellationToken cancellationToken = default)
        {
            CheckState();

            return _consumer.AcceptAsync(message, transaction, cancellationToken);
        }

        public void Reject(Message message, bool undeliverableHere)
        {
            CheckState();

            _consumer.Reject(message, undeliverableHere);
        }

        private void CheckState()
        {
            if (_closed)
            {
                if (_failureCause != null)
                {
                    throw new ConsumerClosedException("The Consumer was closed due to an unrecoverable error.", _failureCause);
                }
                else
                {
                    throw new ConsumerClosedException();
                }
            }
        }

        public void Suspend()
        {
            var wasSuspended = IsSuspended();
            _manualResetEvent.Reset();

            if (!wasSuspended)
            {
                Log.ConsumerSuspended(_logger);
            }
        }

        public void Resume()
        {
            var wasSuspended = IsSuspended();
            _manualResetEvent.Set();

            if (wasSuspended)
            {
                Log.ConsumerResumed(_logger);
            }
        }

        private bool IsSuspended()
        {
            return !_manualResetEvent.IsSet;
        }

        public async Task RecoverAsync(IConnection connection, CancellationToken cancellationToken)
        {
            await DisposeUnderlyingConsumerSafe().ConfigureAwait(false);
            _consumer = await connection.CreateConsumerAsync(_configuration, cancellationToken).ConfigureAwait(false);
            Log.ProducerRecovered(_logger);
        }

        public async Task TerminateAsync(Exception exception)
        {
            _closed = true;
            _failureCause = exception;
            _manualResetEvent.Set();
            await DisposeUnderlyingConsumerSafe().ConfigureAwait(false);
        }

        private async Task DisposeUnderlyingConsumerSafe()
        {
            try
            {
                await DisposeUnderlyingConsumer().ConfigureAwait(false);
            }
            catch (Exception)
            {
                // ignored
            }
        }

        public async ValueTask DisposeAsync()
        {
            await DisposeUnderlyingConsumer().ConfigureAwait(false);
            Closed?.Invoke(this);
        }
        
        private async ValueTask DisposeUnderlyingConsumer()
        {
            if (_consumer != null)
            {
                await _consumer.DisposeAsync().ConfigureAwait(false);
            }
        }

        public event Closed Closed;

        public event RecoveryRequested RecoveryRequested;

        private static class Log
        {
            private static readonly Action<ILogger, Exception> _retryingConsumeAsync = LoggerMessage.Define(
                LogLevel.Trace,
                0,
                "Retrying receive after Consumer reestablished.");
            
            private static readonly Action<ILogger, Exception> _consumerRecovered = LoggerMessage.Define(
                LogLevel.Trace,
                0,
                "Consumer recovered.");
            
            private static readonly Action<ILogger, Exception> _consumerSuspended = LoggerMessage.Define(
                LogLevel.Trace,
                0,
                "Consumer suspended.");
            
            private static readonly Action<ILogger, Exception> _consumerResumed = LoggerMessage.Define(
                LogLevel.Trace,
                0,
                "Consumer resumed.");
            
            public static void RetryingReceiveAsync(ILogger logger)
            {
                if (logger.IsEnabled(LogLevel.Trace))
                {
                    _retryingConsumeAsync(logger, null);    
                }
            }
            
            public static void ProducerRecovered(ILogger logger)
            {
                if (logger.IsEnabled(LogLevel.Trace))
                {
                    _consumerRecovered(logger, null);
                }
            }
            
            public static void ConsumerSuspended(ILogger logger)
            {
                if (logger.IsEnabled(LogLevel.Trace))
                {
                    _consumerSuspended(logger, null);
                }
            }
            
            public static void ConsumerResumed(ILogger logger)
            {
                if (logger.IsEnabled(LogLevel.Trace))
                {
                    _consumerResumed(logger, null);
                }
            }
        }
    }
}