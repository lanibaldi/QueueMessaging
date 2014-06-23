using System;
using System.Collections.Generic;
using System.Linq;
using System.Messaging;
using System.Text;
using System.Transactions;
using log4net;

namespace QueueMessaging
{
    /// <summary>
    /// Manage queue message receiving
    /// </summary>
    public class MessageReceiver
    {
        static object lockObject = new Object();

        /// <summary>
        /// http://msdn.microsoft.com/en-us/library/system.messaging.messagequeue.receive(v=vs.110).aspx
        /// </summary>
        /// <param name="queueName"></param>
        /// <returns></returns>
        public Message ReceiveQueueMessage(string queueName)
        {
            ILog logger = LogManager.GetLogger(GetType().FullName);

            lock (lockObject)
            {                
                if (string.IsNullOrEmpty(queueName))
                {
                    Console.WriteLine("Queue name is null or empty.");
                    if (logger != null)
                        logger.Error("Queue name is null or empty.");
                    return null;
                }

                Message msg = null;
                try
                {
                    if (!MessageQueue.Exists(queueName))
                    {
                        Console.WriteLine("Queue {0} does not exist.", queueName);
                        if (logger != null)
                            logger.Error(string.Format("Queue {0} does not exist.", queueName));
                        return null;
                    }
                    try
                    {
                        MessageQueue queue = new MessageQueue(queueName);
                        queue.Formatter = new System.Messaging.XmlMessageFormatter(new String[] { "System.String,mscorlib" });

                        Console.WriteLine("Receiving message from queue=\"{0}\", machine=\"{1}\"...", queueName, queue.MachineName);
                        if (logger != null)
                            logger.Info(string.Format("Receiving message from queue=\"{0}\", machine=\"{1}\"...", queueName, queue.MachineName));
                        using (var ts = new TransactionScope())
                        {
                            msg = queue.Receive(TimeSpan.FromSeconds(10), MessageQueueTransactionType.Automatic);
                            ts.Complete();
                        }
                        Console.WriteLine("Message receiving done.");
                        if (logger != null)
                            logger.Info("Message receiving done.");

                        // Verify message
                        if (msg == null)
                        {
                            if (logger != null)
                                logger.Warn("Message is null.");
                        }
                        else
                        {
                            // Message label cannot be null
                            if (logger != null)
                            {
                                logger.Debug(string.Format("Message label=\"{0}\"", msg.Label));
                                logger.Debug(string.Format("Message content=\"{0}\"", (msg.Body == null ? "NONE" : msg.Body.ToString())));
                            }
                        }
                    }
                    catch (MessageQueueException msgqexc)
                    {
                        if (msgqexc.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout)
                        {
                            Console.WriteLine("Timeout expired.");
                            if (logger != null)
                                logger.Warn("Timeout expired.");
                        }
                        else
                        {
                            Console.Error.WriteLine(msgqexc);
                            // http://msdn.microsoft.com/en-us/library/system.messaging.messagequeueerrorcode(v=vs.110).aspx
                            if (logger != null)
                                logger.Error(string.Format("Receiving message failure: {0} - {1}",
                                    msgqexc.MessageQueueErrorCode, msgqexc.Message));
                        }
                    }
                }
                catch (Exception exc)
                {
                    Console.Error.WriteLine(exc.Message);
                    if (logger != null)
                        logger.Error("Receiving message failure.", exc);
                }
                return msg;
            }
        }
    }
}
