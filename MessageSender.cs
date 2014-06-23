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
    /// Manage queue message sending 
    /// </summary>
    public class MessageSender
    {
        static object lockObject = new Object();

        /// <summary>
        /// http://msdn.microsoft.com/en-us/library/system.messaging.messagequeue.send(v=vs.110).aspx
        /// </summary>
        /// <param name="queueName"></param>
        /// <param name="messageContent"></param>
        /// <returns></returns>
        public bool SendQueueMessage(string queueName, string messageContent)
        {
            ILog logger = LogManager.GetLogger(GetType().FullName);

            lock (lockObject)
            {
                if (string.IsNullOrEmpty(queueName))
                {
                    Console.WriteLine("Queue name is null or empty.");
                    if (logger != null)
                        logger.Error("Queue name is null or empty.");
                    return false;
                }
                if (messageContent == null)
                {
                    Console.Error.WriteLine("Message content cannot be null.");
                    if (logger != null)
                        logger.Error("Message content cannot be null.");
                    return false;
                }
                if (logger != null)
                    logger.Debug(string.Format("Message content: \"{0}\"", messageContent));

                // The maximum length of a message queue label is 124 characters.
                string label = string.Format("AUDIT@{0}", DateTime.Now.ToString("s"));
                try
                {
                    if (!MessageQueue.Exists(queueName))
                    {
                        Console.WriteLine("Queue {0} does not exist.", queueName);
                        if (logger != null)
                            logger.Error(string.Format("Queue {0} does not exist.", queueName));
                        return false;
                    }

                    MessageQueue queue = new MessageQueue(queueName);
                    if (queue.Transactional)
                    {
                        try
                        {
                            queue.Formatter = new System.Messaging.XmlMessageFormatter(new String[] { "System.String,mscorlib" });

                            Console.WriteLine("Sending message to queue=\"{0}\", machine=\"{1}\"...", queueName, queue.MachineName);
                            if (logger != null)
                                logger.Info(string.Format("Sending message to queue=\"{0}\", machine=\"{1}\"...", queueName, queue.MachineName));

                            using (var ts = new TransactionScope())
                            {
                                queue.Send(messageContent, label, MessageQueueTransactionType.Automatic);
                                ts.Complete();
                            }

                            Console.WriteLine("Message sending done.");
                            if (logger != null)
                                logger.Info("Message sending done.");
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
                                    logger.Error(string.Format("Sending message failure: code={0} - message={1}",
                                        msgqexc.MessageQueueErrorCode, msgqexc.Message));
                                return false;
                            }
                        }
                    }
                    else
                    {
                        Console.WriteLine("Queue {0} is not transactional.", queueName);
                        if (logger != null)
                            logger.Error(string.Format("Queue {0} is not transactional.", queueName));
                        return false;
                    }
                }
                catch (Exception exc)
                {
                    Console.Error.WriteLine(exc.Message);
                    if (logger != null)
                        logger.Error("Sending message failure.", exc);
                    return false;
                }
                return true;
            }
        }
    }
}
