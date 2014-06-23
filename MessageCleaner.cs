using System;
using System.Messaging;
using log4net;

namespace QueueMessaging
{
    /// <summary>
    /// Manage queue message purging 
    /// </summary>
    public class MessageCleaner
    {
        static object lockObject = new Object();

        /// <summary>
        /// http://msdn.microsoft.com/en-us/library/system.messaging.messagequeue.purge(v=vs.110).aspx
        /// </summary>
        /// <param name="queueName"></param>
        public void DeleteAllMessages(string queueName)
        {
            ILog logger = LogManager.GetLogger(GetType().FullName);

            lock (lockObject)
            {
                 if (string.IsNullOrEmpty(queueName))
                {
                    Console.WriteLine("Queue name is null or empty.");
                    if (logger != null)
                        logger.Error("Queue name is null or empty.");
                    return;
                }

                try
                {
                    if (!MessageQueue.Exists(queueName))
                    {
                        Console.WriteLine("Queue {0} does not exist.", queueName);
                        if (logger != null)
                            logger.Error(string.Format("Queue {0} does not exist.", queueName));
                        return;
                    }

                    MessageQueue queue = new MessageQueue(queueName);
                    Console.WriteLine("Purging messages in queue=\"{0}\", machine=\"{1}\"...", queueName, queue.MachineName);
                    if (logger != null)
                        logger.Info(string.Format("Purging messages in queue=\"{0}\", machine=\"{1}\"...", queueName, queue.MachineName));
                    // Delete all queue messages
                    queue.Purge();
                    Console.WriteLine("Messages purging done.");
                    if (logger != null)
                        logger.Info("Messages purging done.");
                }
                catch (Exception exc)
                {
                    Console.Error.WriteLine(exc.Message);
                    if (logger != null)
                        logger.Error("Error purging message queue", exc);
                }
            }
        }
    }
}
