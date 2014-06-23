using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Xml;
using log4net;

namespace QueueMessaging
{
    /// <summary>
    /// Manage queue message building 
    /// </summary>
    public class MessageBuilder
    {
        static object lockObject = new Object();

        public string BuildQueueMessage(string xmlContent)
        {
            ILog logger = LogManager.GetLogger(GetType().FullName);

            lock (lockObject)
            {
                var queueMessage = string.Empty;

                if (string.IsNullOrEmpty(xmlContent))
                {
                    Console.Error.WriteLine("Xml content is null or empty.");
                    if (logger != null)
                        logger.Warn("Xml content is null or empty.");
                    return string.Empty;
                }

                XmlDocument xmlDoc = new XmlDocument();
                try
                {
                    Console.WriteLine("Message building start.");
                    if (logger != null)
                        logger.Info("Message building start.");
                    try
                    {
                        xmlDoc.LoadXml(xmlContent);
                        // Set queued date
                        SetQueuedDate(xmlDoc, logger);
                        // Set queue message
                        queueMessage = xmlDoc.OuterXml;
                        if (logger != null)
                            logger.Debug(string.Format("Message content: \"{0}\"", queueMessage));
                    }
                    catch (XmlException xexc)
                    {
                        Console.Error.WriteLine(xexc);
                        if (logger != null)
                            logger.Error(string.Format("Building message failure: {0} - {1}", 
                                xexc.Source, xexc.Message));
                        // Dump xml into temp file
                        DumpMessage(xmlContent, logger);
                    }
                    Console.WriteLine("Message building done.");
                    if (logger != null)
                        logger.Info("Message building done.");
                }
                catch (Exception exc)
                {
                    Console.Error.WriteLine(exc);
                    if (logger != null)
                        logger.Error("Message building failure", exc);
                    // Dump xml into temp file
                    DumpMessage(xmlContent, logger);
                }
                return queueMessage;
            }
        }

        /// <summary>
        /// Dump xml content into temp file
        /// </summary>
        /// <param name="xmlContent"></param>
        /// <param name="logger"></param>
        private void DumpMessage(string xmlContent, ILog logger)
        {
            string fileName = Path.GetTempFileName();
            if (logger != null)
                logger.Warn(string.Format("Dumping xml file: {0}", fileName));
            File.WriteAllText(fileName, xmlContent);
        }
        /// <summary>
        /// Set the inner text of dateQueued node
        /// </summary>
        /// <param name="xmlDoc"></param>
        /// <param name="logger"></param>
        private void SetQueuedDate(XmlDocument xmlDoc, ILog logger)
        {
            if (xmlDoc != null)
            {
                if (logger != null)
                    logger.Info("Adding the queued date info to message...");

                XmlNamespaceManager xmlNamespaceMgr = new XmlNamespaceManager(xmlDoc.NameTable);
                xmlNamespaceMgr.AddNamespace("pa", "urn:my-schema");

                XmlNode xmlNode = xmlDoc.SelectSingleNode("pa:envelope/pa:body", xmlNamespaceMgr);
                if (xmlNode == null)
                {
                    if (logger != null)
                        logger.Warn("Cannot select <body> node.");
                }
                else
                {
                    XmlNode xmlNewNode = xmlDoc.CreateNode(XmlNodeType.Element, "pa:dateQueued", "urn:privacyaudit-schema");
                    xmlNewNode.InnerText = DateTime.Now.ToString("s");

                    XmlNode xmlOldNode = xmlDoc.SelectSingleNode("pa:envelope/pa:body/pa:dateQueued", xmlNamespaceMgr);
                    if (xmlOldNode == null)
                        xmlNode.AppendChild(xmlNewNode);
                    else
                        xmlNode.ReplaceChild(xmlNewNode, xmlOldNode);

                    if (logger != null)
                        logger.DebugFormat("Queued date node set to: {0}", xmlNewNode.InnerText);
                }
            }
        }
    }
}
