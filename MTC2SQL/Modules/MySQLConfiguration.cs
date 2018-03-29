using System;
using System.IO;
using System.Xml;
using System.Xml.Serialization;
using NLog;

namespace MTC2SQL.Modules
{
    [XmlRoot("MySql")]
    public class MySQLConfiguration
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        [XmlIgnore]
        public const string FILENAME = "mysql.config";

        [XmlIgnore]
        public const string DEFAULT_FILENAME = "mysql.config.default";

        [XmlElement("Server")]
        public string Server { get; set; }

        [XmlElement("Port")]
        public int Port { get; set; }

        [XmlElement("User")]
        public string User { get; set; }

        [XmlElement("Password")]
        public string Password { get; set; }

        [XmlElement("Database")]
        public string Database { get; set; } 

        public MySQLConfiguration()
        {
            Port = 3306;
        }

        public static MySQLConfiguration Get(string path)
        {
            if (File.Exists(path))
            {
                try
                {
                    var serializer = new XmlSerializer(typeof(MySQLConfiguration));
                    using (var fileReader = new FileStream(path, FileMode.Open))
                    using (var xmlReader = XmlReader.Create(fileReader))
                    {
                        var config = (MySQLConfiguration)serializer.Deserialize(xmlReader);

                        return config;
                    }
                }
                catch(Exception ex)
                {
                    logger.Trace<Exception>(ex);
                }
            }
            return null;
        }


    }
}
