using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NLog;
using System.Threading;
using System.Xml.Serialization;
using TrakHound.Api.v2;
using TrakHound.Api.v2.Streams;
using TrakHound.Api.v2.Streams.Data;
using MTC2SQL.DataGroups;


namespace MTC2SQL
{
    public class MySQL
    {
        /// <summary>
        /// Handles all of the functions for sending data to MySQL
        /// </summary>
        private static Logger log = LogManager.GetCurrentClassLogger();
        private static List<ComponentDefinitionData> storedComponents = new List<ComponentDefinitionData>();
        private static List<DataItemDefinitionData> storedDataItems = new List<DataItemDefinitionData>();
        private static List<SampleData> storedCurrentSamples = new List<SampleData>();
        
        private class FilteredDataItem
        {
            private string _dataGroupId;
            public string DataGroupId { get { return _dataGroupId; } }

            private string _deviceId;
            public string DeviceId { get { return _deviceId; } }

            private string _dataItemId;
            public string DataItemId { get { return _dataItemId; } }

            public FilteredDataItem(string dataGroupId, string deviceId, string dataItemId)
            {
                _dataGroupId = dataGroupId;
                _deviceId = deviceId;
                _dataItemId = dataItemId;
            }
        }

        private List<FilteredDataItem> filteredDataItems = new List<FilteredDataItem>();
        private object _lock = new object();
        private ManualResetEvent sendStop;
        private Thread bufferThread;
        private StreamClient streamClient;
        private bool connected;

        /// <summary>
        /// List of Configured DataGroups for processing data
        /// </summary>
        [XmlArray("DataGroups")]
        [XmlArrayItem("DataGroup")]
        public List<DataGroup> DataGroups { get; set; }

        /// <summary>
        /// Gets or Sets the name of the DataServer
        /// </summary>
        [XmlAttribute("name")]
        public string Name { get; set; }

        
        /// <summary>
        /// Gets or Sets the server of the MySQL,数据库服务器的server
        /// </summary>
        private string _server;
        [XmlAttribute("server")]
        public string Server
        {
            get { return _server; }
            set
            {
                _server = value;
            }
        }

        //用户名
        private string _user;
        [XmlAttribute("user")]
        public string User
        {
            get { return _user; }
            set
            {
                _user = value;
            }
        }


        //数据库密码
        private string _password;
        [XmlAttribute("password")]
        public string Password
        {
            get { return _password; }
            set
            {
                _password = value;
            }
        }

        //数据库端口
        private int _port;
        [XmlAttribute("port")]
        public int Port { get; set; }

        //数据库名称
        private string _database;
        [XmlAttribute("database")]
        public string Database
        {
            get { return _database; }
            set
            {
                _database = value;
            }
        }
        
        public MySQL()
        {
            Port = 3306;
            Server = "localhost";
        }

        ///Start the MySQL streaming
        public void Start()
        {
            sendStop = new ManualResetEvent(false);
        }

        public void Stop()
        {
            if (sendStop != null) sendStop.Set();
            if (streamClient != null) streamClient.Close();
            
        }
    }
}
