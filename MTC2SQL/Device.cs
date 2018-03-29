using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MTConnect;
using MTConnect.Clients;
using NLog;
using System.Timers;
using System.Xml.Serialization;
using TrakHound.Api.v2;
using TrakHound.Api.v2.Data;
using TrakHound.Api.v2.Streams.Data;
using MTConnectDevices = MTConnect.MTConnectDevices;
using MTConnectStreams = MTConnect.MTConnectStreams;

namespace MTC2SQL
{
    /// <summary>
    /// 处理 MTConnect Agent connection data streams
    /// </summary>
    public class Device
    {
        private const int STATUS_UPDATE_INTERVAL = 60000; // 1 Minute

        private static Logger log = LogManager.GetCurrentClassLogger();

        private object _lock = new object();
        private string agentUrl;

        private Timer statusUpdateTimer;
        private string availabilityId;
        private bool prev_available;
        private bool prev_connected;
        private bool first = true;


        [XmlIgnore]
        public ConnectionDefinitionData AgentConnection;

        private MTConnectClient _agentClient;
        /// <summary>
        /// Gets the underlying MTConnectClient object. Read Only.
        /// </summary>
        [XmlIgnore]
        public MTConnectClient AgentClient { get { return _agentClient; } }

        private string _deviceId;
        /// <summary>
        /// Gets the Device ID. Read Only.
        /// </summary>
        [XmlAttribute("deviceId")]
        public string DeviceId
        {
            get { return _deviceId; }
            set
            {
                if (_deviceId != null) throw new InvalidOperationException("Cannot set value. DeviceId is ReadOnly!");
                _deviceId = value;
                if (AgentConnection == null) AgentConnection = new ConnectionDefinitionData();
                AgentConnection.DeviceId = _deviceId;
            }
        }

        private string _address;
        /// <summary>
        /// Gets the Address for the MTConnect Agent. Read Only.
        /// </summary>
        [XmlAttribute("address")]
        public string Address
        {
            get { return _address; }
            set
            {
                if (_address != null) throw new InvalidOperationException("Cannot set value. Address is ReadOnly!");
                _address = value;
                if (AgentConnection == null) AgentConnection = new ConnectionDefinitionData();
                AgentConnection.Address = _address;
            }
        }

        private string _physicalAddress;
        /// <summary>
        /// Gets the Physical Address for the MTConnect Agent. Read Only.
        /// </summary>
        [XmlAttribute("physicalAddress")]
        public string PhysicalAddress
        {
            get { return _physicalAddress; }
            set
            {
                if (_physicalAddress != null) throw new InvalidOperationException("Cannot set value. PhysicalAddress is ReadOnly!");
                _physicalAddress = value;
                if (AgentConnection == null) AgentConnection = new ConnectionDefinitionData();
                AgentConnection.PhysicalAddress = _physicalAddress;
            }
        }

        private int _port = -1;
        /// <summary>
        /// Gets the Port for the MTConnect Agent. Read Only.
        /// </summary>
        [XmlAttribute("port")]
        public int Port
        {
            get { return _port; }
            set
            {
                if (_port >= 0) throw new InvalidOperationException("Cannot set value. Port is ReadOnly!");
                _port = value;
                if (AgentConnection == null) AgentConnection = new ConnectionDefinitionData();
                AgentConnection.Port = _port;
            }
        }

        private string _deviceName;
        /// <summary>
        /// Gets the Name of the MTConnect Device. Read Only.
        /// </summary>
        [XmlAttribute("deviceName")]
        public string DeviceName
        {
            get { return _deviceName; }
            set
            {
                if (_deviceName != null) throw new InvalidOperationException("Cannot set value. DeviceName is ReadOnly!");
                _deviceName = value;
            }
        }

        private int _interval = -1;
        /// <summary>
        /// Gets the Name of the MTConnect Device. Read Only.
        /// </summary>
        [XmlAttribute("interval")]
        public int Interval
        {
            get
            {
                if (_interval < 0) _interval = 500;
                return _interval;
            }
            set
            {
                if (_interval >= 0) throw new InvalidOperationException("Cannot set value. Interval is ReadOnly!");
                if (value < 0) throw new ArgumentOutOfRangeException("Interval", "Interval must be greater than zero!");
                _interval = value;
            }
        }

        /// <summary>
        /// Event raised when a new AgentDefinition is read.
        /// </summary>
        public event AgentDefinitionsHandler AgentDefinitionsReceived;

        /// <summary>
        /// Event raised when a new AssetDefinition is read.
        /// </summary>
        public event AssetDefinitionsHandler AssetDefinitionsReceived;

        /// <summary>
        /// Event raised when a new DeviceDefinition is read.
        /// </summary>
        public event DeviceDefinitionsHandler DeviceDefinitionsReceived;

        /// <summary>
        /// Event raised when new ComponentDefinitions are read.
        /// </summary>
        public event ComponentDefinitionsHandler ComponentDefinitionsReceived;

        /// <summary>
        /// Event raised when new DataItemDefinitions are read.
        /// </summary>
        public event DataItemDefinitionsHandler DataItemDefinitionsReceived;

        /// <summary>
        /// Event raised when new Samples are read.
        /// </summary>
        public event SamplesHandler SamplesReceived;

        /// <summary>
        /// Event raised when the Status is updated.
        /// </summary>
        public event StatusHandler StatusUpdated;


        public Device() { }

        public Device(string deviceId, Connection connection, string deviceName)
        {
            Init(deviceId, connection, deviceName, 100);
        }

        public Device(string deviceId, Connection connection, string deviceName, int interval)
        {
            Init(deviceId, connection, deviceName, interval);
        }

        private void Init(string deviceId, Connection connection, string deviceName, int interval)
        {
            if (connection != null)
            {
                AgentConnection = new ConnectionDefinitionData();
                AgentConnection.Address = connection.Address;
                AgentConnection.Port = connection.Port;
                AgentConnection.PhysicalAddress = connection.PhysicalAddress;
                AgentConnection.DeviceId = deviceId;

                _deviceId = deviceId;
                _address = connection.Address;
                _physicalAddress = connection.PhysicalAddress;
                _port = connection.Port;
                _deviceName = deviceName;
                _interval = interval;
            }
        }

        /// <summary>
        /// Start the Device and begin reading the MTConnect Data.
        /// </summary>
        public void Start()
        {
            // Initialize Status
            UpdateStatus(false, false);

            // Start Status Update Timer
            statusUpdateTimer = new Timer();
            statusUpdateTimer.Interval = STATUS_UPDATE_INTERVAL;
            statusUpdateTimer.Elapsed += StatusUpdateTimer_Elapsed;
            statusUpdateTimer.Start();

            // Start MTConnect Agent Client
            StartAgentClient();
        }

        /// <summary>
        /// Stop the Device
        /// </summary>
        public void Stop()
        {
            if (statusUpdateTimer != null) statusUpdateTimer.Stop();

            if (_agentClient != null) _agentClient.Stop();
        }

        private void StartAgentClient()
        {
            if (!string.IsNullOrEmpty(Address))
            {
                // Normalize Properties
                _address = _address.Replace("http://", "");
                if (_port < 0) _port = 5000;
                if (_interval < 0) _interval = 500;

                // Create the MTConnect Agent Base URL
                agentUrl = string.Format("http://{0}:{1}", _address, _port);

                // Create a new MTConnectClient using the baseUrl
                _agentClient = new MTConnectClient(agentUrl, _deviceName);
                _agentClient.Interval = _interval;

                // Subscribe to the Event handlers to receive status events
                _agentClient.Started += _agentClient_Started;
                _agentClient.Stopped += _agentClient_Stopped;

                // Subscribe to the Event handlers to receive the MTConnect documents
                _agentClient.ProbeReceived += DevicesSuccessful;
                _agentClient.CurrentReceived += StreamsSuccessful;
                _agentClient.SampleReceived += StreamsSuccessful;
                _agentClient.AssetsReceived += AssetsReceived;
                _agentClient.ConnectionError += _agentClient_ConnectionError;

                // Start the MTConnectClient
                _agentClient.Start();
            }
            else
            {
                log.Warn("MTConnect Address Invalid : " + _deviceId + " : " + _address);
            }
        }

        private void _agentClient_ConnectionError(Exception ex)
        {
            log.Info("Error Connecting to MTConnect Agent @ " + _agentClient.BaseUrl);
            log.Trace(ex);

            UpdateConnectedStatus(false);
        }

        private void _agentClient_Started()
        {
            log.Info("MTConnect Client Started : " + agentUrl + "/" + _deviceName);
        }

        private void _agentClient_Stopped()
        {
            log.Info("MTConnect Client Stopped : " + agentUrl + "/" + _deviceName);
        }

        private void StatusUpdateTimer_Elapsed(object sender, ElapsedEventArgs e)
        {
            lock (_lock) UpdateStatus(prev_connected, prev_available);
        }

        private void DevicesSuccessful(MTConnectDevices.Document document)
        {
            log.Trace("MTConnect Devices Document Received @ " + DateTime.Now.ToString("o"));

            UpdateConnectedStatus(true);

            if (document.Header != null && document.Devices != null && document.Devices.Count == 1)
            {
                long agentInstanceId = document.Header.InstanceId;
                DateTime timestamp = document.Header.CreationTime;

                // Send Agent Definition 发送Agent的定义
                AgentDefinitionsReceived.Invoke(Create(_deviceId, document.Header));

                var dataItemDefinitions = new List<DataItemDefinitionData>();
                var componentDefinitions = new List<ComponentDefinitionData>();
                var dataFilters = new List<DataFilter>();

                var device = document.Devices[0];

                // Send Device Definition 发送Device的定义
                DeviceDefinitionsReceived.Invoke(Create(_deviceId, agentInstanceId, device));

                // Add DataItems 向数据项定义数据集合dataItemDefinitions中添加设备的数据项定义数据的数据项
                foreach (var item in device.DataItems)
                {
                    dataItemDefinitions.Add(Create(_deviceId, agentInstanceId, device.Id, item));
                }

                // Add Component Definitions 向组件定义数据集合componentDefinitions中添加设备的组件定义数据的数据项
                componentDefinitions.AddRange(GetComponents(_deviceId, agentInstanceId, device.Id, device.Components));

                // Add Component DataItems 添加组件的数据项
                foreach (var component in device.GetComponents())
                {
                    foreach (var dataItem in component.DataItems)
                    {
                        dataItemDefinitions.Add(Create(_deviceId, agentInstanceId, component.Id, dataItem));
                    }
                }

                // Get the Availability DataItem获取可用的DataItem
                var avail = device.DataItems.Find(o=>o.Type=="AVAILABILITY");
                if (avail != null) availabilityId = avail.Id;

                // Send ContainerDefinition Objects 发送定义的组件对象
                if (componentDefinitions.Count > 0) ComponentDefinitionsReceived.Invoke(componentDefinitions);

                // Send DataItemDefinition Objects 发送数据项定义对象
                if (dataItemDefinitions.Count > 0) DataItemDefinitionsReceived.Invoke(dataItemDefinitions);
            }
        }

        private static List<ComponentDefinitionData> GetComponents(string deviceId, long agentInstanceId, string parentId, MTConnectDevices.ComponentCollection components)
        {
            var l = new List<ComponentDefinitionData>();

            foreach (var component in components.Components)
            {
                l.Add(Create(deviceId, agentInstanceId, parentId, component));
                l.AddRange(GetComponents(deviceId, agentInstanceId, component.Id, component.SubComponents));
            }

            return l;
        }

        private void StreamsSuccessful(MTConnectStreams.Document document)
        {
            log.Trace("MTConnect Streams Document Received @ " + DateTime.Now.ToString("o"));

            UpdateConnectedStatus(true);

            if (!document.DeviceStreams.IsNullOrEmpty())
            {
                var samples = new List<SampleData>();

                var deviceStream = document.DeviceStreams[0];

                foreach (var dataItem in deviceStream.DataItems)
                {
                    samples.Add(Create(_deviceId, document.Header.InstanceId, dataItem));
                }

                // Get Availability
                if (!string.IsNullOrEmpty(availabilityId))
                {
                    var avail = deviceStream.DataItems.Find(o => o.DataItemId == availabilityId);
                    if (avail != null) UpdateAvailableStatus(avail.CDATA == "AVAILABLE");
                }

                if (samples.Count > 0) SamplesReceived.Invoke(samples);
            }
        }

        private void AssetsReceived(MTConnect.MTConnectAssets.Document document)
        {
            log.Trace("MTConnect Assets Document Received @ " + DateTime.Now.ToString("o"));

            UpdateConnectedStatus(true);

            if (document.Assets != null)
            {
                var assets = new List<AssetDefinitionData>();

                foreach (var asset in document.Assets.Assets)
                {
                    assets.Add(Create(_deviceId, document.Header.InstanceId, asset));
                }

                if (assets.Count > 0) AssetDefinitionsReceived.Invoke(assets);
            }
        }

        private static AgentDefinitionData Create(string deviceId, MTConnect.Headers.MTConnectDevicesHeader header)
        {
            var obj = new AgentDefinitionData();

            // TrakHound Properties
            obj.DeviceId = deviceId;
            obj.Timestamp = header.CreationTime;

            // MTConnect Properties
            obj.InstanceId = header.InstanceId;
            obj.Sender = header.Sender;
            obj.Version = header.Version;
            obj.BufferSize = header.BufferSize;
            obj.TestIndicator = header.TestIndicator;

            return obj;
        }

        private static AssetDefinitionData Create(string deviceId, long agentInstanceId, MTConnect.MTConnectAssets.Asset asset)
        {
            var obj = new AssetDefinitionData();

            // TrakHound Properties
            obj.DeviceId = deviceId;
            obj.AgentInstanceId = agentInstanceId;
            obj.Id = asset.AssetId;
            obj.Timestamp = asset.Timestamp;
            obj.Type = asset.Type;
            obj.Xml = asset.Xml;

            return obj;
        }

        private static DeviceDefinitionData Create(string deviceId, long agentInstanceId, MTConnectDevices.Device device)
        {
            var obj = new DeviceDefinitionData();

            //TrakHound properties
            obj.DeviceId = deviceId;

            // MTConnect Properties
            obj.AgentInstanceId = agentInstanceId;
            obj.Id = device.Id;
            obj.Uuid = device.Uuid;
            obj.Name = device.Name;
            obj.NativeName = device.NativeName;
            obj.SampleInterval = device.SampleInterval;
            obj.SampleRate = device.SampleRate;
            obj.Iso841Class = device.Iso841Class;
            if (device.Description != null)
            {
                obj.Manufacturer = device.Description.Manufacturer;
                obj.Model = device.Description.Model;
                obj.SerialNumber = device.Description.SerialNumber;
                obj.Station = device.Description.Station;
                obj.Description = device.Description.CDATA;
            }

            return obj;
        }

        private static ComponentDefinitionData Create(string deviceId, long agentInstanceId, string parentId, MTConnectDevices.Component component)
        {
            var obj = new ComponentDefinitionData();

            // TrakHound Properties
            obj.DeviceId = deviceId;
            obj.ParentId = parentId;

            // MTConnect Properties
            obj.AgentInstanceId = agentInstanceId;
            obj.Type = component.Type;
            obj.Id = component.Id;
            obj.Uuid = component.Uuid;
            obj.Name = component.Name;
            obj.NativeName = component.NativeName;
            obj.SampleInterval = component.SampleInterval;
            obj.SampleRate = component.SampleRate;

            return obj;
        }

        private static DataItemDefinitionData Create(string deviceId, long agentInstanceId, string parentId, MTConnectDevices.DataItem dataItem)
        {
            var obj = new DataItemDefinitionData();

            // TrakHound Properties
            obj.DeviceId = deviceId;
            obj.ParentId = parentId;

            // MTConnect Properties
            obj.AgentInstanceId = agentInstanceId;
            obj.Id = dataItem.Id;
            obj.Name = dataItem.Name;
            obj.Category = dataItem.Category.ToString();
            obj.Type = dataItem.Type;
            obj.SubType = dataItem.SubType;
            obj.Statistic = dataItem.Statistic;
            obj.Units = dataItem.Units;
            obj.NativeUnits = dataItem.NativeUnits;
            obj.NativeScale = dataItem.NativeScale;
            obj.CoordinateSystem = dataItem.CoordinateSystem;
            obj.SampleRate = dataItem.SampleRate;
            obj.Representation = dataItem.Representation;
            obj.SignificantDigits = dataItem.SignificantDigits;

            return obj;
        }

        private static SampleData Create(string deviceId, long agentInstanceId, MTConnectStreams.DataItem dataItem)
        {
            var obj = new SampleData();

            obj.DeviceId = deviceId;

            obj.Id = dataItem.DataItemId;
            obj.AgentInstanceId = agentInstanceId;
            obj.Sequence = dataItem.Sequence;
            obj.Timestamp = dataItem.Timestamp;
            obj.CDATA = dataItem.CDATA;
            if (dataItem.Category == DataItemCategory.CONDITION) obj.Condition = ((MTConnectStreams.Condition)dataItem).ConditionValue.ToString();

            return obj;
        }

        private static StatusData Create(string deviceId, bool connected, bool available)
        {
            var obj = new StatusData();
            obj.DeviceId = deviceId;
            obj.Timestamp = DateTime.UtcNow;
            obj.Connected = connected;
            obj.Available = available;

            return obj;
        }

        private void UpdateAvailableStatus(bool available)
        {
            bool changed = false;
            bool connected = false;

            lock (_lock)
            {
                connected = prev_connected;
                changed = available != prev_available || connected != prev_connected;
                prev_available = available;
            }

            if (changed || first) UpdateStatus(connected, available);

            first = false;
        }

        private void UpdateConnectedStatus(bool connected)
        {
            bool changed = false;
            bool available = false;

            lock (_lock)
            {
                available = prev_available;
                changed = available != prev_available || connected != prev_connected;
                prev_connected = connected;
            }

            if (changed || first) UpdateStatus(connected, available);

            first = false;
        }

        private void UpdateStatus(bool connected, bool available)
        {
            log.Info("Status Updated : " + _deviceId + " : Connected=" + connected + " : Available=" + available);
            StatusUpdated.Invoke(Create(_deviceId, connected, available));
        }
    }
}
