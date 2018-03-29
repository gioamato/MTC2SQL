using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Reflection;
using System.Xml.Serialization;
using TrakHound.Api.v2;
using TrakHound.Api.v2.Streams;
using TrakHound.Api.v2.Streams.Data;
using TrakHound.MTConnectSniffer;
using MTC2SQL.DataGroups;
using NLog;
using MTC2SQL.Modules;


namespace MTC2SQL
{
    /// <summary>
    /// Handles all functions for collecting data from MTConnect Agent and sending that data to Database servers
    /// </summary>
    public class DataClient
    {
        private static Logger log = LogManager.GetCurrentClassLogger();
        //同步锁
        internal static object _lock = new object();
        //发现的设备数量
        private int devicesFound = 0;

        private MTConnectDevice foundDevice;
        private DeviceStartQueue deviceStartQueue = new DeviceStartQueue();

        //DataClient的Configuration
        private Configuration _configuration;
        
        //connected
        private bool connected;

        //数据库队列
        internal static DatabaseQueue Queue = new DatabaseQueue();

        //存储的ComponentDefinitionData,DataItemDefinitionData,SampleData，防止重复发送写入数据库
        private static List<ComponentDefinitionData> storedComponents = new List<ComponentDefinitionData>();
        private static List<DataItemDefinitionData> storedDataItems = new List<DataItemDefinitionData>();
        private static List<SampleData> storedCurrentSamples = new List<SampleData>();

        [XmlArray("DataGroups")]
        [XmlArrayItem("DataGroup")]
        public List<DataGroup> DataGroups { get; set; }
        /// <summary>
        /// Gets the configuration that was used to create the DataClient.Read Only.
        /// </summary>
        private Configuration Configuration { get{return _configuration;}}
        
        /// <summary>
        /// FilteredDataItem类
        /// </summary>
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

        /// <summary>
        /// 过滤的数据项集合
        /// </summary>
        private List<FilteredDataItem> filteredDataItems = new List<FilteredDataItem>();


        public DataClient(Configuration config)
        {
            PrintHeader();
            _configuration = config;
            deviceStartQueue.DeviceStarted += DeviceStartQueue_DeviceStarted;
        }

        public void Start()
        {
            log.Info("---------------------------");

            //// Start MySQLs
            //foreach (var mySQL in _configuration.MySQLs)
            //{
            //    mySQL.Start();
            //    log.Info("DataServer Started : " + mySQL.Name + " @ " + mySQL.Server);
            //}

            //初始化DataGroups
            DataGroups = _configuration.DataGroups;
            // Start Devices
            deviceStartQueue.Start();
            foreach (var device in _configuration.Devices)
            {
                log.Info("Device Read : " + device.DeviceId + " : " + device.DeviceName + " : " + device.Address + " : " + device.Port);
                StartDevice(device);
            }

            log.Info("---------------------------");

        }

        public void Stop()
        {
            log.Info("MTC2SQL DataClient Stopping..");
            
            //stop Devices
            foreach (var device in _configuration.Devices)
            {
                device.Stop();
            }

            //stop DatabaseQueue
            Queue.Stop();

            //stop DeviceStartQueue
            if (deviceStartQueue != null)
            {
                deviceStartQueue.Stop();
            }
        }
        private void StartDevice(Device device)
        {
            device.AgentDefinitionsReceived += AgentDefinitionReceived;
            device.DeviceDefinitionsReceived += DeviceDefinitionReceived;
            device.ComponentDefinitionsReceived += ComponentDefinitionsReceived;
            device.DataItemDefinitionsReceived += DataDefinitionsReceived;
            device.SamplesReceived += SamplesReceived;
            device.AssetDefinitionsReceived += AssetDefinitionsReceived;
            device.StatusUpdated += StatusUpdated;

            //Add to Start Queue (to prevent all Devices from starting at once and using too many resources)
            deviceStartQueue.Add(device);

            //Send to DatabaseQueue
            Queue.Add(GetSendList(device.AgentConnection));
        }

        /// <summary>
        /// AgentDefinitionReceived的事件处理函数
        /// </summary>
        /// <param name="definition"></param>
        private void AgentDefinitionReceived(AgentDefinitionData definition)
        {
            //Send to DatabaseQueue
            Queue.Add(GetSendList(definition));
        }

        /// <summary>
        /// DeviceDefinitionsReceived的事件处理函数
        /// </summary>
        /// <param name="definition"></param>
        private void DeviceDefinitionReceived(DeviceDefinitionData definition)
        {
            //Send to DatabaseQueue
            Queue.Add(GetSendList(definition));
        }

        /// <summary>
        /// ComponentDefinitionsReceived的事件处理函数
        /// </summary>
        /// <param name="definitions"></param>
        private void ComponentDefinitionsReceived(List<ComponentDefinitionData> definitions)
        {
            // Send to DatabaseQueue
            Queue.Add(GetSendList(definitions.ToList<IStreamData>()));
        }

        /// <summary>
        /// DataItemDefinitionsReceived的事件处理函数
        /// </summary>
        /// <param name="definitions"></param>
        private void DataDefinitionsReceived(List<DataItemDefinitionData> definitions)
        {
            // Send to DatabaseQueue
            Queue.Add(GetSendList(definitions.ToList<IStreamData>()));
        }

        /// <summary>
        /// SamplesReceived的事件处理函数
        /// </summary>
        /// <param name="samples"></param>
        private void SamplesReceived(List<SampleData> samples)
        {
            // Send to DatabaseQueue,此处需要过滤区分archive samples和current samples
            Queue.Add(GetSendList(samples.ToList<IStreamData>()));
        }

        /// <summary>
        /// AssetDefinitionsReceived的事件处理函数
        /// </summary>
        /// <param name="definitions"></param>
        private void AssetDefinitionsReceived(List<AssetDefinitionData> definitions)
        {
            // Send to DatabaseQueue
            Queue.Add(GetSendList(definitions.ToList<IStreamData>()));
        }

        /// <summary>
        /// StatusUpdated的事件处理函数
        /// </summary>
        /// <param name="status"></param>
        private void StatusUpdated(StatusData status)
        {
            // Send to DatabaseQueue
            Queue.Add(GetSendList(status));
        }

        /// <summary>
        /// 设备开始事件处理函数
        /// </summary>
        /// <param name="device"></param>
        private void DeviceStartQueue_DeviceStarted(Device device)
        {
            log.Info("Device Started : " + device.DeviceId + " : " + device.DeviceName + " : " + device.Address + " : " + device.Port);
        }

        private static void PrintHeader()
        {
            log.Info("------------------------");
            log.Info("MTC2SQL DataClient: v"+Assembly.GetExecutingAssembly().GetName().Version.ToString());
            log.Info(@"Copyright 2017 BeiHang Inc., All Rights Reserved");
            log.Info("------------------------");
        }

        /// <summary>
        /// 向DatabaseQueue添加IStreamData
        /// </summary>
        /// <param name="data"></param>
        public List<IStreamData> GetSendList(IStreamData data)
        {
            return GetSendList(new List<IStreamData> { data });
        }

        /// <summary>
        /// 向DatabaseQueue添加IStreamData集合前
        /// </summary>
        /// <param name="data"></param>
        public List<IStreamData> GetSendList(List<IStreamData> data)
        {
            //Update the static stroed lists
            UpdateStoredLists(data);

            //Create list of data and send it to 
            var filtered = FilterStreamData(data);
            if (filtered.Count > 0)
            {
                var sendList = new List<IStreamData>();

                sendList.AddRange(filtered);
                return sendList;
            }
            return null;
        }

        /// <summary>
        /// Update stored list data
        /// </summary>
        /// <param name="data"></param>
        private void UpdateStoredLists(List<IStreamData> data)
        {
            var components = data.OfType<ComponentDefinitionData>().ToList();
            var dataItems = data.OfType<DataItemDefinitionData>().ToList();

            if (!components.IsNullOrEmpty() || !dataItems.IsNullOrEmpty())
            {
                // Add Components to stored list
                foreach (var component in components)
                {
                    lock (_lock)
                    {
                        int i = storedComponents.FindIndex(o => o.DeviceId == component.DeviceId && o.Id == component.Id);
                        if (i >= 0) storedComponents.RemoveAt(i);
                        storedComponents.Add(component);
                    }
                }

                // Add DataItems to stored list
                foreach (var dataItem in dataItems)
                {
                    lock (_lock)
                    {
                        int i = storedDataItems.FindIndex(o => o.DeviceId == dataItem.DeviceId && o.Id == dataItem.Id);
                        if (i >= 0) storedDataItems.RemoveAt(i);
                        storedDataItems.Add(dataItem);
                    }
                }

                // Update FilteredDataItems list
                foreach (var dataGroup in DataGroups)
                {
                    // Clear the previous items for this DataGroup first
                    lock (_lock) filteredDataItems.RemoveAll(o => o.DataGroupId == dataGroup.Id);

                    // Create a new list
                    var filteredItems = dataGroup.CheckFilters(storedDataItems.ToList(), storedComponents.ToList());
                    if (!filteredItems.IsNullOrEmpty())
                    {
                        foreach (var filteredItem in filteredItems)
                        {
                            lock (_lock) filteredDataItems.Add(new FilteredDataItem(dataGroup.Id, filteredItem.DeviceId, filteredItem.Id));
                        }
                    }
                }
            }

            // Add Samples to stored list
            foreach (var sample in data.OfType<SampleData>().ToList())
            {
                lock (_lock)
                {
                    int i = storedCurrentSamples.FindIndex(o => o.DeviceId == sample.DeviceId && o.Id == sample.Id);
                    if (i >= 0) storedCurrentSamples.RemoveAt(i);
                    storedCurrentSamples.Add(sample);
                }
            }
        }

        /// <summary>
        /// filter stream data 
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        private List<IStreamData> FilterStreamData(List<IStreamData> data)
        {
            var filtered = new List<IStreamData>();

            if (data != null && data.Count > 0)
            {
                // Add Statuses
                var statusData = data.OfType<StatusData>().ToList();
                if (!statusData.IsNullOrEmpty())
                {
                    var statuses = new List<StatusData>();

                    var deviceIds = statusData.Select(o => o.DeviceId).Distinct();
                    foreach (var deviceId in deviceIds)
                    {
                        var latestStatus = statusData.FindAll(o => o.DeviceId == deviceId).OrderByDescending(o => o.Timestamp).First();
                        statuses.Add(latestStatus);
                    }

                    filtered.AddRange(statuses);
                }

                // Add any Definitions
                filtered.AddRange(data.OfType<ConnectionDefinitionData>().ToList());
                filtered.AddRange(data.OfType<AgentDefinitionData>().ToList());
                filtered.AddRange(data.OfType<AssetDefinitionData>().ToList());
                filtered.AddRange(data.OfType<ComponentDefinitionData>().ToList());
                filtered.AddRange(data.OfType<DataItemDefinitionData>().ToList());
                filtered.AddRange(data.OfType<DeviceDefinitionData>().ToList());

                // Add Samples using the configured Filters
                var samples = data.OfType<SampleData>().ToList();
                if (!samples.IsNullOrEmpty())
                {
                    // Filter Samples
                    var filteredSamples = FilterSamples(samples);
                    foreach (var sample in filteredSamples)
                    {
                        filtered.Add(sample);
                        log.Debug(sample.StreamDataType.ToString() + " : " + sample.DeviceId + " : " + sample.Id + " : " + sample.Timestamp + " : " + sample.CDATA + " : " + sample.Condition);
                    }
                }
            }

            return filtered;
        }

        /// <summary>
        /// Filter Samples 
        /// </summary>
        /// <param name="samples"></param>
        /// <returns></returns>
        private List<SampleData> FilterSamples(List<SampleData> samples)
        {
            var filtered = new List<SampleData>();

            List<FilteredDataItem> dataItems = null;

            lock (_lock) dataItems = filteredDataItems.ToList();

            if (dataItems != null)
            {
                // Archive
                foreach (var dataGroup in DataGroups.FindAll(o => o.CaptureMode == CaptureMode.ARCHIVE))
                {
                    var newFiltered = FilterSamples(samples, dataGroup);
                    foreach (var sample in newFiltered)
                    {
                        if (!filtered.Exists(o => o.DeviceId == sample.DeviceId && o.Id == sample.Id))
                        {
                            sample.SetStreamDataType(StreamDataType.ARCHIVED_SAMPLE);
                            filtered.Add(sample);
                        }
                    }
                }

                // Current
                foreach (var dataGroup in DataGroups.FindAll(o => o.CaptureMode == CaptureMode.CURRENT))
                {
                    var newFiltered = FilterSamples(samples, dataGroup);
                    foreach (var sample in newFiltered)
                    {
                        if (!filtered.Exists(o => o.DeviceId == sample.DeviceId && o.Id == sample.Id))
                        {
                            sample.SetStreamDataType(StreamDataType.CURRENT_SAMPLE);
                            filtered.Add(sample);
                        }
                    }
                }
            }

            return filtered;
        }

        /// <summary>
        /// FilterSamples from DataGroup
        /// </summary>
        /// <param name="samples"></param>
        /// <param name="dataGroup"></param>
        /// <returns></returns>
        private List<SampleData> FilterSamples(List<SampleData> samples, DataGroup dataGroup)
        {
            var filtered = new List<SampleData>();

            List<FilteredDataItem> dataItems = null;

            lock (_lock) dataItems = filteredDataItems.ToList();

            if (dataItems != null)
            {
                var deviceIds = new List<string>();

                // Find all of the FilteredDataItems that match each Sample
                foreach (var sample in samples)
                {
                    bool match = dataItems.Exists(o => o.DataGroupId == dataGroup.Id && o.DeviceId == sample.DeviceId && o.DataItemId == sample.Id);
                    if (match && !filtered.Exists(o => o.DeviceId == sample.DeviceId && o.Id == sample.Id))
                    {
                        if (!deviceIds.Exists(o => o == sample.DeviceId)) deviceIds.Add(sample.DeviceId);
                        filtered.Add(sample);
                    }
                }

                if (filtered.Count > 0)
                {
                    // Include other DataGroups
                    foreach (var groupName in dataGroup.IncludedDataGroups)
                    {
                        // Find group by name
                        var includedGroup = DataGroups.Find(o => o.Name == groupName);
                        if (includedGroup != null)
                        {
                            List<SampleData> storedSamples = null;
                            lock (_lock) storedSamples = storedCurrentSamples.ToList();
                            if (!storedSamples.IsNullOrEmpty())
                            {
                                // Filter out any DeviceIds that weren't updated
                                var samplesToFilter = new List<SampleData>();
                                foreach (var deviceId in deviceIds) samplesToFilter.AddRange(storedSamples.FindAll(o => o.DeviceId == deviceId));

                                // Filter Samples using the Included Group's Filters
                                var storedFiltered = FilterSamples(samplesToFilter, includedGroup);
                                foreach (var sample in storedFiltered)
                                {
                                    // Add to list if new
                                    if (!filtered.Exists(o => o.DeviceId == sample.DeviceId && o.Id == sample.Id && o.Timestamp >= sample.Timestamp))
                                    {
                                        filtered.Add(sample);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            return filtered;
        }

    }
}
