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
    /// <summary>
    /// Handles all of the functions for sending data to a TrakHound DataServer application
    /// </summary>
    public class DataServer
    {
        /// <summary>
        /// The maximum number of items to send to a DataServer at one time
        /// </summary>
        private const int MAX_SEND_COUNT = 2000;

        /// <summary>
        /// The interval (in milliseconds) that the Buffer is read
        /// </summary>
        private const int BUFFER_READ_INTERVAL = 5000;

        /// <summary>
        /// The maximum number of items to read from the Buffer at one time
        /// </summary>
        private const int MAX_BUFFER_READ_COUNT = 5000;

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

        private string _hostname;
        /// <summary>
        /// Gets or Sets the Hostname of the DataServer
        /// </summary>
        [XmlAttribute("hostname")]
        public string Hostname
        {
            get { return _hostname; }
            set
            {
                _hostname = value;

                if (Buffer != null) _buffer._hostname = _hostname;
            }
        }

        /// <summary>
        /// Gets or Sets the port used to stream data to DataServer
        /// </summary>
        [XmlAttribute("port")]
        public int Port { get; set; }

        /// <summary>
        /// Gets or Sets whether to use SSL when connecting to the DataServer
        /// </summary>
        [XmlAttribute("useSSL")]
        public bool UseSSL { get; set; }

        /// <summary>
        /// Gets or Sets the interval at which data is sent to the DataServer
        /// </summary>
        [XmlAttribute("sendInterval")]
        public int SendInterval { get; set; }

        private Buffer _buffer;
        /// <summary>
        /// Gets or Sets the Buffer to use for buffering data between connection interruptions
        /// </summary>
        [XmlElement("Buffer")]
        public Buffer Buffer
        {
            get { return _buffer; }
            set
            {
                _buffer = value;
                if (_buffer != null) _buffer._hostname = Hostname;
            }
        }

        /// <summary>
        /// Gets or Sets the API Key used to send data to the TrakHound Cloud
        /// </summary>
        [XmlAttribute("apiKey")]
        public string ApiKey { get; set; }

        public DataServer()
        {
            SendInterval = 500;
            Port = 8472;
        }

        /// <summary>
        /// Start the DataServer streaming
        /// </summary>
        public void Start()
        {
            sendStop = new ManualResetEvent(false);

            streamClient = new StreamClient(Hostname, Port, UseSSL);
            streamClient.SendFailed += StreamClient_SendFailed;
            streamClient.SendSuccessful += StreamClient_SendSuccessful;
            streamClient.Connected += StreamClient_Connected;
            streamClient.Disconnected += StreamClient_Disconnected;
            streamClient.Start();

            // Start Buffer Thread if the Buffer is configured
            if (Buffer != null)
            {
                Buffer.Start(Hostname);

                bufferThread = new Thread(new ThreadStart(BufferWorker));
                bufferThread.Start();
            }
        }


        /// <summary>
        /// Stop the DataServer
        /// </summary>
        public void Stop()
        {
            if (sendStop != null) sendStop.Set();

            if (streamClient != null) streamClient.Close();

            if (Buffer != null) Buffer.Stop();

            log.Info("DataServer : " + Hostname + " Stopped");
        }

        /// <summary>
        /// Send a single item to the DataServer
        /// </summary>
        public void Add(IStreamData data)
        {
            Add(new List<IStreamData>() { data });
        }

        /// <summary>
        /// Send a list of items to the DataServer
        /// </summary>
        public void Add(List<IStreamData> data)
        {
            // Update the static stored lists
            UpdateStoredLists(data);

            // Create list of data and send it to DataServer
            var filtered = FilterStreamData(data);
            if (filtered.Count > 0)
            {
                var sendList = new List<IStreamData>();

                if (Buffer != null)
                {
                    // Get the max amount of items to send at one time
                    sendList.AddRange(filtered.Take(MAX_SEND_COUNT).ToList());

                    // Add the rest to the Buffer
                    if (filtered.Count > MAX_SEND_COUNT)
                    {
                        var bufferList = filtered.GetRange(MAX_SEND_COUNT, filtered.Count - MAX_SEND_COUNT);
                        bufferList = bufferList.FindAll(o => o.StreamDataType != StreamDataType.CURRENT_SAMPLE);
                        if (bufferList.Count > 0)
                        {
                            Buffer.Add(bufferList);
                            log.Info(Hostname + " : " + bufferList.Count + " Added to Buffer. Exceeded Max Send Count.");
                        }
                    }
                }
                else
                {
                    sendList.AddRange(filtered);
                    if (filtered.Count > MAX_SEND_COUNT)
                    {
                        log.Warn(Hostname + " : " + (filtered.Count - MAX_SEND_COUNT) + " Added to Buffer. Exceeded Max Send Count. Configure a Buffer to not lose data!");
                    }
                }

                // Add the Api Key
                if (!string.IsNullOrEmpty(ApiKey))
                {
                    foreach (var item in sendList) item.ApiKey = ApiKey;
                }

                // Send filtered Samples
                streamClient.Write(sendList);
            }
        }

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


        private void StreamClient_SendSuccessful(int successfulCount)
        {
            log.Info(Hostname + " : " + successfulCount + " Items Sent Successfully");
        }

        private void StreamClient_SendFailed(List<IStreamData> streamData)
        {
            if (Buffer != null)
            {
                // Don't buffer Current Samples or Statuses
                var bufferItems = streamData.FindAll(o => o.StreamDataType != StreamDataType.CURRENT_SAMPLE && o.StreamDataType != StreamDataType.STATUS);
                var failedItems = streamData.FindAll(o => o.StreamDataType == StreamDataType.CURRENT_SAMPLE || o.StreamDataType == StreamDataType.STATUS);
                if (bufferItems.Count > 0)
                {
                    Buffer.Add(bufferItems);
                    log.Warn(string.Format("{0} : {1} Falied to Send. {2} Added to Buffer.", Hostname, bufferItems.Count + failedItems.Count, bufferItems.Count));
                }
                else if (failedItems.Count > 0)
                {
                    log.Warn(Hostname + " : " + failedItems.Count + " Failed to Send.");
                }
            }
            else
            {
                log.Warn(Hostname + " : " + streamData.Count + " Failed to Send.");
            }
        }

        private void StreamClient_Connected(object sender, System.EventArgs e)
        {
            log.Warn("Connected to : " + Hostname);
            connected = true;
        }

        private void StreamClient_Disconnected(object sender, System.EventArgs e)
        {
            log.Warn("Disconnected to : " + Hostname);
            connected = false;
        }


        private void BufferWorker()
        {
            do
            {
                if (connected)
                {
                    int maxRecords = MAX_BUFFER_READ_COUNT;

                    var sendList = new List<IStreamData>();

                    sendList.AddRange(Buffer.Read<ConnectionDefinitionData>(maxRecords - sendList.Count).ToList<IStreamData>());
                    sendList.AddRange(Buffer.Read<AgentDefinitionData>(maxRecords - sendList.Count).ToList<IStreamData>());
                    sendList.AddRange(Buffer.Read<ComponentDefinitionData>(maxRecords - sendList.Count).ToList<IStreamData>());
                    sendList.AddRange(Buffer.Read<DataItemDefinitionData>(maxRecords - sendList.Count).ToList<IStreamData>());
                    sendList.AddRange(Buffer.Read<DeviceDefinitionData>(maxRecords - sendList.Count).ToList<IStreamData>());
                    sendList.AddRange(Buffer.Read<SampleData>(maxRecords - sendList.Count).ToList<IStreamData>());

                    if (sendList.Count > 0)
                    {
                        var ids = sendList.Select(o => o.EntryId).ToList();

                        log.Info(Hostname + " : " + sendList.Count + " Samples Read from Buffer");

                        // Send Samples to Data Server
                        streamClient.Write(sendList);
                        Buffer.Remove(ids);
                    }
                }
            } while (!sendStop.WaitOne(BUFFER_READ_INTERVAL, true));
        }
    }
}
