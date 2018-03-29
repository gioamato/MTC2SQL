using System;
using System.Collections.Generic;
using System.Linq;
using NLog;
using System.Threading;
using TrakHound.Api.v2;
using TrakHound.Api.v2.Streams;
using TrakHound.Api.v2.Streams.Data;
using TrakHound.Api.v2.Data;
using MTC2SQL.Modules;
using System.IO;


namespace MTC2SQL
{
    /// <summary>
    /// 处理把数据写入配置好的SQL数据库的方法
    /// </summary>
    public class DatabaseQueue
    {
        private static Logger log = LogManager.GetCurrentClassLogger();

        private object _lock = new object();

        private List<IStreamData> queue = new List<IStreamData>();   

        private ManualResetEvent stop;
        private Thread thread;
        

        /// <summary>
        /// 获取和设置队列被读取和查询执行的周期
        /// </summary>
        public int Interval { get; set; }

        /// <summary>
        /// 获取和设置队列被读取和查询执行失败后重新读取的周期
        /// </summary>
        public int RetryInterval { get; set; }

        /// <summary>
        /// 获取和设置每次从队列读取查询的最大数量
        /// </summary>
        public int MaxSamplePerQuery { get; set; }

        /// <summary>
        /// 获取底层队列的数量
        /// </summary>
        public int Count
        {
            get
            {
                lock (_lock)
                {
                    if (queue != null)
                        return queue.Count;
                }
                return -1;
            }
        }

        public DatabaseQueue()
        {
            Interval = 200;
            RetryInterval = 5000;
            MaxSamplePerQuery = 2000;

            //开始写入数据库
            Start();
        }

        /// <summary>
        /// 开始工作写入数据库
        /// </summary>
        public void Start()
        {
            stop = new ManualResetEvent(false);

            //初始化数据库的配置
            string databaseConfigurationPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, MySQLConfiguration.FILENAME);
            string databaseConfigurationDefaultPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, MySQLConfiguration.DEFAULT_FILENAME);
            if (!File.Exists(databaseConfigurationPath) && File.Exists(databaseConfigurationDefaultPath))
            {
                File.Copy(databaseConfigurationDefaultPath, databaseConfigurationPath);
            }
            if (MySQLDatabase.Initialize(databaseConfigurationPath))
            {
                log.Info("initialize Database successful");
            }

            //新建一个线程执行写入数据库的操作
            thread = new Thread(new ThreadStart(Worker));
            thread.Start();
        }

        /// <summary>
        /// 停止写入数据库的线程
        /// </summary>
        public void Stop()
        {
            if (stop!= null)
                stop.Set();
        }

        /// <summary>
        /// 向队列中添加流数据
        /// </summary>
        /// <param name="data"></param>
        public void Add(IStreamData data)
        {
            if (data != null)
            {
                lock (_lock)
                    queue.Add(data);
            }
        }

        /// <summary>
        /// 向队列中添加流数据
        /// </summary>
        /// <param name="data"></param>
        public void Add(List<IStreamData> data)
        {
            if (data != null && data.Count > 0)
            {
                foreach (var item in data)
                {
                    lock (_lock)
                        queue.Add(item);
                }
            }
        }

        /// <summary>
        /// 写入数据库的具体方法
        /// </summary>
        private void Worker()
        {
            int interval = Interval;

            do
            {
                List<IStreamData> streamData = null;

                //从队列获取流数据接口集合
                lock (_lock) streamData = queue.Take(MaxSamplePerQuery).ToList();

                if (streamData != null && streamData.Count > 0)
                {
                    //发送的数据的Id集合
                    var sentIds = new List<string>();
                    //是否发送成功
                    bool success = true;

                    //把ConnectionDefintions写入数据库
                    //根据指定类型ConnectionDefinitionData从流数据中筛选该类型的元素集合
                    var connections = streamData.OfType<ConnectionDefinitionData>().ToList();
                    //如果success为真且ConnectionDefinitionData类型的元素集合connections不为空MySQLDatabase.Write(connections)
                    if (success && !connections.IsNullOrEmpty())
                    {
                        if (MySQLDatabase.Write(connections))
                        {
                            sentIds.AddRange(GetSentDataIds(connections.ToList<IStreamData>(), "Connections"));
                        }
                        else
                        {
                            log.Info(string.Format("Error writing {0}{1}", connections.Count, "Connections"));
                            success = false;
                        }
                    }

                    //把AgentDefinitions写入数据库
                    var agents = streamData.OfType<AgentDefinitionData>().ToList();
                    if (success && !agents.IsNullOrEmpty())
                    {
                        if (MySQLDatabase.Write(agents))
                        {
                            sentIds.AddRange(GetSentDataIds(agents.ToList<IStreamData>(), "Agents"));
                        }
                        else
                        {
                            log.Info(string.Format("Error writing {0}{1}", agents.Count, "Agents"));
                            success = false;
                        }
                    }

                    //把AssetDefinitions写入数据库
                    var assets = streamData.OfType<AssetDefinitionData>().ToList();
                    if (success && !assets.IsNullOrEmpty())
                    {
                        if (MySQLDatabase.Write(assets))
                        {
                            sentIds.AddRange(GetSentDataIds(assets.ToList<IStreamData>(), "Assets"));
                        }
                        else
                        {
                            log.Info(string.Format("Error writing {0}{1}", assets.Count, "Assets"));
                            success = false;
                        }
                    }

                    //把ComponentDefinitions写入数据库, test results=success
                    var components = streamData.OfType<ComponentDefinitionData>().ToList();
                    if (success && !components.IsNullOrEmpty())
                    {
                        if (MySQLDatabase.Write(components))
                        {
                            sentIds.AddRange(GetSentDataIds(components.ToList<IStreamData>(), "Components"));
                        }
                        else
                        {
                            log.Info(string.Format("Error writing {0}{1}", components.Count, "Components"));
                            success = false;
                        }
                    }

                    // 把DataItemDefinitionData类型的数据写入数据库,test results=success
                    var dataItems = streamData.OfType<DataItemDefinitionData>().ToList();
                    if (success && !dataItems.IsNullOrEmpty())
                    {
                        if (MySQLDatabase.Write(dataItems)) sentIds.AddRange(GetSentDataIds(dataItems.ToList<IStreamData>(), "DataItems"));
                        else
                        {
                            log.Info(string.Format("Error writing {0} {1}", dataItems.Count, "DataItems"));
                            success = false;
                        }
                    }

                    // 把DeviceDefinitions写入数据库,test results=success
                    var devices = streamData.OfType<DeviceDefinitionData>().ToList();
                    if (success && !devices.IsNullOrEmpty())
                    {
                        if (MySQLDatabase.Write(devices)) sentIds.AddRange(GetSentDataIds(devices.ToList<IStreamData>(), "Devices"));
                        else
                        {
                            
                            log.Info(string.Format("Error writing {0} {1}", devices.Count, "Devices"));
                            success = false;
                        }
                    }

                    //把加工的不合格零件RejectedPart写入数据库
                    
                    // 把采样类型 Samples的数据写入数据库
                    var samples = streamData.OfType<SampleData>().ToList();
                    if (success && !samples.IsNullOrEmpty())
                    {
                        var newSamples = new List<SampleData>();

                        // Only add the newest CURRENT_SAMPLE data,test result=success
                        var deviceIds = samples.FindAll(o => o.StreamDataType == StreamDataType.CURRENT_SAMPLE).Select(o => o.DeviceId).Distinct();
                        foreach (var deviceId in deviceIds)
                        {
                            var dataItemIds = samples.FindAll(o => o.StreamDataType == StreamDataType.CURRENT_SAMPLE && o.DeviceId == deviceId).Select(o => o.Id).Distinct();
                            foreach (var dataItemId in dataItemIds)
                            {
                                var sample = samples.FindAll(o => o.DeviceId == deviceId && o.Id == dataItemId).OrderByDescending(o => o.Timestamp).First();
                                newSamples.Add(sample);
                            }

                            // Clear the unused CURRENT_SAMPLE data from the queue
                            foreach (var sample in samples)
                            {
                                if (!newSamples.Exists(o => o.EntryId == sample.EntryId))
                                {
                                    lock (_lock) queue.RemoveAll(o => o.EntryId == sample.EntryId);
                                }
                            }
                        }

                        // Add any ARCHIVE_SAMPLE data,test results=success
                        newSamples.AddRange(samples.FindAll(o => o.StreamDataType == StreamDataType.ARCHIVED_SAMPLE));

                        if (MySQLDatabase.Write(newSamples)) sentIds.AddRange(GetSentDataIds(newSamples.ToList<IStreamData>(), "Samples"));
                        else
                        {
                            log.Info(string.Format("Error writing {0} {1}", newSamples.Count, "Samples"));
                            success = false;
                        }
                    }

                    // Write Statuses to Database,test results=false
                    var statuses = streamData.OfType<StatusData>().ToList();
                    if (success && statuses != null && statuses.Count > 0)
                    {
                        var newStatuses = new List<StatusData>();

                        var deviceIds = statuses.Select(o => o.DeviceId).Distinct();
                        foreach (var deviceId in deviceIds)
                        {
                            var status = statuses.FindAll(o => o.DeviceId == deviceId).OrderByDescending(o => o.Timestamp).First();
                            newStatuses.Add(status);
                        }

                        if (MySQLDatabase.Write(newStatuses)) sentIds.AddRange(GetSentDataIds(newStatuses.ToList<IStreamData>(), "Status"));
                        else
                        {
                            log.Info(string.Format("Error writing {0} {1}", newStatuses.Count, "Statuses"));
                            success = false;
                        }
                    }


                    if (sentIds.Count > 0)
                    {
                        // Remove written samples
                        foreach (var id in sentIds)
                        {
                            lock (_lock) queue.RemoveAll(o => o.EntryId == id);
                        }
                    }

                    if (!success)
                    {
                        interval = RetryInterval;
                        log.Info("Queue Failed to Send Successfully : Retrying in " + RetryInterval + "ms");
                    }
                    else interval = Interval;

                }
            }while (!stop.WaitOne(interval, true));
        }       

        private List<string> GetSentDataIds(List<IStreamData> sentData, string tag)
        {
            var sent = sentData.Select(o => o.EntryId).ToList();
            if (sent.Count > 0)
            {
                log.Info(sent.Count + " " + tag + " Written to Database successfully");
                return sent;
            }
            return new List<string>();
        }
    }
}
