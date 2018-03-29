using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace MTC2SQL
{
    class DeviceStartQueue
    {
        //同步锁
        private object _lock = new object();

        //设备队列
        private List<Device> queue = new List<Device>();

        //信号和线程
        private ManualResetEvent stop;
        private Thread thread;

        //委托事件
        public delegate void DeviceStartedHandler(Device device);
        public event DeviceStartedHandler DeviceStarted;

        //延时时间
        public int Delay { get; set; }

        //计数值
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

        //构造函数
        public DeviceStartQueue()
        {
            Delay = 500;
        }

        //开始
        public void Start()
        {
            stop = new ManualResetEvent(false);

            thread = new Thread(new ThreadStart(Worker));
            thread.Start();
        }

        public void Stop()
        {
            lock (_lock)
            {
                if (stop != null)
                    stop.Set();
            }
        }

        public void Add(Device device)
        {
            if (device != null)
            {
                lock (_lock)
                {
                    queue.Add(device);
                }
            }
        }

        private void Worker()
        {
            do
            {
                List<Device> devices = null;

                lock (_lock)
                {
                    devices = queue.ToList();
                }

                if (devices != null && devices.Count > 0)
                {
                    var device = devices[0];
                    device.Start();

                    DeviceStarted.Invoke(device);

                    //Remove from queue
                    lock (_lock)
                        queue.RemoveAll(o => o.DeviceId == device.DeviceId);
                }
            } while (!stop.WaitOne(Delay, true));
        }
    }
}
