// Copyright (c) 2017 TrakHound Inc., All Rights Reserved.

// This file is subject to the terms and conditions defined in
// file 'LICENSE', which is part of this source code package.

using NLog;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Xml;
using System.Xml.Serialization;
using TrakHound.MTConnectSniffer;

namespace MTC2SQL.DeviceFinder
{
    /// <summary>
    /// Handles finding new MTConnect Devices on a network
    /// </summary>
    public class DeviceFinder
    {
        private static Logger log = LogManager.GetCurrentClassLogger();

        private Sniffer sniffer;
        private Thread thread;
        private ManualResetEvent stop;

        /// <summary>
        /// Gets or Sets the interval at which the network is scanned for new MTConnect Devices
        /// </summary>
        [XmlAttribute("scanInterval")]
        public int ScanInterval { get; set; }

        /// <summary>
        /// Range of Ports to scan
        /// </summary>
        [XmlElement("Ports")]
        public PortRange Ports { get; set; }

        /// <summary>
        /// Range of IP Addresses to scan
        /// </summary>
        [XmlElement("Addresses")]
        public AddressRange Addresses { get; set; }

        /// <summary>
        /// Raised when a new MTConnect Device is found on the network
        /// </summary>
        public event Sniffer.DeviceHandler DeviceFound;

        /// <summary>
        /// Raised when all scanning has finished
        /// </summary>
        public event Sniffer.RequestStatusHandler SearchCompleted;

        /// <summary>
        /// Start the DeviceFinder
        /// </summary>
        public void Start()
        {
            sniffer = new Sniffer();
            sniffer.RequestsCompleted += Sniffer_RequestsCompleted;
            sniffer.DeviceFound += Sniffer_DeviceFound;

            var ports = GetPortRange();
            if (ports != null) sniffer.PortRange = ports;

            var ips = GetAddressRange();
            if (ips != null) sniffer.AddressRange = ips;

            if (sniffer.PortRange != null) log.Info(string.Format("Searching for Devices : Ports : {0} to {1}", sniffer.PortRange[0], sniffer.PortRange[sniffer.PortRange.Length - 1]));
            if (sniffer.AddressRange != null) log.Info(string.Format("Searching for Devices : Addresses : {0} to {1}", sniffer.AddressRange[0], sniffer.AddressRange[sniffer.AddressRange.Length - 1]));

            stop = new ManualResetEvent(false);

            thread = new Thread(new ThreadStart(Worker));
            thread.Start();
        }

        /// <summary>
        /// Stop the DeviceFinder
        /// </summary>
        public void Stop()
        {
            if (stop != null) stop.Set();

            if (sniffer != null) sniffer.Stop();

            log.Info("Device Finder Stopped");
        }


        private void Worker()
        {
            do
            {
                log.Info("Scanning Network for MTConnect Devices..");

                // Start the MTConnectSniffer
                sniffer.Start();

                // Only repeat if ScanInterval is set
                if (ScanInterval <= 0) break;

            } while (!stop.WaitOne(ScanInterval, true));
        }

        private void Sniffer_DeviceFound(MTConnectDevice device)
        {
            DeviceFound.Invoke(device);
        }

        private void Sniffer_RequestsCompleted(long milliseconds)
        {
            SearchCompleted.Invoke(milliseconds);
        }

        private int[] GetPortRange()
        {
            if (Ports != null)
            {
                var l = new List<int>();

                // Add Allowed Ports
                if (Ports.AllowedPorts != null) l.AddRange(Ports.AllowedPorts);

                for (int i = Ports.Minimum; i <= Ports.Maximum; i++)
                {
                    bool allow = true;

                    // Check if in Denied list
                    if (Ports.DeniedPorts != null) allow = !Ports.DeniedPorts.ToList().Exists(o => o == i);

                    // Check if already added to list
                    allow = allow && !l.Exists(o => o == i);

                    if (allow) l.Add(i);
                }

                return l.ToArray();
            }

            return null;
        }

        private IPAddress[] GetAddressRange()
        {
            if (Addresses != null)
            {
                var l = new List<IPAddress>();

                // Add Allowed Ports
                if (Addresses.AllowedAddresses!= null) l.AddRange(GetIpAddressFromString(Addresses.AllowedAddresses));

                IPAddress min;
                IPAddress max;

                IPAddress.TryParse(Addresses.Minimum, out min);
                IPAddress.TryParse(Addresses.Maximum, out max);

                if (min != null && max != null)
                {
                    var minBytes = min.GetAddressBytes();
                    var maxBytes = max.GetAddressBytes();

                    var b = minBytes[3];
                    var e = maxBytes[3];

                    bool allow = true;

                    for (int i = b; i <= e; i++)
                    {
                        byte x = (byte)i;
                        var ip = new IPAddress(new byte[] { minBytes[0], minBytes[1], minBytes[2], x });

                        // Check if in Denied list
                        if (Addresses.DeniedAddresses != null) allow = !Addresses.DeniedAddresses.ToList().Exists(o => o.ToString() == ip.ToString());

                        // Check if already added to list
                        allow = allow && !l.Exists(o => o.ToString() == i.ToString());

                        if (allow) l.Add(ip);
                    }

                }

                return l.ToArray();
            }

            return null;
        }


        private static IPAddress[] GetIpAddressFromString(string[] strings)
        {
            var l = new List<IPAddress>();

            foreach (var s in strings)
            {
                IPAddress ip;
                if (IPAddress.TryParse(s, out ip)) l.Add(ip);
            }

            return l.ToArray();
        }

    }
}
