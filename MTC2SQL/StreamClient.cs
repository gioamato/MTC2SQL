using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using NLog;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using TrakHound.Api.v2.Streams;
using Json = TrakHound.Api.v2.Json;

namespace MTC2SQL
{
    /// <summary>
    /// Client class for streaming data to a TrakHound Data Server
    /// </summary>
    public class StreamClient
    {
        private static Logger log = LogManager.GetCurrentClassLogger();

        private object _lock = new object();
        private Thread thread;
        private ManualResetEvent stop;

        private TcpClient client;
        private Stream stream;
        private StreamWriter streamWriter;
        private StreamReader streamReader;
        private List<IStreamData> queue = new List<IStreamData>();
        private bool connected;

        private string _serverHostname;
        /// <summary>
        /// Data Server hostname to send sample data to
        /// </summary>
        public string ServerHostname { get { return _serverHostname; } }

        private bool _useSSL;
        public bool UseSSL { get { return _useSSL; } }

        private int _port;
        public int Port { get { return _port; } }


        public event EventHandler Connected;
        public event EventHandler Disconnected;

        public delegate void SendSuccessfulHandler(int successfulCount);
        public event SendSuccessfulHandler SendSuccessful;

        public delegate void SendFailedHandler(List<IStreamData> streamData);
        public event SendFailedHandler SendFailed;


        public int Timeout { get; set; }
        public int ReconnectionDelay { get; set; }

        public StreamClient(string serverHostname)
        {
            Init();
            _serverHostname = serverHostname;
        }

        public StreamClient(string serverHostname, int port, bool useSSL)
        {
            Init();
            _port = port;
            _serverHostname = serverHostname;
            _useSSL = useSSL;
        }

        private void Init()
        {
            ReconnectionDelay = 2000;
            Timeout = 5000;
            _port = 8472;
        }

        public void Start()
        {
            stop = new ManualResetEvent(false);

            thread = new Thread(new ThreadStart(Worker));
            thread.Start();
        }


        public void Close()
        {
            if (stop != null) stop.Set();

            if (stream != null) stream.Close();
            if (client != null) client.Close();
        }

        /// <summary>
        /// Write a single Sample to the Data Server
        /// </summary>
        public void Write(IStreamData data)
        {
            var l = new List<IStreamData>() { data };
            Write(l);
        }

        /// <summary>
        /// Write a List of Samples to the Data Server
        /// </summary>
        public void Write(List<IStreamData> data)
        {
            if (data != null && data.Count > 0)
            {
                lock (_lock)
                {
                    // Add StreamData to Queue
                    queue.AddRange(data);

                    // Send pulse to notifiy worker thread that new items are in Queue
                    Monitor.Pulse(_lock);
                }
            }
        }

        private void Worker()
        {
            do
            {
                var sendList = new List<IStreamData>();

                lock (_lock)
                {
                    // Wait till pulse is sent to signal that new items are in queue
                    if (queue.Count == 0) Monitor.Wait(_lock);

                    sendList.AddRange(queue);
                    queue.Clear();
                }

                SendData(sendList);

            } while (!stop.WaitOne(0, true));
        }

        private bool ConnectToClient()
        {
            int delay = 0;

            try
            {
                // Connect to TcpClient
                if (client == null || !client.Connected)
                {
                    // Create a new TcpClient
                    client = new TcpClient(_serverHostname, _port);
                    client.ReceiveTimeout = Timeout;
                    client.SendTimeout = Timeout;

                    // Get Stream
                    if (_useSSL)
                    {
                        // Create new SSL Stream from client's NetworkStream
                        var sslStream = new SslStream(client.GetStream(), false, new RemoteCertificateValidationCallback(ValidateServerCertificate), null);
                        sslStream.AuthenticateAsClient(_serverHostname);
                        PrintCertificateInfo(sslStream);

                        stream = Stream.Synchronized(sslStream);
                    }
                    else
                    {
                        stream = Stream.Synchronized(client.GetStream());
                    }

                    streamWriter = new StreamWriter(stream);
                    streamReader = new StreamReader(stream);

                    log.Info("Connection Established with " + ServerHostname + ":" + _port);
                    Connected.Invoke(this, new EventArgs());
                    connected = true;
                }

                return true;
            }
            catch (Exception ex)
            {
                if (client != null) client.Close();
                log.Warn("Error Connecting to " + ServerHostname + ":" + _port + ". Retrying in " + delay + "ms..");
                log.Trace(ex);
            }

            if (!connected) Disconnected.Invoke(this, new EventArgs());

            return false;
        }

        private void DisconnectClient()
        {
            if (client != null)
            {
                log.Debug("Stream Client : Disconnecting Client");

                try
                {
                    client.Close();
                    client = null;
                }
                catch (Exception ex)
                {
                    log.Trace(ex);
                }
            }

            Disconnected.Invoke(this, new EventArgs());
        }

        private void SendData(List<IStreamData> sendList)
        {
            int attempts = 0;
            var writeQueue = sendList.ToList();

            do
            {
                attempts++;

                // Connect to TCP Client
                if (ConnectToClient())
                {
                    // Write the data to the client's stream
                    writeQueue = WriteList(writeQueue);
                }
                else
                {
                    log.Debug("StreamClient : Connection Error : " + sendList.Count + " Items");
                }

                // Some items weren't sent successfully so try reconnecting to client
                if (writeQueue.Count > 0) DisconnectClient();

            } while (writeQueue.Count > 0 && attempts < 2);

            // Send Count of Successful Items
            int successfullySent = sendList.Count - writeQueue.Count;
            if (successfullySent > 0) SendSuccessful.Invoke(successfullySent);

            // Check if items were left in queue (failed to send)
            if (writeQueue.Count > 0)
            {
                SendFailed.Invoke(writeQueue);
            }
        }

        /// <summary>
        /// Write the list of IStreamData to the client connection
        /// </summary>
        /// <param name="sendList">List of IStreadData to write</param>
        /// <returns>List of IStreamData that failed to be written</returns>
        private List<IStreamData> WriteList(List<IStreamData> sendList)
        {
            int failures = 0;
            var authFailed = new List<IStreamData>();
            var failed = new List<IStreamData>();

            // Send each Item
            for (var i = 0; i < sendList.Count; i++)
            {
                var item = sendList[i];

                log.Trace("StreamClient : Send Item : " + item.StreamDataType.ToString() + " : " + item.EntryId);

                if (!authFailed.Exists(o => o.DeviceId == item.DeviceId))
                {
                    // Attempt to send data and get the Response Code back
                    var responseCode = WriteData(item);
                    if (responseCode == 401)
                    {
                        log.Info("Authentication Failed : ApiKey=" + item.ApiKey + " : DeviceId=" + item.DeviceId);
                        authFailed.Add(item);

                        // Add to failed
                        failed.Add(item);
                    }
                    else if (responseCode != 200)
                    {
                        // Add to failed
                        failed.Add(item);

                        // After 2 failed attempts. Break and try to reconnect.
                        if (failures++ >= 2) break;
                    }
                }
                else
                {
                    // Add to failed
                    failed.Add(item);
                }
            }

            return failed;
        }

        /// <summary>
        /// Write the single IStreamData object to the client stream
        /// </summary>
        /// <param name="data">The IStreamData object to write</param>
        /// <returns>The DataServer's Response Code</returns>
        private int WriteData(IStreamData data)
        {
            if (stream != null && data != null)
            {
                try
                {
                    var json = Json.Convert.ToJson(data);
                    if (!string.IsNullOrEmpty(json))
                    {
                        // Write JSON to stream
                        streamWriter.WriteLine(json);
                        streamWriter.Flush();

                        // Read Response Code
                        var response = streamReader.ReadLine();
                        if (!string.IsNullOrEmpty(response))
                        {
                            int responseCode;
                            if (int.TryParse(response, out responseCode))
                            {
                                log.Trace(response);
                                return responseCode;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    log.Debug("Error While Sending Data. Send Failed.");
                    log.Trace(ex);
                }
            }

            return -1;
        }

        private void PrintCertificateInfo(SslStream stream)
        {
            // Local
            var local = stream.LocalCertificate;
            if (local != null) PrintCertificateInfo(local, "Local");

            // Remote
            var remote = stream.RemoteCertificate;
            if (remote != null) PrintCertificateInfo(remote, "Remote");
        }

        private void PrintCertificateInfo(X509Certificate cert, string title = null)
        {
            log.Trace("SSL Certificate Information (" + title + ")");
            log.Trace("---------------------------");
            log.Trace("Subject : " + cert.Subject);
            log.Trace("Serial Number : " + cert.GetSerialNumber());
            log.Trace("Format : " + cert.GetFormat());
            log.Trace("Effective Date : " + cert.GetEffectiveDateString());
            log.Trace("Expiration Date : " + cert.GetExpirationDateString());
            log.Trace("---------------------------");
        }

        private static bool ValidateServerCertificate(
              object sender,
              X509Certificate certificate,
              X509Chain chain,
              SslPolicyErrors sslPolicyErrors)
        {
            if (sslPolicyErrors == SslPolicyErrors.None)
                return true;

            log.Error("Certificate error: {0}", sslPolicyErrors);

            // Do not allow this client to communicate with unauthenticated servers.
            return false;
        }
    }
}
