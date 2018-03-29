using System;
using System.Windows;
using System.Windows.Controls;

namespace MTC2SQL
{
    /// <summary>
    /// DataServerItem.xaml 的交互逻辑
    /// </summary>
    public partial class DataServerItem : UserControl
    {
        public DataServer DataServer { get; set; }

        public string Id { set; get; }

        public string Hostname
        {
            get { return (string)GetValue(HostnameProperty); }
            set { SetValue(HostnameProperty, value); }
        }

        public static readonly DependencyProperty HostnameProperty =
            DependencyProperty.Register("Hostname", typeof(string), typeof(DataServerItem), new PropertyMetadata(null, new PropertyChangedCallback(DataServerItem_PropertyChanged)));

        public int Port
        {
            get { return (int)GetValue(PortProperty); }
            set { SetValue(PortProperty, value); }
        }

        public static readonly DependencyProperty PortProperty =
            DependencyProperty.Register("Port", typeof(int), typeof(DataServerItem), new PropertyMetadata(8472, new PropertyChangedCallback(DataServerItem_PropertyChanged)));

        public bool UseSSL
        {
            get { return (bool)GetValue(UseSSLProperty); }
            set { SetValue(UseSSLProperty, value); } 
        }

        public static readonly DependencyProperty UseSSLProperty =
            DependencyProperty.Register("UseSSL", typeof(bool), typeof(DataServerItem), new PropertyMetadata(false, new PropertyChangedCallback(DataServerItem_PropertyChanged)));

        public int SendInterval
        {
            get { return (int)GetValue(SendIntervalProperty); }
            set { SetValue(SendIntervalProperty, value); }
        }

        public static readonly DependencyProperty SendIntervalProperty =
            DependencyProperty.Register("SendInterval", typeof(int), typeof(DataServerItem), new PropertyMetadata(500, new PropertyChangedCallback(DataServerItem_PropertyChanged)));

        public DataServerItem()
        {
            Init();
        }

        public DataServerItem(DataServer dataServer)
        {
            Init();
            if (dataServer != null)
            {
                Hostname = dataServer.Hostname;
                Port = dataServer.Port;
                UseSSL = dataServer.UseSSL;
                SendInterval = dataServer.SendInterval;

                DataServer = dataServer;
            }
        }

        private void Init()
        {
            InitializeComponent();
            DataContext = this;
            Id = Guid.NewGuid().ToString();
        }

        internal void SetDataServerProperties()
        {
            if (DataServer != null)
            {
                DataServer.Hostname = Hostname;
                DataServer.Port = Port;
                DataServer.UseSSL = UseSSL;
                DataServer.SendInterval = SendInterval;
            }
        }
        private static void DataServerItem_PropertyChanged(DependencyObject obj, DependencyPropertyChangedEventArgs e)
        {
            var o = obj as DataServerItem;
            if (o != null) o.SetDataServerProperties();
        }
        
    }
}
