using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.IO;
using NLog;
using TrakHound.MTConnectSniffer;
using TrakHound.Api.v2.Authentication;
using System.Collections.ObjectModel;
using TrakHound.Api.v2;
using TrakHound.Api.v2.Data;
using TrakHound.Api.v2.Streams.Data;
using MTC2SQL.Modules;


namespace MTC2SQL
{
    /// <summary>
    /// MainWindow.xaml 的交互逻辑
    /// </summary>
    public partial class MainWindow : Window
    {
        private static Logger log = LogManager.GetCurrentClassLogger();
        public static Configuration Configuration;
        public static FileSystemWatcher ConfigurationWatcher;

        private Sniffer sniffer;
        private static DataClient client;

        //是否开始采集数据
        private static bool started = false;

        public bool IsEnabled
        {
            get { return (bool)GetValue(IsEnabledProperty); }
            set { SetValue(IsEnabledProperty, value); }
        }

        public static readonly DependencyProperty IsEnabledPropety = 
            DependencyProperty.Register("IsEnabled", typeof(bool), typeof(MainWindow), new PropertyMetadata(true));

        public Device SelectedDevice
        {
            get { return (Device)GetValue(SelectedDeviceProperty); }
            set
            {
                SetSelectedDevice(value);

                SetValue(SelectedDeviceProperty, value);
            }
        }

        public static readonly DependencyProperty SelectedDeviceProperty =
            DependencyProperty.Register("SelectedDevice", typeof(Device), typeof(MainWindow), new PropertyMetadata(null, new PropertyChangedCallback(SelectedDevice_PropertyChanged)));

        private static void SelectedDevice_PropertyChanged(DependencyObject obj, DependencyPropertyChangedEventArgs e)
        {
            var o = obj as MainWindow;
            if (o != null) o.SetSelectedDevice((Device)e.NewValue);
        }

        internal void SetSelectedDevice(Device device)
        {
            if (device != null)
            {
                SelectedDeviceAddress = device.Address;
                SelectedDevicePort = device.Port;
                SelectedDeviceName = device.DeviceName;
                SelectedDeviceInterval = device.Interval;
            }
            else
            {
                SelectedDeviceAddress = null;
                SelectedDevicePort = 5000;
                SelectedDeviceName = null;
                SelectedDeviceInterval = 500;
            }
        }

        
        /*
        public MySQLItem SelectedMySQL
        {
            get { return (MySQLItem)GetValue(SelectedMySQLProperty); }
            set
            {
                SetSelectedMySQL(value);

                SetValue(SelectedMySQLProperty, value);
            }
        }

        public static readonly DependencyProperty SelectedMySQLProperty =
            DependencyProperty.Register("SelectedMySQL", typeof(MySQLItem), typeof(MainWindow), new PropertyMetadata(null, new PropertyChangedCallback(SelectedMySQL_PropertyChanged)));

        private static void SelectedMySQL_PropertyChanged(DependencyObject obj, DependencyPropertyChangedEventArgs e)
        {
            var o = obj as MainWindow;
            if (o != null) o.SetSelectedMySQL((MySQLItem)e.NewValue);
        }

        internal void SetSelectedMySQL(MySQLItem mySQL)
        {
            if (mySQL != null)
            {
                SelectedMySQLServer = mySQL.Server;
                SelectedMySQLPort = mySQL.Port;
                SelectedMySQLUser = mySQL.User;
                SelectedMySQLPassword = mySQL.Password;
                SelectedMySQLDatabase = mySQL.Database;
                
            }
            else
            {
                SelectedMySQLServer = "localhost";
                SelectedMySQLPort = 3306;
                SelectedMySQLUser = null;
                SelectedMySQLPassword = null;
                SelectedMySQLDatabase = "root";
            }
        }
        */
        

        /*
        public bool FindDevicesAutomatically
        {
            get { return (bool)GetValue(FindDevicesAutomaticallyProperty); }
            set
            {
                SetFindDevicesAutomatically(value);
                SetValue(FindDevicesAutomaticallyProperty, value);
            }
        }

        public static readonly DependencyProperty FindDevicesAutomaticallyProperty =
            DependencyProperty.Register("FindDevicesAutomatically", typeof(bool), typeof(MainWindow), new PropertyMetadata(false, new PropertyChangedCallback(FindDevicesAutomatically_PropertyChanged)));

        private static void FindDevicesAutomatically_PropertyChanged(DependencyObject obj, DependencyPropertyChangedEventArgs e)
        {
            var o = obj as MainWindow;
            if (o != null) o.SetFindDevicesAutomatically((bool)e.NewValue);
        }

        internal void SetFindDevicesAutomatically(bool find)
        {
            SaveDeviceFinder();
        }
        */

        #region "Add Device"

        public string AddDeviceAddress
        {
            get { return (string)GetValue(AddDeviceAddressProperty); }
            set { SetValue(AddDeviceAddressProperty, value); }
        }

        public static readonly DependencyProperty AddDeviceAddressProperty =
            DependencyProperty.Register("AddDeviceAddress", typeof(string), typeof(MainWindow), new PropertyMetadata(null));


        public int AddDevicePort
        {
            get { return (int)GetValue(AddDevicePortProperty); }
            set { SetValue(AddDevicePortProperty, value); }
        }

        public static readonly DependencyProperty AddDevicePortProperty =
            DependencyProperty.Register("AddDevicePort", typeof(int), typeof(MainWindow), new PropertyMetadata(5000));


        public string AddDeviceName
        {
            get { return (string)GetValue(AddDeviceNameProperty); }
            set { SetValue(AddDeviceNameProperty, value); }
        }

        public static readonly DependencyProperty AddDeviceNameProperty =
            DependencyProperty.Register("AddDeviceName", typeof(string), typeof(MainWindow), new PropertyMetadata(null));

        #endregion
        #region "Selected Device"

        public string SelectedDeviceAddress
        {
            get { return (string)GetValue(SelectedDeviceAddressProperty); }
            set { SetValue(SelectedDeviceAddressProperty, value); }
        }

        public static readonly DependencyProperty SelectedDeviceAddressProperty =
            DependencyProperty.Register("SelectedDeviceAddress", typeof(string), typeof(MainWindow), new PropertyMetadata(null));


        public int SelectedDevicePort
        {
            get { return (int)GetValue(SelectedDevicePortProperty); }
            set { SetValue(SelectedDevicePortProperty, value); }
        }

        public static readonly DependencyProperty SelectedDevicePortProperty =
            DependencyProperty.Register("SelectedDevicePort", typeof(int), typeof(MainWindow), new PropertyMetadata(5000));


        public string SelectedDeviceName
        {
            get { return (string)GetValue(SelectedDeviceNameProperty); }
            set { SetValue(SelectedDeviceNameProperty, value); }
        }

        public static readonly DependencyProperty SelectedDeviceNameProperty =
            DependencyProperty.Register("SelectedDeviceName", typeof(string), typeof(MainWindow), new PropertyMetadata(null));


        public int SelectedDeviceInterval
        {
            get { return (int)GetValue(SelectedDeviceIntervalProperty); }
            set { SetValue(SelectedDeviceIntervalProperty, value); }
        }

        public static readonly DependencyProperty SelectedDeviceIntervalProperty =
            DependencyProperty.Register("SelectedDeviceInterval", typeof(int), typeof(MainWindow), new PropertyMetadata(500));

        #endregion

        /*
        #region "Add MySQL"

        public string AddMySQLServer
        {
            get { return (string)GetValue(AddMySQLServerProperty); }
            set { SetValue(AddMySQLServerProperty, value); }
        }

        public static readonly DependencyProperty AddMySQLServerProperty =
            DependencyProperty.Register("AddMySQLServer", typeof(string), typeof(MainWindow), new PropertyMetadata(null));


        public int AddMySQLPort
        {
            get { return (int)GetValue(AddMySQLPortProperty); }
            set { SetValue(AddMySQLPortProperty, value); }
        }

        public static readonly DependencyProperty AddMySQLPortProperty =
            DependencyProperty.Register("AddMySQLPort", typeof(int), typeof(MainWindow), new PropertyMetadata(3306));


        public string AddMySQLUser
        {
            get { return (string)GetValue(AddMySQLUserProperty); }
            set { SetValue(AddMySQLUserProperty, value); }
        }

        public static readonly DependencyProperty AddMySQLUserProperty =
            DependencyProperty.Register("AddMySQLUser", typeof(string), typeof(MainWindow), new PropertyMetadata(null));


        public string AddMySQLPassword
        {
            get { return (string)GetValue(AddMySQLPasswordProperty); }
            set { SetValue(AddMySQLPasswordProperty, value); }
        }

        public static readonly DependencyProperty AddMySQLPasswordProperty =
            DependencyProperty.Register("AddMySQLPassword", typeof(string), typeof(MainWindow), new PropertyMetadata(null));

        public string AddMySQLDatabase
        {
            get { return (string)GetValue(AddMySQLDatabaseProperty); }
            set { SetValue(AddMySQLDatabaseProperty, value); }
        }

        public static readonly DependencyProperty AddMySQLDatabaseProperty =
            DependencyProperty.Register("AddMySQLDatabase", typeof(string), typeof(MainWindow), new PropertyMetadata(null));


        #endregion
        */

        /*
        #region "Selected MySQL"

        public string SelectedMySQLServer
        {
            get { return (string)GetValue(SelectedMySQLServerProperty); }
            set { SetValue(SelectedMySQLServerProperty, value); }
        }

        public static readonly DependencyProperty SelectedMySQLServerProperty =
            DependencyProperty.Register("SelectedMySQLServer", typeof(string), typeof(MainWindow), new PropertyMetadata(null));


        public int SelectedMySQLPort
        {
            get { return (int)GetValue(SelectedMySQLPortProperty); }
            set { SetValue(SelectedMySQLPortProperty, value); }
        }

        public static readonly DependencyProperty SelectedMySQLPortProperty =
            DependencyProperty.Register("SelectedMySQLPort", typeof(int), typeof(MainWindow), new PropertyMetadata(3306));


        public string SelectedMySQLUser
        {
            get { return (string)GetValue(SelectedMySQLUserProperty); }
            set { SetValue(SelectedMySQLUserProperty, value); }
        }

        public static readonly DependencyProperty SelectedMySQLUserProperty =
            DependencyProperty.Register("SelectedMySQLUser", typeof(string), typeof(MainWindow), new PropertyMetadata(null));


        public string SelectedMySQLPassword
        {
            get { return (string)GetValue(SelectedMySQLPasswordProperty); }
            set { SetValue(SelectedMySQLPasswordProperty, value); }
        }

        public static readonly DependencyProperty SelectedMySQLPasswordProperty =
            DependencyProperty.Register("SelectedMySQLPassword", typeof(string), typeof(MainWindow), new PropertyMetadata(null));

        public string SelectedMySQLDatabase
        {
            get { return (string)GetValue(SelectedMySQLDatabaseProperty); }
            set { SetValue(SelectedMySQLDatabaseProperty, value); }
        }

        public static readonly DependencyProperty SelectedMySQLDatabaseProperty =
            DependencyProperty.Register("SelectedMySQLDatabase", typeof(string), typeof(MainWindow), new PropertyMetadata(null));

        #endregion
        */

        //设备的观察者集合，表示一个动态数据集合，在添加项、移除项或刷新整个列表时，此集合将提供通知。
        private ObservableCollection<Device> _devices;
        public ObservableCollection<Device> Devices
        {
            get
            {
                if (_devices == null)
                    _devices = new ObservableCollection<Device>();
                return _devices;
            }

            set
            {
                _devices = value;
            }
        }

        /*
        //MySQLItems
        private ObservableCollection<MySQLItem> _mySQLItems;
        public ObservableCollection<MySQLItem> MySQLItems
        {
            get
            {
                if (_mySQLItems == null)
                {
                    _mySQLItems = new ObservableCollection<MySQLItem>();
                }
                return _mySQLItems;
            }

            set
            {
                _mySQLItems = value;
            }
        }
        */

        public MainWindow()
        {
            InitializeComponent();
            DataContext = this;

            ReadConfigurationFile();
        }

        private void ReadConfigurationFile()
        {
            //Disable File Watcher
            if (ConfigurationWatcher != null) ConfigurationWatcher.EnableRaisingEvents = false;
            Devices.Clear();
            //MySQLItems.Clear();
            SelectedDevice = null;
            //SelectedMySQL = null;

            // Get the default Configuration file path
            string configPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, Configuration.FILENAME);
            
            var config = Configuration.Read(configPath);

            if (config != null)
            {
                Configuration = config;

                // Devices
                if (config.Devices != null)
                {
                    foreach (var device in config.Devices)
                    {
                        Devices.Add(device);
                    }

                    if (Devices != null && Devices.Count > 0) SelectedDevice = Devices[0];
                }

                /*
                // MySQLs
                if (config.DataGroup != null)
                {
                    foreach (var mySQL in config.DataGroup)
                    {
                        MySQLItems.Add(new MySQLItem(mySQL));
                    }

                    if (MySQLItems != null && MySQLItems.Count > 0) SelectedMySQL = MySQLItems[0];
                }
                */
                // Device Finder
                //FindDevicesAutomatically = config.DeviceFinder != null;
                
            }
            else
            {
                config = new Configuration();
                
            }

            StartConfigurationFileWatcher();
            
        }

        /// <summary>
        /// 开始配置文件观察
        /// </summary>
        private void StartConfigurationFileWatcher()
        {
            if (ConfigurationWatcher == null)
            {
                ConfigurationWatcher = new FileSystemWatcher(AppDomain.CurrentDomain.BaseDirectory, Configuration.FILENAME);
                ConfigurationWatcher.Changed += ConfigurationFileChanged;
            }
            ConfigurationWatcher.EnableRaisingEvents = true;
        }

        /**
         * 配置文件改变
         */
        private void ConfigurationFileChanged(object sender, FileSystemEventArgs e)
        {
            if (e.ChangeType == WatcherChangeTypes.Changed)
            {
                Dispatcher.Invoke(new Action(() =>
                {
                    if (MessageBox.Show("DataClient Configuration File has Changed. Do you want to Reload?", "Configuration File Changed", MessageBoxButton.YesNo) == MessageBoxResult.Yes)
                    {
                        ReadConfigurationFile();
                    }
                }));
            }
        }

        internal static void SaveDevices(List<Device> devices)
        {
            if (devices != null)
            {
                if (ConfigurationWatcher != null) ConfigurationWatcher.EnableRaisingEvents = false;

                Configuration.Devices = devices.ToList();
                Configuration.Save();

                if (ConfigurationWatcher != null) ConfigurationWatcher.EnableRaisingEvents = true;
            }
        }

        
        /*
        private void SaveMySQLs()
        {
            if (ConfigurationWatcher != null) ConfigurationWatcher.EnableRaisingEvents = false;

            if (MySQLItems != null)
            {
                Configuration.DataGroup.Clear();
                foreach (var item in MySQLItems)
                {
                    Configuration.DataGroup.Add(item.MySQL);
                }

                Configuration.Save();
            }

            if (ConfigurationWatcher != null) ConfigurationWatcher.EnableRaisingEvents = true;
        }
         */
         
        
        /*
        private void SaveDeviceFinder()
        {
            if (ConfigurationWatcher != null) ConfigurationWatcher.EnableRaisingEvents = false;

            if (FindDevicesAutomatically) Configuration.DeviceFinder = new DeviceFinder.DeviceFinder();
            else Configuration.DeviceFinder = null;

            Configuration.Save();

            if (ConfigurationWatcher != null) ConfigurationWatcher.EnableRaisingEvents = true;
        }
        */
        private void FindDevices_Click(object sender, RoutedEventArgs e)
        {

        }

        private void AddDevice_Click(object sender, RoutedEventArgs e)
        {
            var device = new Device();
            device.DeviceId = Guid.NewGuid().ToString();
            device.Address = AddDeviceAddress;
            device.Port=AddDevicePort;
            device.DeviceName = AddDeviceName;

            Devices.Add(device);
            SelectedDevice = device;

            SaveDevices(Devices.ToList());
        }

        private void SaveDevice_Clicked(object sender, RoutedEventArgs e)
        {
            if (SelectedDevice != null)
            {
                int i = Devices.ToList().FindIndex(o => o.DeviceId == SelectedDevice.DeviceId);
                if (i >=0)
                {
                    var old = Devices[i];

                    var device = new Device();
                    device.DeviceId = old.DeviceId;
                    device.Address = SelectedDeviceAddress;
                    device.Port = SelectedDevicePort;
                    device.DeviceName = SelectedDeviceName;
                    device.Interval = SelectedDeviceInterval;
                    device.PhysicalAddress = old.PhysicalAddress;

                    Devices.RemoveAt(i);
                    Devices.Insert(i, device);
                    SelectedDevice = Devices[i];
                }

                SaveDevices(Devices.ToList());
            }
        }

        private void DeleteDevice_Clicked(object sender, RoutedEventArgs e)
        {
            if (SelectedDevice != null)
            {
                int i = Devices.ToList().FindIndex(o => o.DeviceId == SelectedDevice.DeviceId);
                if (i >= 0)
                {
                    Devices.RemoveAt(i);

                    if (Devices.Count > 0)
                    {
                        i = Math.Min(Devices.Count - 1, i);
                        SelectedDevice = Devices[i];
                    }

                    SaveDevices(Devices.ToList());
                }
            }
        }

        private void ReloadConfiguration_Click(object sender, RoutedEventArgs e)
        {
            ReadConfigurationFile();
        }

        private void Exit_Click(object sender, RoutedEventArgs e)
        {
            //停止DataClient的数据采集DatabaseQueue的数据队列写入线程
            Stop();
            Close();
        }

        private void About_Click(object sender, RoutedEventArgs e)
        {
            MessageBox.Show("About MTC2SQL");
        }

        private void DataGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {

        }

        /*
        private void AddDataServer_Click(object sender, RoutedEventArgs e)
        {
            //load the default dataServer settings to default to
            var dataServer = GetDefaultDataServer();
            if (dataServer != null)
            {
                var item = new DataServerItem(dataServer);
                item.Hostname = AddDataServerHostname;
                item.Port = AddDataServerPort;
                item.UseSSL = AddDataServerUseSSL;

                DataServerItems.Add(item);
                SelectedDataServer = item;

                SaveDataServers();
            }
            else
            {
                MessageBox.Show("Error Adding DataServer. File 'client.config.default' not found or is corrupt.", "Error Adding DataServer");
                log.Error("Get Default DataServer Error :: NOT FOUND!");
            }
        }

        private DataServer GetDefaultDataServer()
        {
            //get the default Configuration file path
            string defaultPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, Configuration.DEFAULT_FILENAME);
            var config = Configuration.Read(defaultPath);
            if (config != null)
            {
                if (config.DataServers != null && config.DataServers.Count > 0)
                {
                    return config.DataServers[0];
                }
            }
            return null;
        }

        private void SaveDataServer_Click(object sender, RoutedEventArgs e)
        {
            if (SelectedDataServer != null)
            {
                var dataServerItem = DataServerItems.ToList().Find(o => o.Id == SelectedDataServer.Id);
                if (dataServerItem != null)
                {
                    dataServerItem.Hostname = SelectedDataServerHostname;
                    dataServerItem.Port = SelectedDataServerPort;
                    dataServerItem.UseSSL = SelectedDataServerUseSSL;
                    dataServerItem.SendInterval = SelectedDataServerSendInterval;

                    SaveDataServers();
                }
            }
        }

        private void DeleteDataServer_Click(object sender, RoutedEventArgs e)
        {
            if (SelectedDataServer != null)
            {
                int i = DataServerItems.ToList().FindIndex(o => o.Id == SelectedDataServer.Id);
                if (i >= 0)
                {
                    DataServerItems.RemoveAt(i);

                    if (DataServerItems.Count > 0)
                    {
                        i = Math.Min(DataServerItems.Count - 1, i);
                        SelectedDataServer = DataServerItems[i];
                    }

                    SaveDataServers();
                }
            }
        }
         */
        
        /*
        /// <summary>
        /// delete mysql_click
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void DeleteMySQL_Click(object sender, RoutedEventArgs e)
        {
            if (SelectedMySQL != null)
            {
                int i = MySQLItems.ToList().FindIndex(o => o.Id == SelectedMySQL.Id);
                if (i >= 0)
                {
                    MySQLItems.RemoveAt(i);
                    if (MySQLItems.Count > 0)
                    {
                        i = Math.Min(MySQLItems.Count - 1, i);
                        SelectedMySQL = MySQLItems[i];
                    }
                    SaveMySQLs();
                }
            }
        }

        private void SaveMySQL_Click(object sender, RoutedEventArgs e)
        {
            if (SelectedMySQL != null)
            {
                var mySQLItem = MySQLItems.ToList().Find(o => o.Id == SelectedMySQL.Id);
                if (mySQLItem != null)
                {
                    mySQLItem.Server = SelectedMySQLServer;
                    mySQLItem.User = SelectedMySQLUser;
                    mySQLItem.Password = SelectedMySQLPassword;
                    mySQLItem.Port = SelectedMySQLPort;
                    mySQLItem.Database = SelectedMySQLDatabase;

                    SaveMySQLs();
                }
            }
        }

        private void AddMySQL_Click(object sender, RoutedEventArgs e)
        {
            var mySQL = GetDefaultMySQL();
            if (mySQL != null)
            {
                var item = new MySQLItem(mySQL);
                item.Server = AddMySQLServer;
                item.Port = AddMySQLPort;
                item.User = AddMySQLUser;
                item.Password = AddMySQLPassword;
                item.Database = AddMySQLDatabase;

                MySQLItems.Add(item);
                SelectedMySQL = item;

                SaveMySQLs();
            }
            else
            {
                MessageBox.Show("Error Adding MySQL. File 'client.config.default' not found or is corrupt.", "Error Adding MySQL");
                log.Error("Get Default MySQL Error :: NOT FOUND!");
            }
        }

        private MySQL GetDefaultMySQL()
        {
            //get the default Configuration file path
            string defaultPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, Configuration.DEFAULT_FILENAME);
            var config = Configuration.Read(defaultPath);
            if (config != null)
            {
                if (config.DataGroup != null && config.DataGroup.Count > 0)
                {
                    return config.DataGroup[0];
                }
            }
            return null;
        }

        */
        private void StartCollectButton_Click(object sender, RoutedEventArgs e)
        {
            Start();
            this.statusLabel.Content = "MTC2SQL Started..";
        }

        /// <summary>
        /// Start the DataClient to Collecting data from MTConnect and send to configurated database 
        /// </summary>
        private static void Start()
        {
            //获取默认的Configuration 文件路径
            string configPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, Configuration.FILENAME);

            var config = Configuration.Read(configPath);
            if (config != null)
            {
                log.Info("Configuration file read from '" + configPath + "'");
                log.Info("------------------------------------");

                //创建一个新的DataClient
                client = new DataClient(config);
                client.Start();
                started = true;
            }
        }

        private void StopCollectButton_Click(object sender, RoutedEventArgs e)
        {
            //停止DataClient的采集线程与写入数据库的线程
            Stop();
            this.statusLabel.Content = "MTC2SQL Stopped..";
        }

        /// <summary>
        /// stop the DataClient
        /// </summary>
        private static void Stop()
        {
            if (client != null)
            {
                client.Stop();
                started = false;
            }
        }
    }
}
