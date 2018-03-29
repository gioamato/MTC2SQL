using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace MTC2SQL
{
    /// <summary>
    /// MySQLItem.xaml 的交互逻辑
    /// </summary>
    public partial class MySQLItem : UserControl
    {
        public MySQL MySQL { get; set; }

        public string Id { get; set; }

        public string Server
        {
            get { return (string)GetValue(ServerProperty); }
            set { SetValue(ServerProperty,value); }
        }

        public static readonly DependencyProperty ServerProperty =
            DependencyProperty.Register("Server", typeof(string), typeof(MySQLItem), new PropertyMetadata(null, new PropertyChangedCallback(MySQLItem_PropertyChanged)));


        public string User
        {
            get { return (string)GetValue(UserProperty); }
            set { SetValue(UserProperty, value); }
        }

        public static readonly DependencyProperty UserProperty =
            DependencyProperty.Register("User", typeof(string), typeof(MySQLItem), new PropertyMetadata(null, new PropertyChangedCallback(MySQLItem_PropertyChanged)));

        public string Password
        {
            get { return (string)GetValue(PasswordProperty); }
            set { SetValue(PasswordProperty, value); }
        }

        public static readonly DependencyProperty PasswordProperty =
            DependencyProperty.Register("Password", typeof(string), typeof(MySQLItem), new PropertyMetadata(null, new PropertyChangedCallback(MySQLItem_PropertyChanged)));

        public int Port
        {
            get { return (int)GetValue(PortProperty); }
            set { SetValue(PortProperty, value); }
        }

        public static readonly DependencyProperty PortProperty =
            DependencyProperty.Register("Port", typeof(int), typeof(MySQLItem), new PropertyMetadata(3306, new PropertyChangedCallback(MySQLItem_PropertyChanged)));

        public string Database
        {
            get { return (string)GetValue(DatabaseProperty); }
            set { SetValue(DatabaseProperty, value); }
        }

        public static readonly DependencyProperty DatabaseProperty =
            DependencyProperty.Register("Database", typeof(string), typeof(MySQLItem), new PropertyMetadata(null, new PropertyChangedCallback(MySQLItem_PropertyChanged)));
        

        public MySQLItem()
        {
            Init();
        }

        public MySQLItem(MySQL mySQL)
        {
            Init();
            if (mySQL != null)
            {
                Server = mySQL.Server;
                User = mySQL.User;
                Password = mySQL.Password;
                Port = mySQL.Port;
                Database = mySQL.Database;

                MySQL = mySQL;
            }
        }

        private void Init()
        {
            InitializeComponent();
            DataContext = this;

            Id = Guid.NewGuid().ToString();
        }


        internal void setMySQLProperties()
        {
            if (MySQL != null)
            {
                MySQL.Server = Server;
                MySQL.User = User;
                MySQL.Password = Password;
                MySQL.Port = Port;
                MySQL.Database = Database;
            }
        }
        private static void MySQLItem_PropertyChanged(DependencyObject obj, DependencyPropertyChangedEventArgs e)
        {
            var o = obj as MySQLItem;
            if (o != null)
                o.setMySQLProperties();
        }
    }
}
