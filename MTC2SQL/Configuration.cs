using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.Xml.Serialization;
using NLog;
using System.IO;
using MTC2SQL.DataGroups;

namespace MTC2SQL
{
    /// <summary>
    /// The configuration for a DataClient class
    /// </summary>
    [XmlRoot("DataClient")]
    public class Configuration
    {
        private static Logger log = LogManager.GetCurrentClassLogger();

        /// <summary>
        /// The default filename used for the Configuration file
        /// </summary>
        [XmlIgnore]
        public const string FILENAME = "client.config";

        [XmlIgnore]
        public const string DEFAULT_FILENAME = "client.config.default";

        protected string _path;
        /// <summary>
        /// Gets the file path that the Configuration was read from. Read Only.
        /// </summary>
        [XmlIgnore]
        public string Path { get { return _path; } }

        /// <summary>
        /// Gets or Sets the DeviceFinder to use for finding new MTConnect devices on the network
        /// </summary>
        [XmlElement("DeviceFinder")]
        public DeviceFinder.DeviceFinder DeviceFinder { get; set; }

        /// <summary>
        /// Gets or Sets a list of Devices that read from the MTConnect Agent.
        /// </summary>
        [XmlArray("Devices")]
        [XmlArrayItem("Device", typeof(Device))]
        public List<Device> Devices { get; set; }


        /// <summary>
        /// Gets or Sets DataGroups
        /// </summary>
        [XmlArray("DataGroups")]
        [XmlArrayItem("DataGroup")]
        public List<DataGroup> DataGroups { get; set; }

        public Configuration()
        {
            Devices = new List<Device>();
            DataGroups = new List<DataGroup>();
        }

        /// <summary>
        /// Reads a new Configuration from a file path.
        /// </summary>
        /// <param name="path">The path of the configuration file to read from</param>
        /// <returns>The new Configuration object</returns>
        public static Configuration Read(string path)
        {
            if (File.Exists(path))
            {
                try
                {
                    // Create a new XML Serializer
                    var serializer = new XmlSerializer(typeof(Configuration));

                    // Create a new FileStream to Open the configuration file for reading
                    using (var fileReader = new FileStream(path, FileMode.Open))
                    using (var xmlReader = XmlReader.Create(fileReader))
                    {
                        // Deserialize the Configuration object using the XML Serializer
                        var config = (Configuration)serializer.Deserialize(xmlReader);

                        // Set the path that the configuration was read from
                        config._path = path;

                        // Return the new Configuration object
                        return config;
                    }
                }
                catch (Exception ex)
                {
                    log.Error(ex);
                }
            }

            return null;
        }

        /// <summary>
        /// Save the Configuration back to the original file path it was initially read from. Overwrites original file.
        /// </summary>
        public void Save()
        {
            if (!string.IsNullOrEmpty(Path))
            {
                try
                {
                    // Create a new XML Serializer
                    var serializer = new XmlSerializer(typeof(Configuration));

                    // Create a new FileStream to Create/Overwrite the file
                    using (var fileWriter = new FileStream(Path, FileMode.Create))
                    using (var xmlWriter = XmlWriter.Create(fileWriter, new XmlWriterSettings() { Indent = true }))
                    {
                        // Serialize the Configuration object to XML
                        serializer.Serialize(xmlWriter, this);
                    }

                    log.Info("Configuration Saved : " + Path);
                }
                catch (Exception ex)
                {
                    log.Error(ex);
                }
            }
            else log.Warn("Configuration could not be saved. No Path is set.");
        }
    }
}
