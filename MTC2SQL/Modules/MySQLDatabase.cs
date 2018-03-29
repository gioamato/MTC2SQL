using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Reflection;
using TrakHound.Api.v2;
using TrakHound.Api.v2.Data;
using TrakHound.Api.v2.Streams.Data;
using NLog;

namespace MTC2SQL.Modules
{
    public static class MySQLDatabase
    {
        private static Logger log = LogManager.GetCurrentClassLogger();

        /// <summary>
        /// The currently loaded IDatabaseModule
        /// </summary>
        public static IDatabaseModule Module;


        public static bool Initialize(string databaseConfigurationPath)
        {
            MySQLModule module=new MySQLModule();
            if (module != null)
            {
                if (module.Initialize(databaseConfigurationPath))
                Module = module;
                return true;
            }

            return false;
        }

        public static void Close()
        {
            if (Module != null)
            {
                try
                {
                    Module.Close();
                }
                catch (Exception ex)
                {
                    log.Trace(ex);
                }
            }
        }

        #region "Read"

        /// <summary>
        /// Read all of the Connections available from the DataServer
        /// </summary>
        public static List<ConnectionDefinition> ReadConnections()
        {
            if (Module != null) return Module.ReadConnections();

            return null;
        }

        /// <summary>
        /// Read the ConnectionDefintion from the database
        /// </summary>
        public static ConnectionDefinition ReadConnection(string deviceId)
        {
            if (Module != null) return Module.ReadConnection(deviceId);

            return null;
        }

        /// <summary>
        /// Read the most current AgentDefintion from the database
        /// </summary>
        public static AgentDefinition ReadAgent(string deviceId)
        {
            if (Module != null) return Module.ReadAgent(deviceId);

            return null;
        }

        /// <summary>
        /// Read AssetDefintions from the database
        /// </summary>
        public static List<AssetDefinition> ReadAssets(string deviceId, string assetId, DateTime from, DateTime to, DateTime at, long count)
        {
            if (Module != null) return Module.ReadAssets(deviceId, assetId, from, to, at, count);

            return null;
        }

        /// <summary>
        /// Read the ComponentDefinitions for the specified Agent Instance Id from the database
        /// </summary>
        public static List<ComponentDefinition> ReadComponents(string deviceId, long agentInstanceId)
        {
            if (Module != null) return Module.ReadComponents(deviceId, agentInstanceId);

            return null;
        }

        /// <summary>
        /// Read the DataItemDefinitions for the specified Agent Instance Id from the database
        /// </summary>
        public static List<DataItemDefinition> ReadDataItems(string deviceId, long agentInstanceId)
        {
            if (Module != null) return Module.ReadDataItems(deviceId, agentInstanceId);

            return null;
        }

        /// <summary>
        /// Read the DeviceDefintion for the specified Agent Instance Id from the database
        /// </summary>
        public static DeviceDefinition ReadDevice(string deviceId, long agentInstanceId)
        {
            if (Module != null) return Module.ReadDevice(deviceId, agentInstanceId);

            return null;
        }

        /// <summary>
        /// Read Samples from the database
        /// </summary>
        public static List<Sample> ReadSamples(string[] dataItemIds, string deviceId, DateTime from, DateTime to, DateTime at, long count)
        {
            if (Module != null) return Module.ReadSamples(dataItemIds, deviceId, from, to, at, count);

            return null;
        }

        /// <summary>
        /// Read RejectedParts from the database
        /// </summary>
        public static List<RejectedPart> ReadRejectedParts(string deviceId, string[] partIds, DateTime from, DateTime to, DateTime at)
        {
            if (Module != null) return Module.ReadRejectedParts(deviceId, partIds, from, to, at);

            return null;
        }

        /// <summary>
        /// Read VerifiedParts from the database
        /// </summary>
        public static List<VerifiedPart> ReadVerifiedParts(string deviceId, string[] partIds, DateTime from, DateTime to, DateTime at)
        {
            if (Module != null) return Module.ReadVerifiedParts(deviceId, partIds, from, to, at);

            return null;
        }

        /// <summary>
        /// Read the Status from the database
        /// </summary>
        public static Status ReadStatus(string deviceId)
        {
            if (Module != null) return Module.ReadStatus(deviceId);

            return null;
        }

        #endregion

        #region "Write"

        /// <summary>
        /// Write ConnectionDefinitionDatas to the database
        /// </summary>
        public static bool Write(List<ConnectionDefinitionData> definitions)
        {
            if (Module != null) return Module.Write(definitions);

            return false;
        }

        /// <summary>
        /// Write AgentDefintionDatas to the database
        /// </summary>
        public static bool Write(List<AgentDefinitionData> definitions)
        {
            if (Module != null) return Module.Write(definitions);

            return false;
        }

        /// <summary>
        /// Write AssetDefintionDatas to the database
        /// </summary>
        public static bool Write(List<AssetDefinitionData> definitions)
        {
            if (Module != null) return Module.Write(definitions);

            return false;
        }

        /// <summary>
        /// Write ComponentDefintionDatas to the database
        /// </summary>
        public static bool Write(List<ComponentDefinitionData> definitions)
        {
            if (Module != null) return Module.Write(definitions);

            return false;
        }

        /// <summary>
        /// Write DataItemDefintionDatas to the database
        /// </summary>
        public static bool Write(List<DataItemDefinitionData> definitions)
        {
            if (Module != null) return Module.Write(definitions);

            return false;
        }

        /// <summary>
        /// Write DeviceDefintionDatas to the database
        /// </summary>
        public static bool Write(List<DeviceDefinitionData> definitions)
        {
            if (Module != null) return Module.Write(definitions);

            return false;
        }

        /// <summary>
        /// Write Samples to the database
        /// </summary>
        public static bool Write(List<SampleData> samples)
        {
            if (Module != null) return Module.Write(samples);

            return false;
        }


        /// <summary>
        /// Write RejectedParts to the database
        /// </summary>
        public static bool Write(List<RejectedPart> parts)
        {
            if (Module != null) return Module.Write(parts);

            return false;
        }

        /// <summary>
        /// Write VerifiedParts to the database
        /// </summary>
        public static bool Write(List<VerifiedPart> parts)
        {
            if (Module != null) return Module.Write(parts);

            return false;
        }

        /// <summary>
        /// Write Statuses to the database
        /// </summary>
        public static bool Write(List<StatusData> statuses)
        {
            if (Module != null) return Module.Write(statuses);

            return false;
        }

        #endregion

        #region "Delete"

        /// <summary>
        /// Delete RejectedPart from the database
        /// </summary>
        public static bool DeleteRejectedPart(string deviceId, string partId)
        {
            if (Module != null) return Module.DeleteRejectedPart(deviceId, partId);

            return false;
        }

        /// <summary>
        /// Delete VerifiedPart from the database
        /// </summary>
        public static bool DeleteVerifiedPart(string deviceId, string partId)
        {
            if (Module != null) return Module.DeleteVerifiedPart(deviceId, partId);

            return false;
        }

        #endregion

    }
}
