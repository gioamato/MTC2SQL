// Copyright (c) 2017 TrakHound Inc., All Rights Reserved.

// This file is subject to the terms and conditions defined in
// file 'LICENSE', which is part of this source code package.

using System.Collections.Generic;
using System.Linq;
using System.Xml.Serialization;
using TrakHound.Api.v2;
using TrakHound.Api.v2.Data;
using TrakHound.Api.v2.Streams.Data;

namespace MTC2SQL.DataGroups
{
    /// <summary>
    /// Defines what data to capture and how it is captured to be sent to a database
    /// </summary>
    public class DataGroup
    {
        /// <summary>
        /// Unique Identifier created at runtime
        /// </summary>
        [XmlIgnore]
        public string Id { get; set; }

        /// <summary>
        /// The Name of the DataGroup
        /// </summary>
        [XmlAttribute("name")]
        public string Name { get; set; }

        /// <summary>
        /// The CaptureMode of the DataGroup
        /// </summary>
        [XmlAttribute("captureMode")]
        public CaptureMode CaptureMode { get; set; }

        /// <summary>
        /// List of Allowed Types to capture
        /// </summary>
        [XmlArray("Allow")]
        [XmlArrayItem("Filter")]
        public List<string> Allowed { get; set; }

        /// <summary>
        /// List of Denied Types to not capture
        /// </summary>
        [XmlArray("Deny")]
        [XmlArrayItem("Filter")]
        public List<string> Denied { get; set; }

        /// <summary>
        /// List of other DataGroups to include when capturing for this group
        /// </summary>
        [XmlArray("Include")]
        [XmlArrayItem("DataGroup")]
        public List<string> IncludedDataGroups { get; set; }

        public DataGroup()
        {
            Id = System.Guid.NewGuid().ToString();
        }

        /// <summary>
        /// Check a List of DataItemDefintionsData objects based on the DataGroup's filters
        /// </summary>
        /// <returns>A list of DataItemDefinitionData objects that are allowed through the DataGroup</returns>
        public List<DataItemDefinitionData> CheckFilters(List<DataItemDefinitionData> dataItemDefinitions, List<ComponentDefinitionData> componentDefinitions)
        {
            var allowed = new List<DataItemDefinitionData>();

            // Get the Components as a list of ComponentDefinition objects
            var components = componentDefinitions.ToList<ComponentDefinition>();

            foreach (var dataItem in dataItemDefinitions)
            {
                bool match = Allowed == null || Allowed.Count == 0;

                // Search Allowed Filters
                foreach (var filter in Allowed)
                {
                    var dataFilter = new DataFilter(filter, dataItem, components);
                    match = dataFilter.IsMatch();
                    if (match) break;
                }

                if (match)
                {
                    // Search Denied Filters
                    foreach (var filter in Denied)
                    {
                        var dataFilter = new DataFilter(filter, dataItem, components);
                        bool denied = dataFilter.IsMatch();
                        if (denied)
                        {
                            match = false;
                            break;
                        }
                    }
                }

                if (match) allowed.Add(dataItem);
            }

            return allowed;
        }

    }
}
