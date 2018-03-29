// Copyright (c) 2017 TrakHound Inc., All Rights Reserved.

// This file is subject to the terms and conditions defined in
// file 'LICENSE', which is part of this source code package.

namespace MTC2SQL.DataGroups
{
    /// <summary>
    /// Type to define when to capture data
    /// </summary>
    public enum CaptureMode
    {
        /// <summary>
        /// Only capture data when included in another DataGroup
        /// </summary>
        INCLUDE,

        /// <summary>
        /// Only capture the most current data
        /// </summary>
        CURRENT,

        /// <summary>
        /// Capture all data and add to archive
        /// </summary>
        ARCHIVE
    }
}
