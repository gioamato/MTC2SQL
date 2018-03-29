using System;
using System.Collections.Generic;


namespace MTC2SQL
{
    /// <summary>
    /// Handles functions related to Reading/Writing of CSV files
    /// </summary>
    static class Csv
    {
        /// <summary>
        /// Converts an object to a CSV line
        /// </summary>
        public static string ToCsv(object obj)
        {
            var l = new List<object>();

            // Read each property of the object and add to list
            foreach (var property in obj.GetType().GetProperties())
            {
                l.Add(property.GetValue(obj, null));
            }

            // Convert the list of strings to a CSV line and Return
            return string.Join(",", l);
        }

        /// <summary>
        /// Read an object from a CSV line/>
        /// </summary>
        /// <typeparam name="T">Type of the object to return</typeparam>
        /// <param name="line">the CSV line to parse</param>
        public static T FromCsv<T>(string line)
        {
            // Create an array representing each field in the CSV line
            var fields = line.Split(',');

            // Get list of properties for the Type "T"
            var properties = typeof(T).GetProperties();

            // Make sure that the array matches the number of properties
            if (fields.Length >= properties.Length)
            {
                // Create a new instance of the object of type "T"
                var obj = (T)Activator.CreateInstance(typeof(T));

                // Loop through the properties and set values based on the corresponding CSV field
                for (int i = 0; i < properties.Length; i++)
                {
                    var p = properties[i];
                    if (p.CanWrite) p.SetValue(obj, Convert.ChangeType(fields[i], p.PropertyType), null);
                }

                return obj;
            }

            return default(T);
        }
    }
}
