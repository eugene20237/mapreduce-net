using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;

namespace MapReduce.NET
{
    public static class TypeFinder
    {
        public static Type FindType(string toFind)
        {
            if (toFind == null)
                return null;

            Type type = null;

            Assembly[] assemblies = AppDomain.CurrentDomain.GetAssemblies();

            foreach (Assembly assembly in assemblies)
            {
                type = assembly.GetType(toFind);

                if (type != null)
                    break;
            }

            if (type == null)
            {
                string assemblyname = toFind.Substring(0, toFind.IndexOf('.'));
                Assembly asm = Assembly.Load(assemblyname);
                type = asm.GetType(toFind);
            }

            return type;
        }

        public static void MapDictionary(object mapTo, IDictionary<string, string> dict)
        {
            if (mapTo == null)
                return;

            if (dict == null)
                return;

            Type t = mapTo.GetType();

            foreach (var key in dict.Keys)
            {
                PropertyInfo pi = t.GetProperty(key, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.FlattenHierarchy | BindingFlags.Instance);

                if (pi == null)
                    continue;

                try
                {
                    var val = Convert.ChangeType(dict[key], pi.PropertyType);
                    pi.SetValue(mapTo, val, null);

                }
                catch (Exception)
                {
                }
            }
        }
    }
}
