using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json;

namespace MapReduce.NET.Serializer
{
    public class JsonSerializer : ISerializer
    {
        public object Serialize<T>(T item)
        {
            return JsonConvert.SerializeObject(item);
        }

        public T Deserialize<T>(object source)
        {
            return JsonConvert.DeserializeObject<T>(source as string);
        }

    }
}
