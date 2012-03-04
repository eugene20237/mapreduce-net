using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using System.IO;
using System.Reflection;

namespace MapReduce.NET.Serializer
{
    public class ProtobufSerializer : ISerializer
    {
        MemoryStream ms = new MemoryStream();

        public object Serialize<T>(T item) 
        {
            ProtoBuf.Serializer.Serialize(ms, item);

            byte[] retval = ms.ToArray();

            ms.SetLength(0);

            return retval;
        }

        public T Deserialize<T>(object source)
        {
            byte[] buff = source as byte[];

            ms.Write(buff, 0, buff.Length);

            T retval = ProtoBuf.Serializer.Deserialize<T>(ms);

            ms.Position = 0;

            ms.SetLength(0);

            return retval;
        }
    }
}
