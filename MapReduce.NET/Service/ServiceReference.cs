using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ServiceModel.Web;
using System.IO;
using System.IO.Compression;

namespace MapReduce.NET.Service
{

    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.ServiceModel", "4.0.0.0")]
    [System.ServiceModel.ServiceContractAttribute(ConfigurationName = "IMapReduceService")]
    public interface IMapReduceService<K,V>
    {

        [WebInvoke(Method = "GET")]
        [System.ServiceModel.OperationContractAttribute(Action = "http://tempuri.org/IMapReduceService/Status", ReplyAction = "http://tempuri.org/IMapReduceService/StatusResponse")]
        Mapreduce.NET.Service.StatusMessage Status();

        [WebInvoke(Method = "POST")]
        [System.ServiceModel.OperationContractAttribute(Action = "http://tempuri.org/IMapReduceService/Start", ReplyAction = "http://tempuri.org/IMapReduceService/StartResponse")]
        Mapreduce.NET.Service.StatusMessage Start(string config, System.Collections.Generic.Dictionary<string, string> parameters);

        [WebInvoke(Method = "GET")]
        [System.ServiceModel.OperationContractAttribute(Action = "http://tempuri.org/IMapReduceService/Abort", ReplyAction = "http://tempuri.org/IMapReduceService/AbortResponse")]
        void AbortMapReduce();

        [WebInvoke(Method = "GET")]
        [System.ServiceModel.OperationContractAttribute(Action = "http://tempuri.org/IMapReduceService/WaitToFinish", ReplyAction = "http://tempuri.org/IMapReduceService/WaitToFinishResponse")]
        void WaitToFinish();

        [WebInvoke(Method = "GET")]
        [System.ServiceModel.OperationContractAttribute(Action = "http://tempuri.org/IMapReduceService/GetResultSetList", ReplyAction = "http://tempuri.org/IMapReduceService/GetResultSetListResponse")]
        System.Guid[] GetResultSetList();

        [WebInvoke(Method = "GET")]
        [System.ServiceModel.OperationContractAttribute(Action = "http://tempuri.org/IMapReduceService/GetFileList", ReplyAction = "http://tempuri.org/IMapReduceService/GetFileListResponse")]
        string[] GetFileList(System.Guid resultSetId);

        [WebInvoke(Method = "GET")]
        [System.ServiceModel.OperationContractAttribute(Action = "http://tempuri.org/IMapReduceService/RemoveResultSet", ReplyAction = "http://tempuri.org/IMapReduceService/RemoveResultSetResponse")]
        void RemoveResultSet(System.Guid resuletSetId);

        [WebInvoke(Method = "GET")]
        [System.ServiceModel.OperationContractAttribute(Action = "http://tempuri.org/IMapReduceService/GetMemoryResult", ReplyAction = "http://tempuri.org/IMapReduceService/GetMemoryResultResponse")]
        string GetMemoryResult(bool purgeData);
    }

    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.ServiceModel", "4.0.0.0")]
    public interface IMapReduceServiceChannel<K,V> : IMapReduceService<K,V>, System.ServiceModel.IClientChannel
    {
    }

    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.ServiceModel", "4.0.0.0")]
    public partial class MapReduceServiceClient<K, V> : System.ServiceModel.ClientBase<IMapReduceService<K, V>>, IMapReduceService<K, V>
    {

        public MapReduceServiceClient()
        {
        }

        public MapReduceServiceClient(string endpointConfigurationName) :
            base(endpointConfigurationName)
        {
        }

        public MapReduceServiceClient(string endpointConfigurationName, string remoteAddress) :
            base(endpointConfigurationName, remoteAddress)
        {
        }

        public MapReduceServiceClient(string endpointConfigurationName, System.ServiceModel.EndpointAddress remoteAddress) :
            base(endpointConfigurationName, remoteAddress)
        {
        }

        public MapReduceServiceClient(System.ServiceModel.Channels.Binding binding, System.ServiceModel.EndpointAddress remoteAddress) :
            base(binding, remoteAddress)
        {
        }

        public Mapreduce.NET.Service.StatusMessage Status()
        {
            return base.Channel.Status();
        }

        public Mapreduce.NET.Service.StatusMessage Start(string config, System.Collections.Generic.Dictionary<string, string> parameters)
        {
            return base.Channel.Start(config, parameters);
        }

        public void AbortMapReduce()
        {
            base.Channel.AbortMapReduce();
        }

        public void WaitToFinish()
        {
            base.Channel.WaitToFinish();
        }

        public System.Guid[] GetResultSetList()
        {
            return base.Channel.GetResultSetList();
        }

        public string[] GetFileList(System.Guid resultSetId)
        {
            return base.Channel.GetFileList(resultSetId);
        }

        public void RemoveResultSet(System.Guid resuletSetId)
        {
            base.Channel.RemoveResultSet(resuletSetId);
        }

        public string GetMemoryResult(bool purgeData)
        {
            return base.Channel.GetMemoryResult(purgeData);
        }

        public IDictionary<K, V> GetMemoryResultDictionary(bool purgeData)
        {
             var result = base.Channel.GetMemoryResult(purgeData);
            byte[] resultByteArr = Convert.FromBase64String(result);

            MemoryStream ms = new MemoryStream(resultByteArr);

            GZipStream gz = new GZipStream(ms, CompressionMode.Decompress);

            IDictionary<K,V> dict = ProtoBuf.Serializer.Deserialize<IDictionary<K,V>>(gz);

            return dict;
        }

    }
}
