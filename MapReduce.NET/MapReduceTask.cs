using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Collections;
using System.IO;
using System.Threading;
using MapReduce.NET.Input;
using System.Diagnostics;
using MapReduce.NET.Collections;

namespace MapReduce.NET
{
    public class MapReduceTask : IUpdateSource
    {
        private Stopwatch sw;
        private long lastStatusUpdate;
        private uint reportEveryNth = 100;
        internal event StatusDelegate StatusUpdate;

        public string MapName { get; set; }
        public string ReduceName { get; set; }
        public IOTask Input { get; set; }
        public IOTask Output { get; set; }

        public bool Parallel { get; set; }

        public IDictionary ReduceResult { get; set; }

        internal Thread MapThread { get; set; }
        internal Thread ReduceThread { get; set; }

        internal bool PartialSaveInProgress { get; set; }

        public IDictionary<string,string> Parameters { get; set; }


        public bool IsRunning
        {
            get
            {
                return MapThread != null && ReduceThread != null;
            }
        }



        public uint ReportEveryNth
        {
            get { return reportEveryNth; }
            set
            {
                if (value <= 0)
                    return;

                reportEveryNth = value;
            }
        }

        

        public void WaitForComplete()
        {
            // start a partialsave from here when the memory pressure is high

            if (MapThread != null)
                MapThread.Join();

            if (ReduceThread != null)
                ReduceThread.Join();

            MapThread = null;
            ReduceThread = null;
        }

        internal void FindTypesMap(out MethodInfo miMap, out object dictMap, out object mapper)
        {
            Type tmap = TypeFinder.FindType(MapName);

            miMap = null;
            dictMap = null;
            mapper = null;

            if (tmap != null)
            {
                miMap = tmap.GetMethod("Map");
                ParameterInfo[] pisMap = miMap.GetParameters();
                Type[] dictionaryKeyValue = pisMap[2].ParameterType.GetGenericArguments();

                mapper = Activator.CreateInstance(tmap);

                Type dictTypeGeneric = typeof(CircularArray<,>);

                var dictType = dictTypeGeneric.MakeGenericType(dictionaryKeyValue[0], dictionaryKeyValue[1]);

                dictMap = Activator.CreateInstance(dictType);
            }
        }

        internal void FindTypesReduce(out MethodInfo miRed, out IDictionary dictRed, out object reducer)
        {
            Type tred = TypeFinder.FindType(ReduceName);

            miRed = null;
            dictRed = null;
            reducer = null;

            if (tred != null)
            {
                miRed = tred.GetMethod("Reduce");
                ParameterInfo[] pisRed = miRed.GetParameters();
                Type[] dictionaryKeyValue = pisRed[2].ParameterType.GetGenericArguments();

                Type dictTypeGeneric = typeof(CustomDictionary<,>);

                var dictType = dictTypeGeneric.MakeGenericType(pisRed[0].ParameterType, pisRed[2].ParameterType);

                reducer = Activator.CreateInstance(tred);

                dictRed = (IDictionary)Activator.CreateInstance(dictType);

                //PropertyInfo piComparer = reducer.GetType().GetProperty("Comparer");

                //object comparer = piComparer.GetValue(reducer, null);

                //if (comparer != null)
                //    dictRed = (IDictionary)Activator.CreateInstance(dictType, comparer);
                //else
                //    dictRed = (IDictionary)Activator.CreateInstance(dictType);
            }
        }

        internal IDictionary ExecuteMapReduce<MK,MV,NK,NV,RNV>(IEnumerable input, Mapper<MK,MV,NK,NV> mapper, Reducer<NK, NV, RNV> reducer, CircularArray<NK,NV> dictMap, IDictionary<NK,RNV> dictRed, bool executeMap, bool executeReduce) where MK : class
        {
            if(executeMap && mapper != null)
                Map<MK, MV, NK, NV>(input, mapper, dictMap);

            if(executeReduce && reducer != null)
                Reduce<NK, NV, RNV>(reducer, dictMap, dictRed);

            return dictRed as IDictionary;
        }

        private void Map<MK, MV, NK, NV>(IEnumerable input, Mapper<MK, MV, NK, NV> mapper, CircularArray<NK, NV> dictMap) where MK : class
        {
            mapper.Parameters = this.Parameters;
            InputPlugin<MV> inputPlugin = null;

            if (Input != null) // read from external source
            {
                inputPlugin = Input.GetPlugin() as InputPlugin<MV>;
                inputPlugin.Open();

                input = inputPlugin.Read();

                inputPlugin.StatusUpdate += new StatusDelegate(RaiseStatusUpdate);
            }

            if (input == null)
            {
                dictMap.MapInProgress = false;
                throw new ArgumentException("Empty data source");
            }

            bool inputDict = input is IDictionary;

            uint counter = 0;

            foreach (var item in input)
            {
                while (PartialSaveInProgress) // after a short save we continue
                    Thread.Sleep(100);

                if (++counter % ReportEveryNth == 0)
                    RaiseStatusUpdate(UpdateType.Map, this, counter);

                MK key;
                MV val;

                if (!inputDict) // flat input, no keys
                {
                    key = null;
                    val = (MV)item;
                }
                else
                {
                    var dictItem = (KeyValuePair<MK, MV>)item;
                    key = dictItem.Key;
                    val = dictItem.Value;
                }

                if (inputPlugin != null)
                {
                    mapper.Context.Position = inputPlugin.Position;
                    mapper.Context.Location = inputPlugin.Location;
                }

                mapper.Map(key, val, dictMap);
            }

            dictMap.MapInProgress = false;

            if (inputPlugin != null)
                inputPlugin.Close();
        }

        private void Reduce<NK, NV, RNV>(Reducer<NK, NV, RNV> reducer, CircularArray<NK, NV> dictMap, IDictionary<NK, RNV> dictRed)
        {
            reducer.Parameters = this.Parameters;
            NK key;
            NV val;

            //int reduceTooFastCount = 0;

            while (dictMap.MapInProgress || dictMap.HasNext)
            {
                while (PartialSaveInProgress) // a short save and we continue
                    Thread.Sleep(100);

                if (!dictMap.Pop(out key, out val))
                {
                    Thread.Sleep(1);
                    continue;
                }

                int pos = ((CustomDictionary<NK, RNV>)dictRed).InitOrGetPosition(key);

                RNV reducedValue = ((CustomDictionary<NK, RNV>)dictRed).GetAtPosition(pos);

                RNV newReducedValue = reducer.Reduce(key, val, reducedValue);
                ((CustomDictionary<NK, RNV>)dictRed).StoreAtPosition(pos, newReducedValue);
            }

            reducer.BeforeSave(dictRed);
        }


        internal void SetStopWatch(Stopwatch sw)
        {
            this.sw = sw;
        }

        internal void MergeDictionaries<K,V,NV>(Reducer<K,V,NV> reducer, IDictionary from)
        {
            if (from.GetType() != ReduceResult.GetType())
                return;

            IDictionary<K, NV> fromtyped = ((IDictionary<K, NV>)from);

            foreach (var kv in fromtyped)
            {
                if (kv.Value is IEnumerable)
                {
                    foreach (var subitem in kv.Value as IEnumerable)
                    {
                        int pos = ((CustomDictionary<K, NV>)ReduceResult).InitOrGetPosition((K)kv.Key);
                        NV value = ((CustomDictionary<K, NV>)ReduceResult).GetAtPosition(pos);
                        NV newvalue = reducer.Reduce((K)kv.Key, (V)subitem, value);
                        ((CustomDictionary<K, NV>)ReduceResult).StoreAtPosition(pos, newvalue);
                    }
                }
                else
                {
                    int pos = ((CustomDictionary<K, NV>)ReduceResult).InitOrGetPosition((K)kv.Key);
                    NV value = ((CustomDictionary<K, NV>)ReduceResult).GetAtPosition(pos);
                    NV newvalue = reducer.Reduce(kv.Key, (V)(object)kv.Value, value);
                    ((CustomDictionary<K, NV>)ReduceResult).StoreAtPosition(pos, newvalue);
                }
            }

            from.Clear();
        }

        void RaiseStatusUpdate(UpdateType type, IUpdateSource source, uint processedItems)
        {
            if (sw == null)
                return;

            // if the update is too frequent, slow it down to around 1/sec
            if (lastStatusUpdate == 0)
            {
                lastStatusUpdate = sw.ElapsedMilliseconds;
            }
            else if (sw.ElapsedMilliseconds - lastStatusUpdate < 800)
            {
                source.ReportEveryNth *= 2;
            }
            else if (sw.ElapsedMilliseconds - lastStatusUpdate > 1200)
            {
                source.ReportEveryNth = (uint)(source.ReportEveryNth / 1.5f);
            }

            lastStatusUpdate = sw.ElapsedMilliseconds;

            if (StatusUpdate != null)
                StatusUpdate(type, source, processedItems);
        }
    }
}
