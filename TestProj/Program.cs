using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using MapReduce.NET;
using System.Collections;
using System.Reflection.Emit;
using System.Runtime.InteropServices;
using System.Linq.Expressions;
using System.Reflection;
using System.Diagnostics;
using MapReduce.NET.Output;
using MapReduce.NET.Output.ExternalMerge;
using MapReduce.NET.Serializer;

namespace TestProj
{
    class Program
    {
        static void Main(string[] args)
        {
            Mapreduce.NET.Service.ServiceTest.Test();

            //MapReduceDriver prodrec = new MapReduceDriver("configProductRecommendation.json");

            //IDictionary sessprod = prodrec.Start();

            //return;




            //MapReduceDriver dwordcountInv2 = new MapReduceDriver("configGrep.json");

            //IDictionary resultInv2 = dwordcountInv2.Start();

            //return;


            MapReduceDriver dwordcountInv2 = new MapReduceDriver("configInvertedIndex.json");

            dwordcountInv2.Tasks[0].Output.Location = Guid.NewGuid() + ".db3";

            IDictionary resultInv2 = dwordcountInv2.Start();

            return;

            //SQLiteExternalMerge.Merge(new InvertedIndexReducer(), new ProtobufSerializer(), "mout.db3", "m1bin.db3", "m2bin.db3");

            //return;

            //MapReduceDriver dwordcountInv = new MapReduceDriver("configInvertedIndex.json");

            //IDictionary resultInv = dwordcountInv.Start();

            //return;

            //var sw1 = Stopwatch.StartNew();
            //MapReduceDriver dwordcount12 = new MapReduceDriver("configWordCount2.json");

            //IDictionary result = dwordcount12.Start();

            //Console.WriteLine(sw1.ElapsedMilliseconds);
            //return;


            //var sw = Stopwatch.StartNew();
            //MapReduceDriver dwordcount = new MapReduceDriver("configWordCount.json");
            //dwordcount.Start();
            //Console.WriteLine(sw.ElapsedMilliseconds);

            //MapReduceDriver dBig = new MapReduceDriver("configInvertedIndexBin.json");
            //dBig.Start();

            //return;

            //MapReduceDriver d = new MapReduceDriver("config.json");
            //d.Start();

            //Dictionary<string, string> dict = new Dictionary<string, string>();

            //for (int i = 0; i < 1000 * 1000; i++)
            //{
            //    dict.Add(i.ToString(), i.ToString());
            //}

            //MemoryStream ms = new MemoryStream();

            //using (var file = File.Create(@"c:\temp\out.bin"))
            //{
            //    ProtoBuf.Serializer.Serialize(file, dict);
            //}

            //ms.Position = 0;

            //object o = ProtoBuf.Serializer.Deserialize<Dictionary<string,string>>(ms);
        }
    }
}
