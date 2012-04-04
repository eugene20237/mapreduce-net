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
            /*
            Mapreduce.NET.Service.ServiceTest.Test();

            MapReduceDriver prodrec = new MapReduceDriver("configProductRecommendSimple.json");

            IDictionary sessprod = prodrec.Start();


            MapReduceDriver grep = new MapReduceDriver("configGrep.json");

            IDictionary grepresult = grep.Start();


            MapReduceDriver dwordcountInv2 = new MapReduceDriver("configInvertedIndex.json");

            dwordcountInv2.Tasks[0].Output.Location = Guid.NewGuid() + ".db3";

            IDictionary resultInv2 = dwordcountInv2.Start();

            MapReduceDriver wordcount = new MapReduceDriver("configWordCountSQLiteJSONoutput.json");

            IDictionary wordcountresult = wordcount.Start();
            */

            MapReduceDriver dwordcountInv2 = new MapReduceDriver("configInvertedIndexSQLiteBinaryOutput.json");

            dwordcountInv2.Tasks[0].Output.Location = "gutenberg.db3";

            IDictionary resultInv2 = dwordcountInv2.Start();

            


            //SQLiteExternalMerge.Merge(new InvertedIndexReducer(), new ProtobufSerializer(), "mout.db3", "m1bin.db3", "m2bin.db3");
        }
    }
}
