using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Collections;
using MapReduce.NET.Collections;

namespace MapReduce.NET
{
    public abstract class MapReduceBase
    {
        private IDictionary<string, string> parameters;

        internal IDictionary<string, string> Parameters
        {
            get
            {
                return parameters;
            }
            set
            {
                parameters = value;
                TypeFinder.MapDictionary(this, parameters);
            }
        }
    }

    public abstract class Mapper<KEY, VALUE, REDUCEKEY, REDUCEVALUE> :  MapReduceBase
    {
        private MapReduceContext context = new MapReduceContext();

        public MapReduceContext Context
        {
            get { return context; }
            set { context = value; }
        }
        

        public abstract void Map(KEY key, VALUE value, IQueue<REDUCEKEY,REDUCEVALUE> result);
    }

    public abstract class Reducer<REDUCEKEY, REDUCEVALUE, REDUCEDNEWVALUE> : MapReduceBase
    {
        public virtual void Merge(REDUCEDNEWVALUE to, REDUCEDNEWVALUE from) { throw new NotImplementedException("The result of Reduce is not auto-mergable! Override Reducer.Merge."); }

        public virtual void BeforeSave(IDictionary<REDUCEKEY, REDUCEDNEWVALUE> dict) { }

        public abstract REDUCEDNEWVALUE Reduce(REDUCEKEY key, REDUCEVALUE value, REDUCEDNEWVALUE result);
    }
}
