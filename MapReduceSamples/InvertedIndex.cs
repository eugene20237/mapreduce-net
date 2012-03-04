using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MapReduce.NET;
using MapReduce.NET.Collections;


namespace MapReduceSamples
{
    public struct Position
    {
        public string FileName { get; set; }
        public int BytePosition { get; set; }
    }

    public class InvertedIndexMapper : Mapper<string, string, string, Position>
    {
        public override void Map(string key, string value, IQueue<string, Position> result)
        {
            // key is null, coming from txt file

            unsafe
            {
                fixed (char* val = value)
                {
                    for (int i = 0; i < value.Length; i++)
                    {
                        // be careful with unicode input - a character with value of 60000+ is still a valid character!
                        if (val[i] < 'A' || (val[i] < 'a' && val[i] > 'Z') || val[i] > 1000)
                            val[i] = ' ';
                    }
                }
            }

            string[] cleanarr = value.ToLower().Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

            foreach (var item in cleanarr)
            {
                if (IsStopWord(item))
                    continue;

                result.Push(item, new Position { BytePosition = (int)Context.Position, FileName = (string)Context.Location });
            }
        }

        public static bool IsStopWord(string word)
        {
            switch (word)
            {
                case "the": return true;
                case "and": return true;
                case "i": return true;
                case "to": return true;
                case "of": return true;
                case "a": return true;
                case "you": return true;
                case "my": return true;
                case "that": return true;
                case "in": return true;
                case "is": return true;
                case "not": return true;
                case "for": return true;
                case "with": return true;
                case "me": return true;
                case "it": return true;
                case "be": return true;
                case "this": return true;
                case "your": return true;
                case "his": return true;
                case "he": return true;
                case "but": return true;
                case "as": return true;
                case "have": return true;
                case "so": return true;
                case "him": return true;
                case "will": return true;
                case "what": return true;
                case "by": return true;
                case "all": return true;
                case "are": return true;
                case "her": return true;
                case "do": return true;
                case "no": return true;
                case "we": return true;
                case "shall": return true;
                case "if": return true;
                case "on": return true;
                case "or": return true;
                case "our": return true;
                case "from": return true;
                case "at": return true;
                case "they": return true;
                case "she": return true;
                case "let": return true;
                default:
                    return false;
            }
        }
    }

    public class InvertedIndexReducer : Reducer<string, Position, Dictionary<string, List<int>>>
    {
        public override Dictionary<string, List<int>> Reduce(string key, Position value, Dictionary<string, List<int>> result)
        {
            if (result == null)
                result = new Dictionary<string, List<int>>();

            if (!result.ContainsKey(value.FileName))
                result[value.FileName] = new List<int>();

            result[value.FileName].Add(value.BytePosition);

            return result;
        }

        override public void Merge(Dictionary<string, List<int>> to, Dictionary<string, List<int>> from)
        {
            foreach (var key in from.Keys)
            {
                if (to.ContainsKey(key))
                {
                    var targetlist = to[key];
                    foreach (var item in from[key])
                    {
                        targetlist.Add(item);
                    }
                }
                else
                {
                    to.Add(key, from[key]);
                }
            }
        }
    }
}
