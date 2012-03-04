using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MapReduce.NET
{
    public interface IUpdateSource
    {
        uint ReportEveryNth { get; set; }
    }
}
