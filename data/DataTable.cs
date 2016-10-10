// 
// OciAttributeType.cs 
//  
// Part of managed C#/.NET library System.Data.OracleClient.dll for .net core
//
// Author: 
//     Andrey Baboshin <andrey.baboshin@gmail.com>
//         
// Copyright (C) Andrey Baboshin, 2016
// 
// Licensed under the MIT/X11 License.

using System.Collections.Generic;

namespace Sytem.Data.Common.Ex {
    public class DataTable {
        public string Name { get; set; }

        public List<DataColumn> Columns { get; set; }

        public DataTable (string name)
        {
          Name = name;
          Columns = new List<DataColumn>();
        }
    }
}