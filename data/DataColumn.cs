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

using System;

namespace Sytem.Data.Common.Ex {
    public class DataColumn {
        public string Name { get; set; }

        public Type Type { get; set; }

        public DataColumn (string name, Type type)
        {
          Name = name;
          Type = type;
        }
    }
}