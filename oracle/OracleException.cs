// 
// OracleException.cs
//
// Part of the Mono class libraries at
// mcs/class/System.Data.OracleClient/System.Data.OracleClient
//
// Assembly: System.Data.OracleClient.dll
// Namespace: System.Data.OracleClient
//
// Authors: 
//    Tim Coleman <tim@timcoleman.com>
//
// Copyright (C) Daniel Morgan, 2002
// Copyright (C) Tim Coleman , 2003
//
// Licensed under the MIT/X11 License.
//

using System;

namespace System.Data.OracleClient {
	public sealed class OracleException : System.Data.Common.DbException
	{
		public OracleException (string message, Exception innerException)
		:base(message, innerException)
		{
		  
		}

		int code;

		internal OracleException (int code, string message) : base (message)
		{
			this.code = code;
		}
	}
}
