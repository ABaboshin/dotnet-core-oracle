//
// OracleConnection.cs 
//
// Part of the Mono class libraries at
// mcs/class/System.Data.OracleClient/System.Data.OracleClient
//
// Assembly: System.Data.OracleClient.dll
// Namespace: System.Data.OracleClient
//
// Authors: 
//    Daniel Morgan <monodanmorg@yahoo.com>
//    Tim Coleman <tim@timcoleman.com>
//    Hubert FONGARNAND <informatique.internet@fiducial.fr>
//    Marek Safar <marek.safar@gmail.com>
//    Andrey Baboshin <andrey.baboshin@gmail.com>
//
// Copyright (C) Daniel Morgan, 2002, 2005, 2006, 2009
// Copyright (C) Tim Coleman, 2003
// Copyright (C) Hubert FONGARNAND, 2005
// Copyright (C) Andrey Baboshin, 2016
//
// Original source code for setting ConnectionString 
// by Tim Coleman <tim@timcoleman.com>
//
// Copyright (C) Tim Coleman, 2002
//
// Licensed under the MIT/X11 License.
//

//#define ORACLE_DATA_ACCESS
// define ORACLE_DATA_ACCESS for Oracle.DataAccess functionality
// otherwise it defaults to Microsoft's System.Data.OracleClient

using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.Common;
using System.Data.OracleClient.Oci;
using System.Globalization;
using System.Text;

namespace System.Data.OracleClient {

internal struct OracleConnectionInfo
	{
		internal string Username;
		internal string Password;
		internal string Database;
		internal string ConnectionString;
		internal OciCredentialType CredentialType;
		internal bool SetNewPassword;
		internal string NewPassword;
	}

    public sealed class OracleConnection : DbConnection, IDisposable
    {
		string parsedConnectionString;
		byte persistSecurityInfo = 1;
		string connectionString;
		int minPoolSize;
		int maxPoolSize = 100;
		bool pooling = true;
		bool disposed;
		OracleDataReader dataReader;

		OracleTransaction transaction;

		static OracleConnectionPoolManager pools = new OracleConnectionPoolManager ();
		OracleConnectionPool pool;

        public override string ConnectionString
        {
            get {
				if (parsedConnectionString == null)
					return string.Empty;
				return parsedConnectionString;
			}
			set {
				SetConnectionString (value, false);
			}
        }

        public override string Database
        {
            get
            {
                return string.Empty;
            }
        }

        public override string DataSource
        {
            get
            {
				return conInfo.Database;
            }
        }

        public override string ServerVersion
        {
            get
            {
                if (this.State != ConnectionState.Open)
					throw new System.InvalidOperationException ("Invalid operation. The connection is closed.");
				return GetOracleVersion ();
            }
        }

		internal string GetOracleVersion ()
		{
			byte[] buffer = new Byte[256];
			uint bufflen = (uint) buffer.Length;

			IntPtr sh = oci.ServiceContext;
			IntPtr eh = oci.ErrorHandle;

			OciCalls.OCIServerVersion (sh, eh, ref buffer,  bufflen, OciHandleType.Service);
			
			// Get length of returned string
			int 	rsize = 0;
			IntPtr	env = oci.Environment;
			OciCalls.OCICharSetToUnicode (env, null, buffer, out rsize);
			
			// Get string
			StringBuilder ret = new StringBuilder(rsize);
			OciCalls.OCICharSetToUnicode (env, ret, buffer, out rsize);

			return ret.ToString ();
		}

		internal OracleDataReader DataReader {
			get { return dataReader; }
			set { dataReader = value; }
		}


		ConnectionState state;
        public override ConnectionState State
        {
            get
            {
                return state;
            }
        }

		internal OracleTransaction Transaction {
			get { return transaction; }
			set { transaction = value; }
		}

        public override void ChangeDatabase(string databaseName)
        {
            throw new NotImplementedException();
        }

        public override void Close()
        {
            if (transaction != null)
				transaction.Rollback ();

			if (!pooling)
				oci.Disconnect ();
			else if (pool != null)
				pool.ReleaseConnection (oci);

			state = ConnectionState.Closed;
			CreateStateChange (ConnectionState.Open, ConnectionState.Closed);
        }

        OciGlue oci;
        internal OciEnvironmentHandle Environment {
			get { return oci.Environment; }
		}

		internal OciServiceHandle ServiceContext {
			get { return oci.ServiceContext; }
		}

        internal OciErrorHandle ErrorHandle {
			get { return oci.ErrorHandle; }
		}

		public OracleConnection ()
		{
			state = ConnectionState.Closed;
		}

		public OracleConnection (string connectionString)
			: this()
		{
			SetConnectionString (connectionString, false);
		}

        internal OracleConnectionInfo conInfo;

        internal void SetConnectionString (string connectionString, bool persistSecurity)
		{
			persistSecurityInfo = 1;

			conInfo.Username = string.Empty;
			conInfo.Database = string.Empty;
			conInfo.Password = string.Empty;
			conInfo.CredentialType = OciCredentialType.RDBMS;
			conInfo.SetNewPassword = false;
			conInfo.NewPassword = string.Empty;

			if (connectionString == null || connectionString.Length == 0) {
				this.connectionString = connectionString;
				this.parsedConnectionString = connectionString;
				return;
			}

			this.connectionString = connectionString;
			this.parsedConnectionString = this.connectionString;

			connectionString += ";";
			NameValueCollection parameters = new NameValueCollection ();

			bool inQuote = false;
			bool inDQuote = false;
			int inParen = 0;

			string name = String.Empty;
			string value = String.Empty;
			StringBuilder sb = new StringBuilder ();

			foreach (char c in connectionString) {
				switch (c) {
				case '\'':
					inQuote = !inQuote;
					break;
				case '"' :
					inDQuote = !inDQuote;
					break;
				case '(':
					inParen++;
					sb.Append (c);
					break;
				case ')':
					inParen--;
					sb.Append (c);
					break;
				case ';' :
					if (!inDQuote && !inQuote) {
						if (name != String.Empty && name != null) {
							name = name.ToUpper ().Trim ();
							value = sb.ToString ().Trim ();
							parameters [name] = value;
						}
						name = String.Empty;
						value = String.Empty;
						sb = new StringBuilder ();
					}
					else
						sb.Append (c);
					break;
				case '=' :
					if (!inDQuote && !inQuote && inParen == 0) {
						name = sb.ToString ();
						sb = new StringBuilder ();
					}
					else
						sb.Append (c);
					break;
				default:
					sb.Append (c);
					break;
				}
			}

			SetProperties (parameters);

			conInfo.ConnectionString = this.connectionString;

			if (persistSecurity)
				PersistSecurityInfo ();
		}

		private void PersistSecurityInfo ()
		{
			// persistSecurityInfo:
			// 0 = true/yes
			// 1 = false/no (have not parsed out password yet)
			// 2 = like 1, but have parsed out password

			if (persistSecurityInfo == 0 || persistSecurityInfo == 2)
				return;

			persistSecurityInfo = 2;

			if (connectionString == null || connectionString.Length == 0)
				return;

			string conString = connectionString + ";";

			bool inQuote = false;
			bool inDQuote = false;
			int inParen = 0;

			string name = String.Empty;
			StringBuilder sb = new StringBuilder ();
			int nStart = 0;
			int nFinish = 0;
			int i = -1;

			foreach (char c in conString) {
				i ++;

				switch (c) {
				case '\'':
					inQuote = !inQuote;
					break;
				case '"' :
					inDQuote = !inDQuote;
					break;
				case '(':
					inParen++;
					sb.Append (c);
					break;
				case ')':
					inParen--;
					sb.Append (c);
					break;
				case ';' :
					if (!inDQuote && !inQuote) {
						if (name != String.Empty && name != null) {
							name = name.ToUpper ().Trim ();
							if (name.Equals ("PASSWORD") || name.Equals ("PWD")) {
								nFinish = i;
								string part1 = String.Empty;
								string part3 = String.Empty;
								sb = new StringBuilder ();
								if (nStart > 0) {
									part1 = conString.Substring (0, nStart);
									if (part1[part1.Length - 1] == ';')
										part1 = part1.Substring (0, part1.Length - 1);
									sb.Append (part1);
								}
								if (!part1.Equals (String.Empty))
									sb.Append (';');
								if (conString.Length - nFinish - 1 > 0) {
									part3 = conString.Substring (nFinish, conString.Length - nFinish);
									if (part3[0] == ';')  
										part3 = part3.Substring(1, part3.Length - 1);
									sb.Append (part3);
								}
								parsedConnectionString = sb.ToString ();
								return;
							}
						}
						name = String.Empty;
						sb = new StringBuilder ();
						nStart = i;
						nFinish = i;
					}
					else
						sb.Append (c);
					break;
				case '=' :
					if (!inDQuote && !inQuote && inParen == 0) {
						name = sb.ToString ();
						sb = new StringBuilder ();
					}
					else
						sb.Append (c);
					break;
				default:
					sb.Append (c);
					break;
				}
			}
		}

        private void SetProperties (NameValueCollection parameters)
		{
			string value;
			foreach (string name in parameters) {
				value = parameters[name];

				switch (name) {
				case "UNICODE":
					break;
				case "ENLIST":
					break;
				case "CONNECTION LIFETIME":
					// TODO:
					break;
				case "INTEGRATED SECURITY":
					if (!ConvertToBoolean ("integrated security", value))
						conInfo.CredentialType = OciCredentialType.RDBMS;
					else
						conInfo.CredentialType = OciCredentialType.External;
					break;
				case "PERSIST SECURITY INFO":
					if (!ConvertToBoolean ("persist security info", value))
						persistSecurityInfo = 1;
					else
						persistSecurityInfo = 0;
					break;
				case "MIN POOL SIZE":
					minPoolSize = int.Parse (value);
					break;
				case "MAX POOL SIZE":
					maxPoolSize = int.Parse (value);
					break;
				case "DATA SOURCE" :
				case "SERVER" :
					conInfo.Database = value;
					break;
				case "PASSWORD" :
				case "PWD" :
					conInfo.Password = value;
					break;
				case "UID" :
				case "USER ID" :
					conInfo.Username = value;
					break;
				case "POOLING" :
					pooling = ConvertToBoolean("pooling", value);
					break;
				default:
					throw new ArgumentException("Connection parameter not supported: '" + name + "'");
				}
			}
		}

        private bool ConvertToBoolean(string key, string value)
		{
			string upperValue = value.ToUpper();

			if (upperValue == "TRUE" || upperValue == "YES") {
				return true;
			} else if (upperValue == "FALSE" || upperValue == "NO") {
				return false;
			}

			throw new ArgumentException(string.Format(
				"Invalid value \"{0}\" for key '{1}'.", value, key));
		}

		internal void CreateStateChange (ConnectionState original, ConnectionState current)
		{
			StateChangeEventArgs a = new StateChangeEventArgs (original, current);
			OnStateChange (a);
		}

        public override void Open()
        {
            if (State == ConnectionState.Open)
				return;

			PersistSecurityInfo ();

			if (!pooling || conInfo.SetNewPassword == true) {
				oci = new OciGlue ();
				oci.CreateConnection (conInfo);
			} else {
				pool = pools.GetConnectionPool (conInfo, minPoolSize, maxPoolSize);
				oci = pool.GetConnection ();
			}
			state = ConnectionState.Open;

			CreateStateChange (ConnectionState.Closed, ConnectionState.Open);
        }

        protected override Common.DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            if (state == ConnectionState.Closed)
				throw new InvalidOperationException ("The connection is not open.");
			if (transaction != null)
				throw new InvalidOperationException ("OracleConnection does not support parallel transactions.");

			OciTransactionHandle transactionHandle = oci.CreateTransaction ();
			if (transactionHandle == null) 
				throw new Exception("Error: Unable to start transaction");
			else {
				transactionHandle.Begin ();
				transaction = new OracleTransaction (this, isolationLevel, transactionHandle);
			}

			return transaction;
        }

        protected override DbCommand CreateDbCommand()
        {
            OracleCommand command = new OracleCommand ();
			command.Connection = this;
			return command;
        }

		protected override void Dispose (bool disposing)
		{
			if (!disposed) {
				if (State == ConnectionState.Open)
					Close ();
				dataReader = null;
				transaction = null;
				oci = null;
				pool = null;
				conInfo.Username = string.Empty;
				conInfo.Database = string.Empty;
				conInfo.Password = string.Empty;
				connectionString = null;
				parsedConnectionString = null;
				base.Dispose (disposing);
				disposed = true;
			}
		}


		IFormatProvider format_info;
		internal IFormatProvider SessionFormatProvider {
			get {
				if (format_info == null && state == ConnectionState.Open) {
					NumberFormatInfo numberFormatInfo = new NumberFormatInfo ();
					numberFormatInfo.NumberGroupSeparator
					= GetNlsInfo (Session, (uint)OciNlsServiceType.MAXBUFSZ, OciNlsServiceType.GROUP);
					numberFormatInfo.NumberDecimalSeparator
					= GetNlsInfo (Session, (uint)OciNlsServiceType.MAXBUFSZ, OciNlsServiceType.DECIMAL);
					numberFormatInfo.CurrencyGroupSeparator
					= GetNlsInfo (Session, (uint)OciNlsServiceType.MAXBUFSZ, OciNlsServiceType.MONGROUP);
					numberFormatInfo.CurrencyDecimalSeparator
					= GetNlsInfo (Session, (uint)OciNlsServiceType.MAXBUFSZ, OciNlsServiceType.MONDECIMAL);
					format_info = numberFormatInfo;
				}
				return format_info;
			}
		}

		internal OciSessionHandle Session {
			get { return oci.SessionHandle; }
		}

		internal string GetNlsInfo (OciHandle handle, uint bufflen, OciNlsServiceType item)
		{
			byte[] buffer = new Byte[bufflen];

			OciCalls.OCINlsGetInfo (handle, ErrorHandle, 
				ref buffer, bufflen, (ushort) item);

			// Get length of returned string
			int rsize = 0;
			OciCalls.OCICharSetToUnicode (Environment, null, buffer, out rsize);
			
			// Get string
			StringBuilder ret = new StringBuilder (rsize);
			OciCalls.OCICharSetToUnicode (Environment, ret, buffer, out rsize);

			return ret.ToString ();
		}

		public
		new
		OracleCommand CreateCommand ()
		{
			OracleCommand command = new OracleCommand ();
			command.Connection = this;
			return command;
		}
    }
}

