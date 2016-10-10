//
// OracleCommand.cs
//
// Part of the Mono class libraries at
// mcs/class/System.Data.OracleClient/System.Data.OracleClient
//
// Assembly: System.Data.OracleClient.dll
// Namespace: System.Data.OracleClient
//
// Authors:
//    Daniel Morgan <danielmorgan@verizon.net>
//    Tim Coleman <tim@timcoleman.com>
//    Marek Safar <marek.safar@gmail.com>
//    Andrey Baboshin <andrey.baboshin@gmail.com>
//
// Copyright (C) Daniel Morgan, 2002, 2004-2005
// Copyright (C) Tim Coleman , 2003
// Copyright (C) Andrey Baboshin, 2016
//
// Licensed under the MIT/X11 License.
//


using System.Data.Common;
using System.Data.OracleClient.Oci;
using System.Text;

namespace System.Data.OracleClient {
    public class OracleCommand : DbCommand
    {
		#region Fields

		CommandBehavior behavior;
		string commandText;
		CommandType commandType;
		OracleConnection connection;
		bool designTimeVisible;
		OracleParameterCollection parameters;
		OracleTransaction transaction;
		UpdateRowSource updatedRowSource;
		OciStatementHandle preparedStatement;
		
		int moreResults;

		#endregion // Fields

        #region Constructors

		public OracleCommand ()
			: this (String.Empty, null, null)
		{
		}

		public OracleCommand (string commandText)
			: this (commandText, null, null)
		{
		}

		public OracleCommand (string commandText, OracleConnection connection)
			: this (commandText, connection, null)
		{
		}

		public OracleCommand (string commandText, OracleConnection connection, OracleTransaction tx)
		{
			moreResults = -1;
			preparedStatement = null;
			CommandText = commandText;
			Connection = connection;
			Transaction = tx;
			CommandType = CommandType.Text;
			UpdatedRowSource = UpdateRowSource.Both;
			DesignTimeVisible = true;
			parameters = new OracleParameterCollection ();
		}

		#endregion // Constructors

        public override string CommandText
        {
            get {
				if (commandText == null)
					return string.Empty;

				return commandText;
			}
			set { commandText = value; }
        }

        public override int CommandTimeout
        {
            get { return 0; }
			set { }
        }

        public override CommandType CommandType
        {
            get { return commandType; }
			set {
				if (value == CommandType.TableDirect)
					throw new ArgumentException ("OracleClient provider does not support TableDirect CommandType.");
				commandType = value;
			}
        }

        public override bool DesignTimeVisible
        {
            get { return designTimeVisible; }
			set { designTimeVisible = value; }
        }

        public override UpdateRowSource UpdatedRowSource
        {
            get { return updatedRowSource; }
			set { updatedRowSource = value; }
        }

        public
		new
		OracleConnection Connection {
			get { return connection; }
			set { connection = value; }
		}

        protected override DbConnection DbConnection {
			get { return Connection; }
			set { Connection = (OracleConnection) value; }
		}

        protected override DbParameterCollection DbParameterCollection
        {
            get { return Parameters; }
        }

        protected override DbTransaction DbTransaction
        {
            get { return Transaction; }
			set { Transaction = (OracleTransaction) value; }
        }

        public override void Cancel()
        {
            throw new NotImplementedException();
        }

        private void AssertConnectionIsOpen ()
		{
			if (Connection == null || Connection.State == ConnectionState.Closed)
				throw new InvalidOperationException ("An open Connection object is required to continue.");
		}

        private void AssertTransactionMatch ()
		{
			if (Connection.Transaction != null && Transaction != Connection.Transaction)
				throw new InvalidOperationException ("Execute requires the Command object to have a Transaction object when the Connection object assigned to the command is in a pending local transaction.  The Transaction property of the Command has not been initialized.");
		}

        private void AssertCommandTextIsSet ()
		{
			if (CommandText.Length == 0)
				throw new InvalidOperationException ("The command text for this Command has not been set.");
		}

        public
		new
		OracleTransaction Transaction {
			get { return transaction; }
			set { transaction = value; }
		}

        public override int ExecuteNonQuery()
        {
            moreResults = -1;

			AssertConnectionIsOpen ();
			AssertTransactionMatch ();
			AssertCommandTextIsSet ();
			bool useAutoCommit = false;

			if (Transaction != null)
				Transaction.AttachToServiceContext ();
			else
				useAutoCommit = true;

			OciStatementHandle statement = GetStatementHandle ();
			try {
				return ExecuteNonQueryInternal (statement, useAutoCommit);
			} finally {
				SafeDisposeHandle (statement);
			}
        }

        private int ExecuteNonQueryInternal (OciStatementHandle statement, bool useAutoCommit)
		{
			moreResults = -1;

			if (preparedStatement == null)
				PrepareStatement (statement);

			bool isNonQuery = IsNonQuery (statement);

			BindParameters (statement);
			if (isNonQuery == true)
				statement.ExecuteNonQuery (useAutoCommit);
			else
				statement.ExecuteQuery (false);

			UpdateParameterValues ();

			int rowsAffected = statement.GetAttributeInt32 (OciAttributeType.RowCount, ErrorHandle);

			return rowsAffected;
		}

        void PrepareStatement (OciStatementHandle statement)
		{
			if (commandType == CommandType.StoredProcedure) {
				StringBuilder sb = new StringBuilder ();
				if (Parameters.Count > 0)
					foreach (OracleParameter parm in Parameters) {
						if (sb.Length > 0)
							sb.Append (",");
						sb.Append (parm.ParameterName + "=>:" + parm.ParameterName);
					}

				string sql = "begin " + commandText + "(" + sb.ToString() + "); end;";
				statement.Prepare (sql);
			} else	// Text
				statement.Prepare (commandText);
		}

        internal OciErrorHandle ErrorHandle {
			get { return ((OracleConnection)Connection).ErrorHandle; }
		}

        private bool IsNonQuery (OciStatementHandle statementHandle)
		{
			// assumes Prepare() has been called prior to calling this function

			OciStatementType statementType = statementHandle.GetStatementType ();
			if (statementType.Equals (OciStatementType.Select))
				return false;

			return true;
		}

        private void SafeDisposeHandle (OciStatementHandle h)
		{
			if (h != null && h != preparedStatement) 
				h.Dispose();
		}

        private OciStatementHandle GetStatementHandle ()
		{
			AssertConnectionIsOpen ();
			if (preparedStatement != null)
				return preparedStatement;

			OciStatementHandle h = (OciStatementHandle) Connection.Environment.Allocate (OciHandleType.Statement);
			h.ErrorHandle = Connection.ErrorHandle;
			h.Service = Connection.ServiceContext;
			h.Command = this;
			return h;
		}

        private void BindParameters (OciStatementHandle statement)
		{
			for (int p = 0; p < Parameters.Count; p++)
				Parameters[p].Bind (statement, Connection, (uint) p);
		}

        public override object ExecuteScalar()
        {
            moreResults = -1;
			object output = null;//if we find nothing we return this

			AssertConnectionIsOpen ();
			AssertTransactionMatch ();
			AssertCommandTextIsSet ();

			if (Transaction != null)
				Transaction.AttachToServiceContext ();

			OciStatementHandle statement = GetStatementHandle ();
			try {
				if (preparedStatement == null)
					PrepareStatement (statement);

				bool isNonQuery = IsNonQuery (statement);

				BindParameters (statement);

				if (isNonQuery == true)
					ExecuteNonQueryInternal (statement, false);
				else {
					statement.ExecuteQuery (false);

					if (statement.Fetch ()) {
						OciDefineHandle defineHandle = (OciDefineHandle) statement.Values [0];
						if (!defineHandle.IsNull)
						{
							switch (defineHandle.DataType) {
							case OciDataType.Blob:
							case OciDataType.Clob:
								OracleLob lob = (OracleLob) defineHandle.GetValue (
									Connection.SessionFormatProvider, Connection);
								lob.connection = Connection;
								output = lob.Value;
								lob.Close ();
								break;
							default:
								output = defineHandle.GetValue (
									Connection.SessionFormatProvider, Connection);
								break;
							}
						}
					}
					UpdateParameterValues ();
				}
			} finally {
				SafeDisposeHandle (statement);
			}

			return output;
        }

        public override void Prepare()
        {
            AssertConnectionIsOpen ();
			OciStatementHandle statement = GetStatementHandle ();
			PrepareStatement (statement);
			preparedStatement = statement;
        }

        protected override DbParameter CreateDbParameter()
        {
            return new OracleParameter ();
        }

        internal void UpdateParameterValues ()
		{
			moreResults = -1;
			if (Parameters.Count > 0) {
				bool foundCursor = false;
				for (int p = 0; p < Parameters.Count; p++) {
					OracleParameter parm = Parameters [p];
					if (parm.OracleType.Equals (OracleType.Cursor)) {
						if (!foundCursor && parm.Direction != ParameterDirection.Input) {
							// if there are multiple REF CURSORs,
							// you only can get the first cursor for now
							// because user of OracleDataReader
							// will do a NextResult to get the next 
							// REF CURSOR (if it exists)
							foundCursor = true;
							parm.Update (this);
							if (p + 1 == Parameters.Count)
								moreResults = -1;
							else
								moreResults = p;
						}
					} else
						parm.Update (this);
				}
			}
		}

        public
		new
		OracleDataReader ExecuteReader ()
		{
			return ExecuteReader (CommandBehavior.Default);
		}

        public
		new
		OracleDataReader ExecuteReader (CommandBehavior behavior)
		{
			AssertConnectionIsOpen ();
			AssertTransactionMatch ();
			AssertCommandTextIsSet ();

			moreResults = -1;

			bool hasRows = false;

			this.behavior = behavior;

			if (Transaction != null)
				Transaction.AttachToServiceContext ();

			OciStatementHandle statement = GetStatementHandle ();
			OracleDataReader rd = null;

			try {
				if (preparedStatement == null)
					PrepareStatement (statement);
				else
					preparedStatement = null;	// OracleDataReader releases the statement handle

				bool isNonQuery = IsNonQuery (statement);

				BindParameters (statement);

				if (isNonQuery) 
					ExecuteNonQueryInternal (statement, false);
				else {	
					if ((behavior & CommandBehavior.SchemaOnly) != 0)
						statement.ExecuteQuery (true);
					else
						hasRows = statement.ExecuteQuery (false);

					UpdateParameterValues ();
				}

				if (Parameters.Count > 0) {
					for (int p = 0; p < Parameters.Count; p++) {
						OracleParameter parm = Parameters [p];
						if (parm.OracleType.Equals (OracleType.Cursor)) {
							if (parm.Direction != ParameterDirection.Input) {
								rd = (OracleDataReader) parm.Value;
								break;
							}
						}
					}					
				}

				if (rd == null)
					rd = new OracleDataReader (this, statement, hasRows, behavior);

			} finally {
				if (statement != null && rd == null)
					statement.Dispose();
			}

			return rd;
		}

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            AssertConnectionIsOpen ();
			AssertTransactionMatch ();
			AssertCommandTextIsSet ();

			moreResults = -1;

			bool hasRows = false;

			this.behavior = behavior;

			if (Transaction != null)
				Transaction.AttachToServiceContext ();

			OciStatementHandle statement = GetStatementHandle ();
			OracleDataReader rd = null;

			try {
				if (preparedStatement == null)
					PrepareStatement (statement);
				else
					preparedStatement = null;	// OracleDataReader releases the statement handle

				bool isNonQuery = IsNonQuery (statement);

				BindParameters (statement);

				if (isNonQuery) 
					ExecuteNonQueryInternal (statement, false);
				else {	
					if ((behavior & CommandBehavior.SchemaOnly) != 0)
						statement.ExecuteQuery (true);
					else
						hasRows = statement.ExecuteQuery (false);

					UpdateParameterValues ();
				}

				if (Parameters.Count > 0) {
					for (int p = 0; p < Parameters.Count; p++) {
						OracleParameter parm = Parameters [p];
						if (parm.OracleType.Equals (OracleType.Cursor)) {
							if (parm.Direction != ParameterDirection.Input) {
								rd = (OracleDataReader) parm.Value;
								break;
							}
						}
					}					
				}

				if (rd == null)
					rd = new OracleDataReader (this, statement, hasRows, behavior);

			} finally {
				if (statement != null && rd == null)
					statement.Dispose();
			}

			return rd;
        }

        internal OciStatementHandle GetNextResult () 
		{
			if (moreResults == -1)
				return null;

			if (Parameters.Count > 0) {
				int p = moreResults + 1;
				
				if (p >= Parameters.Count) {
					moreResults = -1;
					return null;
				}

				for (; p < Parameters.Count; p++) {
					OracleParameter parm = Parameters [p];
					if (parm.OracleType.Equals (OracleType.Cursor)) {
						if (parm.Direction != ParameterDirection.Input) {
							if (p + 1 == Parameters.Count)
								moreResults = -1;
							else 
								moreResults = p;
							return parm.GetOutRefCursor (this);
							
						}
					} 
				}
			}

			moreResults = -1;
			return null;
		}

        public
		new
		OracleParameterCollection Parameters {
			get { return parameters; }
		}

        internal void CloseDataReader ()
		{
			Connection.DataReader = null;
			if ((behavior & CommandBehavior.CloseConnection) != 0)
				Connection.Close ();
		}
    }
}