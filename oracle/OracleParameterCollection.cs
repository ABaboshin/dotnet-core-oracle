//
// OracleParameterCollection.cs
//
// Part of the Mono class libraries at
// mcs/class/System.Data.OracleClient/System.Data.OracleClient
//
// Assembly: System.Data.OracleClient.dll
// Namespace: System.Data.OracleClient
//
// Authors:
//    Tim Coleman <tim@timcoleman.com>
//    Andrey Baboshin <andrey.baboshin@gmail.com>
//
// Copyright (C) Tim Coleman , 2003
// Copyright (C) Andrey Baboshin, 2016
//
// Licensed under the MIT/X11 License.
//

using System;
using System.Collections;
using System.Data.Common;
using System.Globalization;

namespace System.Data.OracleClient
{
    public sealed class OracleParameterCollection :
        DbParameterCollection
    {
		#region Fields

		readonly ArrayList list;

		#endregion // Fields

		#region Constructors

		public OracleParameterCollection ()
		{
			list = new ArrayList ();
		}

		#endregion // Constructors

        public override int Count
        {
            get { return list.Count; }
        }

        public override object SyncRoot
        {
            get
            {
                return this;
            }
        }

		static Exception CreateParameterNullException ()
		{
			string msg = string.Format (CultureInfo.InvariantCulture,
				"Only non-null {0} instances are valid for " +
				"{1}.", typeof (OracleParameter).Name,
				typeof (OracleParameterCollection).Name);
			return new ArgumentNullException ("value", msg);
		}

		static void AssertParameterValid (object value)
		{
			if (value == null)
				throw CreateParameterNullException ();

			if (value is OracleParameter)
				return;

			string msg = string.Format (CultureInfo.InvariantCulture,
				"Only non-null {0} instances are valid for " +
				"the {1}, not {2} instances.",
				typeof (OracleParameter).Name,
				typeof (OracleParameterCollection).Name,
				value.GetType ().Name);
			throw new InvalidCastException (msg);
		}

        public override int Add(object value)
        {
            AssertParameterValid (value);

			Add ((OracleParameter) value);
			return IndexOf (value);
        }

		public OracleParameter Add (OracleParameter value)
		{
			if (value == null)
				throw CreateParameterNullException ();
			if (value.Container != null)
				throw new ArgumentException ("The OracleParameter specified in the value parameter is already added to this or another OracleParameterCollection.");
			value.Container = this;
			list.Add (value);
			return value;
		}

		public OracleParameter Add (string parameterName, object value)
		{
			return Add (new OracleParameter (parameterName, value));
		}

		public OracleParameter Add (string parameterName, OracleType dataType)
		{
			return Add (new OracleParameter (parameterName, dataType));
		}

		public OracleParameter Add (string parameterName, OracleType dataType, int size)
		{
			return Add (new OracleParameter (parameterName, dataType, size));
		}

		public OracleParameter Add (string parameterName, OracleType dataType, int size, string srcColumn)
		{
			return Add (new OracleParameter (parameterName, dataType, size, srcColumn));
		}

        public override void AddRange(Array values)
        {
            if (values == null)
				throw new ArgumentNullException ("values");

			foreach (object param in values)
				AssertParameterValid (param);

			foreach (OracleParameter param in values)
				Add (param);
        }

        public override void Clear()
        {
            foreach (OracleParameter param in list)
				param.Container = null;
			list.Clear ();
        }

        public override bool Contains(string value)
        {
			return (IndexOf (value) != -1);
        }

        public override bool Contains(object value)
        {
			return (IndexOf (value) != -1);
        }

        public override void CopyTo(Array array, int index)
        {
            list.CopyTo (array, index);
        }

        public override IEnumerator GetEnumerator()
        {
			return list.GetEnumerator ();
        }

        public override int IndexOf(string parameterName)
        {
            // case-sensitive lookup
			for (int i = 0; i < Count; i += 1) {
				OracleParameter param = (OracleParameter) list [i];
				if (string.Compare (param.ParameterName, parameterName, false/*, CultureInfo.CurrentCulture*/) == 0)
					return i;
			}

			// case-insensitive lookup
			for (int i = 0; i < Count; i += 1) {
				OracleParameter param = (OracleParameter) list [i];
				if (string.Compare (param.ParameterName, parameterName, true/*, CultureInfo.CurrentCulture*/) == 0)
					return i;
			}

			return -1;
        }

        public override int IndexOf(object value)
        {
            if (value != null)
				AssertParameterValid (value);

			for (int i = 0; i < Count; i += 1)
				if (list [i] == value)
					return i;
			return -1;
        }

		static Exception ParameterAlreadyOwnedException ()
		{
			string msg = string.Format (CultureInfo.InvariantCulture,
				"The specified {0} is already owned by this " +
				"or another {1}.", typeof (OracleParameter).Name,
				typeof (OracleParameterCollection).Name);
			throw new ArgumentException (msg);
		}

        public override void Insert(int index, object value)
        {
            AssertParameterValid (value);

			OracleParameter new_value = (OracleParameter) value;

			if (new_value.Container != null) {
				if (new_value.Container != this)
					throw ParameterAlreadyOwnedException ();
				if (IndexOf (value) != -1)
					throw ParameterAlreadyOwnedException ();
			}

			list.Insert (index, new_value);
			new_value.Container = this;
        }

		Exception ParameterNotOwnedException ()
		{
			throw new ArgumentException (string.Format (
				CultureInfo.InvariantCulture,
				"An {0} instance that is not contained " +
				"by this {1} cannot be removed.",
				typeof (OracleParameter).Name,
				this.GetType ().Name));
		}

		Exception ParameterNotOwnedException (string name)
		{
			throw new IndexOutOfRangeException (string.Format (
				CultureInfo.InvariantCulture,
				"{0} parameter '{1}' is not contained by " +
				"this {2}.", typeof (OracleParameter).Name,
				name, this.GetType ().Name));
		}

        public override void Remove(object value)
        {
            AssertParameterValid (value);

			int index = IndexOf (value);
			if (index == -1)
				throw ParameterNotOwnedException ();

			((OracleParameter) value).Container = null;
			list.RemoveAt (index);
        }

        public override void RemoveAt(string parameterName)
        {
            int index = IndexOf (parameterName);
			if (index == -1)
				throw ParameterNotOwnedException (parameterName);

			OracleParameter param = (OracleParameter) list [index];
			param.Container = null;
			list.RemoveAt (index);
        }

		void AssertIndex (int index)
		{
			if (index < 0 || index >= Count)
				throw new IndexOutOfRangeException (string.Format (
					CultureInfo.InvariantCulture, "Index {0} " +
					"is not valid for this {1}.", index,
					typeof (OracleParameterCollection).Name));
		}

        public override void RemoveAt(int index)
        {
            AssertIndex (index);

			OracleParameter param = (OracleParameter) list [index];
			param.Container = null;
			list.RemoveAt (index);
        }

		Exception ParameterNotFoundException (string name, int index)
		{
			string msg = string.Format (CultureInfo.InvariantCulture,
				"Index {0} is not valid for this {1}.",
				index, typeof (OracleParameterCollection).Name);
			throw new IndexOutOfRangeException (msg);
		}

        protected override DbParameter GetParameter(string parameterName)
        {
            int index = IndexOf (parameterName);
			if (index == -1)
				throw ParameterNotFoundException (parameterName, index);
			return (OracleParameter) list [index];
        }

        protected override DbParameter GetParameter(int index)
        {
            AssertIndex (index);
			return (OracleParameter) list [index];
        }

        protected override void SetParameter(string parameterName, DbParameter value)
        {
            int index = IndexOf (parameterName);
			if (index == -1)
				throw ParameterNotFoundException (parameterName, index);

			AssertParameterValid (value);

			OracleParameter new_value = (OracleParameter) value;
			OracleParameter old_value = (OracleParameter) list [index];


			if (new_value.Container != null) {
				if (new_value.Container != this)
					throw ParameterAlreadyOwnedException ();
				if (IndexOf (new_value) != index)
					throw ParameterAlreadyOwnedException ();
			}

			list [index] = new_value;
			new_value.Container = this;
			old_value.Container = null;
        }

        protected override void SetParameter(int index, DbParameter value)
        {
            AssertIndex (index);
			AssertParameterValid (value);

			OracleParameter new_value = (OracleParameter) value;
			OracleParameter old_value = (OracleParameter) list [index];


			if (new_value.Container != null) {
				if (new_value.Container != this)
					throw ParameterAlreadyOwnedException ();
				if (IndexOf (new_value) != index)
					throw ParameterAlreadyOwnedException ();
			}

			list [index] = new_value;
			new_value.Container = this;
			old_value.Container = null;
        }

		public
		new
		OracleParameter this [int index]
		{
			get {
				return (OracleParameter) GetParameter (index);
			}
			set {
				SetParameter (index, value);
			}
		}
    }
}
