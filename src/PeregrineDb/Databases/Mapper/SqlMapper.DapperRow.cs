namespace PeregrineDb.Databases.Mapper
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;

    internal static partial class SqlMapper
    {
        private sealed class DapperRow
            : System.Dynamic.IDynamicMetaObjectProvider
            , IDictionary<string, object>
        {
            private readonly DapperTable table;
            private object[] values;

            public DapperRow(DapperTable table, object[] values)
            {
                this.table = table ?? throw new ArgumentNullException(nameof(table));
                this.values = values ?? throw new ArgumentNullException(nameof(values));
            }

            private sealed class DeadValue
            {
                public static readonly DeadValue Default = new DeadValue();
                private DeadValue() { /* hiding constructor */ }
            }

            int ICollection<KeyValuePair<string, object>>.Count
            {
                get
                {
                    int count = 0;
                    for (int i = 0; i < this.values.Length; i++)
                    {
                        if (!(this.values[i] is DeadValue)) count++;
                    }
                    return count;
                }
            }

            public bool TryGetValue(string key, out object value)
            {
                var index = this.table.IndexOfName(key);
                if (index < 0)
                { // doesn't exist
                    value = null;
                    return false;
                }
                // exists, **even if** we don't have a value; consider table rows heterogeneous
                value = index < this.values.Length ? this.values[index] : null;
                if (value is DeadValue)
                { // pretend it isn't here
                    value = null;
                    return false;
                }
                return true;
            }

            public override string ToString()
            {
                var sb = GetStringBuilder().Append("{DapperRow");
                foreach (var kv in this)
                {
                    var value = kv.Value;
                    sb.Append(", ").Append(kv.Key);
                    if (value != null)
                    {
                        sb.Append(" = '").Append(kv.Value).Append('\'');
                    }
                    else
                    {
                        sb.Append(" = NULL");
                    }
                }

                return sb.Append('}').__ToStringRecycle();
            }

            System.Dynamic.DynamicMetaObject System.Dynamic.IDynamicMetaObjectProvider.GetMetaObject(
                System.Linq.Expressions.Expression parameter)
            {
                return new DapperRowMetaObject(parameter, System.Dynamic.BindingRestrictions.Empty, this);
            }

            public IEnumerator<KeyValuePair<string, object>> GetEnumerator()
            {
                var names = this.table.FieldNames;
                for (var i = 0; i < names.Length; i++)
                {
                    object value = i < this.values.Length ? this.values[i] : null;
                    if (!(value is DeadValue))
                    {
                        yield return new KeyValuePair<string, object>(names[i], value);
                    }
                }
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return this.GetEnumerator();
            }

            #region Implementation of ICollection<KeyValuePair<string,object>>

            void ICollection<KeyValuePair<string, object>>.Add(KeyValuePair<string, object> item)
            {
                IDictionary<string, object> dic = this;
                dic.Add(item.Key, item.Value);
            }

            void ICollection<KeyValuePair<string, object>>.Clear()
            { // removes values for **this row**, but doesn't change the fundamental table
                for (int i = 0; i < this.values.Length; i++)
                    this.values[i] = DeadValue.Default;
            }

            bool ICollection<KeyValuePair<string, object>>.Contains(KeyValuePair<string, object> item)
            {
                return this.TryGetValue(item.Key, out object value) && Equals(value, item.Value);
            }

            void ICollection<KeyValuePair<string, object>>.CopyTo(KeyValuePair<string, object>[] array, int arrayIndex)
            {
                foreach (var kv in this)
                {
                    array[arrayIndex++] = kv; // if they didn't leave enough space; not our fault
                }
            }

            bool ICollection<KeyValuePair<string, object>>.Remove(KeyValuePair<string, object> item)
            {
                IDictionary<string, object> dic = this;
                return dic.Remove(item.Key);
            }

            bool ICollection<KeyValuePair<string, object>>.IsReadOnly => false;
            #endregion

            #region Implementation of IDictionary<string,object>

            bool IDictionary<string, object>.ContainsKey(string key)
            {
                int index = this.table.IndexOfName(key);
                if (index < 0 || index >= this.values.Length || this.values[index] is DeadValue) return false;
                return true;
            }

            void IDictionary<string, object>.Add(string key, object value)
            {
                this.SetValue(key, value, true);
            }

            bool IDictionary<string, object>.Remove(string key)
            {
                int index = this.table.IndexOfName(key);
                if (index < 0 || index >= this.values.Length || this.values[index] is DeadValue) return false;
                this.values[index] = DeadValue.Default;
                return true;
            }

            object IDictionary<string, object>.this[string key]
            {
                get { this.TryGetValue(key, out object val); return val; }
                set { this.SetValue(key, value, false); }
            }

            public object SetValue(string key, object value)
            {
                return this.SetValue(key, value, false);
            }

            private object SetValue(string key, object value, bool isAdd)
            {
                if (key == null) throw new ArgumentNullException(nameof(key));
                int index = this.table.IndexOfName(key);
                if (index < 0)
                {
                    index = this.table.AddField(key);
                }
                else if (isAdd && index < this.values.Length && !(this.values[index] is DeadValue))
                {
                    // then semantically, this value already exists
                    throw new ArgumentException("An item with the same key has already been added", nameof(key));
                }
                int oldLength = this.values.Length;
                if (oldLength <= index)
                {
                    // we'll assume they're doing lots of things, and
                    // grow it to the full width of the table
                    Array.Resize(ref this.values, this.table.FieldCount);
                    for (int i = oldLength; i < this.values.Length; i++)
                    {
                        this.values[i] = DeadValue.Default;
                    }
                }
                return this.values[index] = value;
            }

            ICollection<string> IDictionary<string, object>.Keys
            {
                get { return this.Select(kv => kv.Key).ToArray(); }
            }

            ICollection<object> IDictionary<string, object>.Values
            {
                get { return this.Select(kv => kv.Value).ToArray(); }
            }

            #endregion
        }
    }
}
