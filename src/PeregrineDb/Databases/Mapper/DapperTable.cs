namespace PeregrineDb.Databases.Mapper
{
    using System;
    using System.Collections.Generic;

    internal sealed class DapperTable
    {
        private string[] fieldNames;
        private readonly Dictionary<string, int> fieldNameLookup;

        internal string[] FieldNames => this.fieldNames;

        public DapperTable(string[] fieldNames)
        {
            this.fieldNames = fieldNames ?? throw new ArgumentNullException(nameof(fieldNames));

            this.fieldNameLookup = new Dictionary<string, int>(fieldNames.Length, StringComparer.Ordinal);
            // if there are dups, we want the **first** key to be the "winner" - so iterate backwards
            for (int i = fieldNames.Length - 1; i >= 0; i--)
            {
                string key = fieldNames[i];
                if (key != null) this.fieldNameLookup[key] = i;
            }
        }

        internal int IndexOfName(string name)
        {
            return name != null && this.fieldNameLookup.TryGetValue(name, out int result) ? result : -1;
        }

        internal int AddField(string name)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            if (this.fieldNameLookup.ContainsKey(name)) throw new InvalidOperationException("Field already exists: " + name);
            int oldLen = this.fieldNames.Length;
            Array.Resize(ref this.fieldNames, oldLen + 1); // yes, this is sub-optimal, but this is not the expected common case
            this.fieldNames[oldLen] = name;
            this.fieldNameLookup[name] = oldLen;
            return oldLen;
        }

        internal bool FieldExists(string key) => key != null && this.fieldNameLookup.ContainsKey(key);

        public int FieldCount => this.fieldNames.Length;
    }
}
