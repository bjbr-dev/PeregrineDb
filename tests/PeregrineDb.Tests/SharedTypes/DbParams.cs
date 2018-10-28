namespace PeregrineDb.Tests.SharedTypes
{
    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using Dapper;

    internal class DbParams
        : SqlMapper.IDynamicParameters, IEnumerable<IDbDataParameter>
    {
        private readonly List<IDbDataParameter> parameters = new List<IDbDataParameter>();

        public IEnumerator<IDbDataParameter> GetEnumerator()
        {
            return this.parameters.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public void Add(IDbDataParameter value)
        {
            this.parameters.Add(value);
        }

        void SqlMapper.IDynamicParameters.AddParameters(IDbCommand command, SqlMapper.Identity identity)
        {
            foreach (var parameter in this.parameters)
            {
                command.Parameters.Add(parameter);
            }
        }
    }
}