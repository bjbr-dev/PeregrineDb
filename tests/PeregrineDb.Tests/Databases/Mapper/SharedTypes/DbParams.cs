namespace PeregrineDb.Tests.Databases.Mapper.SharedTypes
{
    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using PeregrineDb.Databases.Mapper;
    using PeregrineDb.Mapping;

    internal class DbParams
        : IDynamicParameters, IEnumerable<IDbDataParameter>
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

        void IDynamicParameters.AddParameters(IDbCommand command, Identity identity)
        {
            foreach (var parameter in this.parameters)
            {
                command.Parameters.Add(parameter);
            }
        }
    }
}