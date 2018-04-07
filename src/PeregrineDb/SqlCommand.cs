namespace PeregrineDb
{
    using System.Data;

    public struct SqlCommand
    {
        public SqlCommand(string commandText, object parameters = null, CommandType? commandType = null)
        {
            this.CommandText = commandText;
            this.Parameters = parameters;
            this.CommandType = commandType;
        }

        public string CommandText { get; }
        
        public object Parameters { get; }

        public CommandType? CommandType { get; }
    }
}