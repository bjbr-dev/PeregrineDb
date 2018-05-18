namespace PeregrineDb
{
    public struct SqlCommand
    {
        public SqlCommand(string commandText, object parameters = null)
        {
            this.CommandText = commandText;
            this.Parameters = parameters;
        }

        public string CommandText { get; }
        
        public object Parameters { get; }
    }
}