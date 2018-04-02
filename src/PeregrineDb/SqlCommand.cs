namespace PeregrineDb
{
    using System.Data;

    public struct SqlCommand
    {
        public SqlCommand(string text, object parameters = null, CommandType? type = null)
        {
            this.Text = text;
            this.Parameters = parameters;
            this.Type = type;
        }

        public string Text { get; }
        
        public object Parameters { get; }

        public CommandType? Type { get; }
    }
}