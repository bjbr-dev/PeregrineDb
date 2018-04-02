namespace PeregrineDb
{
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;

    public struct SqlCommand
    {
        public SqlCommand(string text, Dictionary<string, object> parameters = null, CommandType? type = null)
        {
            this.Text = text;
            this.Parameters = parameters;
            this.Type = type;
        }

        public string Text { get; }

        public Dictionary<string, object> Parameters { get; }

        public CommandType? Type { get; }

        public override string ToString()
        {
            return this.Text + "\n\n" + string.Join("\n", this.Parameters.Select(kvp => $"[{kvp.Key}] = {kvp.Value}"));
        }
    }
}