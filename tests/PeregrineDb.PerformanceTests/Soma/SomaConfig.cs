namespace PeregrineDb.PerformanceTests.Soma
{
    using System;
    using global::Soma.Core;

    internal class SomaConfig : MsSqlConfig
    {
        public override string ConnectionString => BenchmarkBase.ConnectionString;

        public override Action<PreparedStatement> Logger => noOp;

        private static readonly Action<PreparedStatement> noOp = x => { /* nope */ };
    }
}
