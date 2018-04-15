namespace PeregrineDb.Databases.Mapper
{
    using System;
    using System.Collections.Generic;
    using System.Dynamic;
    using System.Linq.Expressions;
    using System.Reflection;

    internal sealed class DapperRowMetaObject
        : DynamicMetaObject
    {
        private static readonly MethodInfo getValueMethod = typeof(IDictionary<string, object>).GetProperty("Item").GetGetMethod();
        private static readonly MethodInfo setValueMethod = typeof(DapperRow).GetMethod("SetValue", new Type[] { typeof(string), typeof(object) });

        public DapperRowMetaObject(
            Expression expression,
            BindingRestrictions restrictions)
            : base(expression, restrictions)
        {
        }

        public DapperRowMetaObject(
            Expression expression,
            BindingRestrictions restrictions,
            object value
        )
            : base(expression, restrictions, value)
        {
        }

        private DynamicMetaObject CallMethod(
            MethodInfo method,
            Expression[] parameters
        )
        {
            return new DynamicMetaObject(
                Expression.Call(Expression.Convert(this.Expression, this.LimitType), method, parameters),
                BindingRestrictions.GetTypeRestriction(this.Expression, this.LimitType)
            );
        }

        public override DynamicMetaObject BindGetMember(GetMemberBinder binder)
        {
            var parameters = new Expression[]
                {
                    Expression.Constant(binder.Name)
                };

            return this.CallMethod(getValueMethod, parameters);
        }

        // Needed for Visual basic dynamic support
        public override DynamicMetaObject BindInvokeMember(InvokeMemberBinder binder, DynamicMetaObject[] args)
        {
            var parameters = new Expression[]
                {
                    Expression.Constant(binder.Name)
                };

            return this.CallMethod(getValueMethod, parameters);
        }

        public override DynamicMetaObject BindSetMember(SetMemberBinder binder, DynamicMetaObject value)
        {
            var parameters = new[]
                {
                    Expression.Constant(binder.Name),
                    value.Expression,
                };

            return this.CallMethod(setValueMethod, parameters);
        }
    }
}
