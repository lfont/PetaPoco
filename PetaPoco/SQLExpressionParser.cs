#region Copyright (c) 2008 by Jahmani Muigai Mwaura and Community
   /*--------------------------------------------------------------------------------------------------
    *  LinqToSql, a Linq to Sql parser for the .NET Platform
    *  by Jahmani Mwaura and community
    *  ------------------------------------------------------------------------------------------------
    *  Version: LGPL 2.1
    *  
    *  Software distributed under the License is distributed on an "AS IS" basis,
    *  WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
    *  for the specific language governing rights and limitations under the
    *  License.
    *  
    *  The Original Code is any part of this file that is not marked as a contribution.
    *  
    *  The Initial Developer of the Original Code is Jahmani Muigai Mwaura.
    *  Portions created by the Initial Developer are Copyright (C) 2008
    *  the Initial Developer. All Rights Reserved.
    *  
    *  Contributor(s): None.
    *--------------------------------------------------------------------------------------------------
    */
#endregion

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace PetaPoco
{
    public class SqlExpressionParser : ExpressionVisitor
    {
        private readonly SqlExpressionParser outerStatement;

        private readonly AggregateType aggregateType = AggregateType.None;

        private readonly StringBuilder sb = new StringBuilder();
        private object[] arguments;

        private readonly int indentLevel = -1;

        private readonly Queue<MethodCallExpression> queryableMethods = new Queue<MethodCallExpression>();

        private SelectHandler selectHandler;
        private JoinHandler joinHandler;
        private CrossJoinHandler crossJoinHandler;
        private WhereHandler whereHandler;
        private Stack<OrderByHandler> orderByHandlers = new Stack<OrderByHandler>();

        //private static ThreadSafeCache<string, SqlExpressionParser> parserCache =
        //    new ThreadSafeCache<string, SqlExpressionParser>();

        private SqlExpressionParser()
            : this(-1, null)
        {
        }

        private SqlExpressionParser(int indentLevel)
            : this(indentLevel, null)
        {
        }

        private SqlExpressionParser(int indentLevel, SqlExpressionParser outerStatement)
            : this(indentLevel, outerStatement, AggregateType.None)
        {
        }

        private SqlExpressionParser(int indentLevel, SqlExpressionParser outerStatement,
                                   AggregateType aggregateType)
        {

            this.indentLevel = indentLevel;
            this.outerStatement = outerStatement;
            this.aggregateType = aggregateType;
        }

        public static Sql TranslateExpression(Expression expression)
        {
            var parser = GetSqlExpressionParser(expression);
            string sql = parser.GetSQLStatement();
            object[] args = parser.GetSQLArguments();
            return new Sql(sql, args);
        }

        private static SqlExpressionParser GetSqlExpressionParser(Expression expression)
        {
            //var key = expression.ToString();

            //if (parserCache.ContainsKey(key))
            //{
            //    return parserCache[key];
            //}

            var sqlExpressionParser = new SqlExpressionParser();

            sqlExpressionParser.Translate(expression);

            //parserCache.TryAdd(key, sqlExpressionParser);

            //Debug.Print(sqlExpressionParser.GetSQLStatement());

            return sqlExpressionParser;
        }

        private string Translate(Expression expression)
        {
            if (expression.NodeType == ExpressionType.Constant &&
                (expression as ConstantExpression).Type != typeof(object))
            {
                return string.Empty;
            }

            if (sb.Length != 0)
            {
                return sb.ToString();
            }

            this.Visit(Evaluator.PartialEval(expression));

            EmitSelectStatement();

            Debug.Print(sb.ToString());

            return sb.ToString();
        }

        protected override Expression VisitMethodCall(MethodCallExpression m)
        {

            if (m.Method.DeclaringType == typeof(Queryable))
            {

                switch (m.Method.Name)
                {
                    case "Select":
                        Debug.Assert(selectHandler == null);
                        selectHandler =
                            SelectHandler.GetSelectHandler(indentLevel + 1, m, aggregateType);
                        if (selectHandler == null)
                        {
                            this.queryableMethods.Enqueue(m);
                        }
                        this.Visit(m.Arguments[0]);
                        break;
                    case "Join":
                        Debug.Assert(joinHandler == null);
                        Debug.Assert(crossJoinHandler == null);
                        joinHandler = JoinHandler.GetJoinHandler(this, indentLevel + 1, m);
                        break;
                    case "SelectMany":
                        Debug.Assert(crossJoinHandler == null);
                        Debug.Assert(joinHandler == null);
                        crossJoinHandler = CrossJoinHandler.GetCrossJoinHandler(this, indentLevel + 1, m);
                        break;
                    case "Where":
                        Debug.Assert(whereHandler == null);
                        int parameterBaseIndex = outerStatement == null ? 0 : outerStatement.ParameterCount;
                        whereHandler =
                            WhereHandler.GetWhereHandler(indentLevel + 1, m, parameterBaseIndex);
                        if (whereHandler == null)
                        {
                            this.queryableMethods.Enqueue(m);
                        }
                        this.Visit(m.Arguments[0]);
                        break;
                    case "OrderBy":
                    case "OrderByDescending":
                    case "ThenBy":
                    case "ThenByDescending":
                        var orderByHandler = OrderByHandler.GetOrderByHandler(indentLevel + 1, m);
                        if (orderByHandler == null)
                        {
                            this.queryableMethods.Enqueue(m);
                        }
                        else
                        {
                            orderByHandlers.Push(orderByHandler);
                        }
                        this.Visit(m.Arguments[0]);
                        break;
                    default:
                        queryableMethods.Enqueue(m);
                        this.Visit(m.Arguments[0]);
                        break;
                }
            }
            else
            {

                throw new NotSupportedException(string.Format("The method '{0}' is not supported", m.Method.Name));
            }

            return m;
        }

        private void EmitSelectStatement()
        {

            GetSelectClause();

            bool hasJoinClause = GetJoinClause() || GetCrossJoinClause();

            GetWhereClause(hasJoinClause);

            GetOrderByClause();
        }

        private void GetSelectClause()
        {

            InitSelectHandler();

            if (crossJoinHandler == null && joinHandler == null)
            {
                sb.Append(selectHandler.GetSelectClause(true));
                return;
            }

            if (crossJoinHandler == null)
            {
                sb.Append(joinHandler.ReplaceAliases(selectHandler.GetSelectClause(false)));
                return;
            }

            sb.Append(crossJoinHandler.ReplaceAliases(selectHandler.GetSelectClause(false)));
        }

        private void GetWhereClause(bool hasJoinClause)
        {
            if (whereHandler != null)
            {
                arguments = whereHandler.GetWhereArguments();

                if (hasJoinClause)
                {
                    ReplaceWhereClauseAliases();
                    return;
                }

                if (selectHandler != null)
                {
                    sb.Append(selectHandler.ReplaceAliases(whereHandler.GetWhereClause(false)));
                    return;
                }

                Debug.Assert(hasJoinClause || selectHandler != null);
            }
        }

        private void ReplaceWhereClauseAliases()
        {

            Debug.Assert(joinHandler != null || crossJoinHandler != null);

            if (joinHandler != null)
            {
                sb.Append(joinHandler.ReplaceAliases(whereHandler.GetWhereClause(false)));
                return;
            }

            sb.Append(crossJoinHandler.ReplaceAliases(whereHandler.GetWhereClause(false)));
        }

        private void GetOrderByClause()
        {

            if (IsTopLevelOrderBy())
            {

                EmitOrderBy();

                sb.Append(Environment.NewLine);

                return;
            }
            else
            {
                LiftOrderByClause();
            }
        }

        private bool IsTopLevelOrderBy()
        {
            return orderByHandlers.Count > 0 && outerStatement == null;
        }

        private void EmitOrderBy()
        {

            var orderByClauses = from handler in orderByHandlers
                                 select handler.GetOrderByClause();

            var orderByClause = string.Join(", ", orderByClauses.ToArray());

            if (joinHandler == null && crossJoinHandler == null)
            {
                Debug.Assert(selectHandler != null);
                sb.Append("ORDER BY " + selectHandler.ReplaceAliases(orderByClause));
                return;
            }

            if (crossJoinHandler == null)
            {
                sb.Append("ORDER BY " + joinHandler.ReplaceAliases(orderByClause));
                return;
            }

            sb.Append("ORDER BY " + crossJoinHandler.ReplaceAliases(orderByClause));
        }

        private void LiftOrderByClause()
        {

            var nestedOrderByClauses = from handler in orderByHandlers
                                       select handler;

            foreach (var ordering in nestedOrderByClauses)
            {
                outerStatement.orderByHandlers.Push(ordering);
            }

        }

        private bool GetJoinClause()
        {

            if (joinHandler != null)
            {
                sb.Append(joinHandler.GetJoinClause());
                return true;
            }
            return false;
        }

        private bool GetCrossJoinClause()
        {

            if (crossJoinHandler != null)
            {
                sb.Append(crossJoinHandler.GetCrossJoinClause());
                return true;
            }

            return false;
        }

        private void GetTableAlias()
        {
            if (joinHandler == null && crossJoinHandler == null)
            {
                sb.Append(" AS " + GetTableAlias(indentLevel));
                sb.Append(Environment.NewLine);
            }
        }

        private static bool IsAggregateMethod(MethodCallExpression m)
        {

            if (m.Method.DeclaringType != typeof(Queryable) &&
                m.Method.DeclaringType != typeof(Enumerable))
            {

                return false;
            }

            switch (m.Method.Name)
            {

                case "Count":
                case "Average":
                case "Max":
                case "Min":
                case "Sum":
                    return true;
                default:
                    return false;
            }
        }

        private static string GetTableAlias(int indentLevel)
        {
            return "t" + indentLevel.ToString();
        }

        private void InitSelectHandler()
        {

            if (selectHandler != null)
            {
                return;
            }

            if (joinHandler != null)
            {
                selectHandler = SelectHandler.GetSelectHandler(indentLevel + 1,
                                    QueryableMethodsProvider.GetSelectCall(joinHandler.Selector),
                                    AggregateType.None);
                return;
            }

            if (crossJoinHandler != null)
            {
                selectHandler = SelectHandler.GetSelectHandler(indentLevel + 1,
                                    QueryableMethodsProvider.GetSelectCall(crossJoinHandler.Selector),
                                    AggregateType.None);
                return;
            }

            Type returnType = GetReturnType();

            if (returnType == null)
            {
                throw new InvalidOperationException("Cannot translate statement");
            }

            selectHandler = SelectHandler.GetSelectHandler(indentLevel + 1, returnType);
        }

        private Type GetReturnType()
        {

            if (whereHandler != null)
            {
                return whereHandler.ReturnType;
            }

            if (orderByHandlers.Count != 0)
            {
                return orderByHandlers.Peek().ReturnType;
            }

            return null;
        }

        private string GetTableName()
        {
            Debug.Assert(selectHandler != null);

            return GetTableName(selectHandler.TableType);
        }

        private string GetSQLStatement()
        {

            Debug.Assert(sb.Length != 0);
            return sb.ToString();
        }

        private object[] GetSQLArguments()
        {
            return arguments;
        }

        private int ParameterCount
        {
            get
            {
                if (whereHandler != null)
                {
                    return whereHandler.ParameterCount;
                }
                return 0;
            }
        }

        private static string GetIndentation(int indentLevel)
        {

            StringBuilder sb = new StringBuilder(indentLevel);

            for (int i = 0; i < indentLevel; i++)
            {
                sb.Append("\t");
            }

            return sb.ToString();
        }

        private static Expression StripQuotes(Expression e)
        {

            while (e.NodeType == ExpressionType.Quote)
            {
                e = ((UnaryExpression)e).Operand;
            }

            return e;
        }

        private static LambdaExpression GetLambdaExpression(Expression expression)
        {

            var selectorLambda = StripQuotes(expression) as LambdaExpression;

            if (selectorLambda == null)
            {

                var constantValue = (ConstantExpression)expression;

                selectorLambda = (LambdaExpression)constantValue.Value;

                Debug.Assert(selectorLambda != null);
            }

            return selectorLambda;
        }

        private static string GetTableName(Type tableType)
        {
            return ((TableNameAttribute)
                      tableType.GetCustomAttributes(typeof(TableNameAttribute), false)[0]).Value;
        }

        private enum AggregateType
        {
            None,
            Count,
            Sum,
            Min,
            Max,
            Average
        }

        private class SelectHandler
        {

            private readonly int indentLevel;

            private readonly AggregateType aggregateType = AggregateType.None;

            private readonly Type returnType = null;

            private readonly Type tableType = null;

            private readonly LambdaExpressionHandler lambdaHandler = null;

            private readonly LambdaExpression selector = null;

            private readonly string selectorExpression = null;

            public int IndentationLevel
            {
                get
                {
                    return indentLevel;
                }
            }

            public Type ReturnType
            {
                get
                {
                    return returnType;
                }
            }

            public Type TableType
            {
                get
                {
                    return tableType;
                }
            }

            private SelectHandler(int indentLevel,
                                  MethodCallExpression expression, AggregateType aggregateType)
            {

                this.indentLevel = indentLevel;

                this.aggregateType = aggregateType;

                selector = GetLambdaExpression(expression.Arguments[1]);

                returnType = selector.Type.GetGenericArguments()[1];

                tableType = selector.Parameters[0].Type;

                lambdaHandler = new LambdaExpressionHandler(indentLevel, selector);

                selectorExpression = lambdaHandler.GetExpressionAsString(true).ToString();
            }

            private SelectHandler(int indentLevel, Type returnType) :
                this(indentLevel,
                     QueryableMethodsProvider.GetSelectCall(returnType), AggregateType.None)
            {
            }

            public static SelectHandler GetSelectHandler(int indentLevel,
                                                         MethodCallExpression expression,
                                                         AggregateType aggregateType)
            {

                Debug.Assert(expression.Method.Name == "Select");
                Debug.Assert(expression.Arguments.Count == 2);
                Debug.Assert(expression.Arguments[0].Type.GetGenericArguments().Length == 1);

                var selector = GetLambdaExpression(expression.Arguments[1]).Parameters[0];

                if (selector.Type.Name == "IGrouping`2")
                {
                    return null;
                }

                SelectHandler selectHandler = new SelectHandler(indentLevel,
                                                                expression, aggregateType);

                return selectHandler;
            }

            public static SelectHandler GetSelectHandler(int indentLevel, Type returnType)
            {
                return new SelectHandler(indentLevel, returnType);
            }

            public string GetSelectClause(bool emitTableAlias)
            {

                StringBuilder sb = new StringBuilder();

                sb.Append(GetIndentation(indentLevel));

                sb.Append("SELECT ");

                sb.Append(GetFields(GetTableAlias(indentLevel)));

                sb.Append(Environment.NewLine);

                sb.Append(GetIndentation(indentLevel));

                sb.Append("FROM ");

                EmitAlias(emitTableAlias, sb);

                sb.Append(Environment.NewLine);

                return sb.ToString();
            }

            private void EmitAlias(bool emitTableAlias, StringBuilder sb)
            {

                if (emitTableAlias)
                {

                    sb.Append(GetTableName(tableType));

                    sb.Append(" AS " + GetTableAlias(indentLevel));
                }
            }

            private string GetFields(string tableAlias)
            {

                var accessedFields = lambdaHandler.GetAccessedFields();

                string fieldList = null;

                if (accessedFields.Length != 0)
                {
                    fieldList = GetFieldsFromSelector(accessedFields);
                }
                else
                {
                    fieldList = GetFieldsFromReturnType(tableAlias);
                }

                var aggregateExpression = ReplaceAliases(selectorExpression);

                switch (aggregateType)
                {
                    case AggregateType.None:
                        return fieldList;
                    case AggregateType.Average:
                        return "Avg(" + aggregateExpression + ")";
                    case AggregateType.Count:
                        return "Count(*) ";
                    case AggregateType.Max:
                        return "Max(" + aggregateExpression + ")";
                    case AggregateType.Min:
                        return "Min(" + aggregateExpression + ")";
                    case AggregateType.Sum:
                        return "Sum(" + aggregateExpression + ")";
                    default:
                        throw new InvalidOperationException();
                }
            }

            private string GetFieldsFromReturnType(string tableAlias)
            {

                var separator = string.Empty;

                if (tableAlias != string.Empty)
                {
                    separator = ".";
                }

                // Hack. Property may not correspond to a column in a table
                return string.Join(", ", (from property in returnType.GetProperties()
                                          where property.PropertyType.IsValueType ||
                                                property.PropertyType == typeof(string)
                                          orderby property.Name
                                          select tableAlias + separator + property.Name)
                                      .ToArray());
            }

            private string GetFieldsFromSelector(string[] fields)
            {

                return ReplaceAliases(string.Join(", ", fields));
            }

            public string ReplaceAliases(string expression)
            {

                StringBuilder sb = new StringBuilder(lambdaHandler.ReplaceAliases(expression));

                sb.Replace(tableType.GUID.ToString(),
                           GetTableAlias(indentLevel).ToString());

                sb.Replace(returnType.GUID.ToString(),
                           GetTableAlias(indentLevel).ToString());

                return sb.ToString();
            }

            public LambdaExpression Selector
            {
                get
                {
                    Debug.Assert(selector != null);
                    return selector;
                }
            }
        }

        private class WhereHandler
        {

            private readonly Type returnType = null;

            private readonly int indentLevel;

            private readonly LambdaExpressionHandler lambdaHandler = null;

            public Type ReturnType
            {
                get
                {
                    return returnType;
                }
            }

            public int ParameterCount
            {
                get
                {
                    return lambdaHandler.ParameterCount;
                }
            }

            private WhereHandler(int indentLevel,
                                 MethodCallExpression expression,
                                 int parameterBaseIndex)
            {

                this.indentLevel = indentLevel;

                returnType = expression.Arguments[0].Type.GetGenericArguments()[0];

                Expression e = StripQuotes(expression.Arguments[1]);

                LambdaExpression lambda = GetLambdaExpression(expression.Arguments[1]);

                lambdaHandler = new LambdaExpressionHandler(indentLevel,
                                                            lambda,
                                                            parameterBaseIndex);
            }

            public static WhereHandler GetWhereHandler(int indentLevel,
                                                       MethodCallExpression expression,
                                                       int parameterBaseIndex)
            {

                Debug.Assert(expression.Method.Name == "Where");

                Debug.Assert(expression.Arguments.Count == 2);

                Debug.Assert(expression.Arguments[0].Type.GetGenericArguments().Length == 1);

                var selector = GetLambdaExpression(expression.Arguments[1]).Parameters[0];

                if (selector.Type.Name == "IGrouping`2")
                {
                    return null;
                }

                return new WhereHandler(indentLevel, expression, parameterBaseIndex);
            }

            public string GetWhereClause(bool replaceAliases)
            {

                return GetIndentation(indentLevel) + "WHERE " +
                       lambdaHandler.GetExpressionAsString(replaceAliases)
                       + Environment.NewLine;
            }

            public object[] GetWhereArguments()
            {
                return lambdaHandler.GetExpressionConstants();
            }
        }

        private class JoinHandler
        {

            private readonly SqlExpressionParser outerStatement;

            private readonly SqlExpressionParser leftStatement;

            private readonly SqlExpressionParser rightStatement;

            private readonly LambdaExpression selector = null;

            private readonly LambdaExpressionHandler leftKeySelector = null;

            private readonly LambdaExpressionHandler rightKeySelector = null;

            private readonly Type leftReturnType = null;

            private readonly Type rightReturnType = null;

            private readonly int indentLevel;

            public LambdaExpression Selector
            {
                get
                {
                    return selector;
                }
            }

            private JoinHandler(SqlExpressionParser outerStatement,
                                int indentLevel,
                                MethodCallExpression expression)
            {

                this.outerStatement = outerStatement;

                this.indentLevel = indentLevel;

                selector = GetLambdaExpression(expression.Arguments[4]);

                leftStatement = new SqlExpressionParser(indentLevel + 1, outerStatement);

                leftStatement.Translate(GetLeftSourceExpression(expression));

                rightStatement = new SqlExpressionParser(indentLevel + 1, outerStatement,
                                                         AggregateType.None);

                rightStatement.Translate(GetRightSourceExpression(expression));

                leftKeySelector = new LambdaExpressionHandler(indentLevel,
                                            (LambdaExpression)StripQuotes(expression.Arguments[2]));

                rightKeySelector = new LambdaExpressionHandler(indentLevel,
                                            (LambdaExpression)StripQuotes(expression.Arguments[3]));

                leftReturnType = leftStatement.selectHandler.ReturnType;

                rightReturnType = rightStatement.selectHandler.ReturnType;
            }

            private static Expression GetLeftSourceExpression(MethodCallExpression expression)
            {
                switch (expression.Arguments[0].NodeType)
                {
                    case ExpressionType.Call:
                        return expression.Arguments[0];
                    case ExpressionType.Constant:
                        return GetSourceExpression(expression.Arguments[0]);
                    default:
                        throw new ArgumentException("Node type not supported " + expression.Arguments[0].NodeType);
                }
            }

            private static MethodCallExpression GetRightSourceExpression(MethodCallExpression expression)
            {

                return GetSourceExpression(expression.Arguments[1]);
            }

            private static MethodCallExpression GetSourceExpression(Expression source)
            {

                Debug.Assert(source.Type.GetGenericArguments().Length == 1);

                return QueryableMethodsProvider.GetSelectCall(source.Type.GetGenericArguments()[0]);
            }

            public static JoinHandler GetJoinHandler(SqlExpressionParser outerStatement,
                                                     int indentLevel,
                                                     MethodCallExpression expression)
            {

                Debug.Assert(expression.Method.Name == "Join");

                Debug.Assert(expression.Arguments.Count == 5);

                return new JoinHandler(outerStatement, indentLevel, expression);
            }

            public string GetJoinClause()
            {

                StringBuilder sb = new StringBuilder();

                /*sb.Append(GetIndentation(indentLevel));
                sb.Append("(");
                sb.Append(Environment.NewLine);
                sb.Append(leftStatement.GetSQLStatement());
                sb.Append(Environment.NewLine);
                sb.Append(GetIndentation(indentLevel));
                sb.Append(") As leftStatement");
                sb.Append(Environment.NewLine);
                sb.Append(GetIndentation(indentLevel));
                sb.Append(" JOIN ");
                sb.Append(Environment.NewLine);
                sb.Append(GetIndentation(indentLevel));
                sb.Append("(");
                sb.Append(Environment.NewLine);
                sb.Append(rightStatement.GetSQLStatement());
                sb.Append(Environment.NewLine);
                sb.Append(GetIndentation(indentLevel));
                sb.Append(") As rightStatement");
                sb.Append(Environment.NewLine);
                sb.Append(GetIndentation(indentLevel));
                sb.Append(" ON ");
                sb.Append(GetJoinExpression(leftKeySelector, leftReturnType, "left") + " = " +
                          GetJoinExpression(rightKeySelector, rightReturnType, "right"));
                sb.Append(Environment.NewLine);*/

                sb.Append(GetIndentation(indentLevel));
                sb.Append(leftStatement.GetTableName());
                sb.Append(Environment.NewLine);
                sb.Append(GetIndentation(indentLevel));
                sb.Append(" INNER JOIN ");
                sb.Append(Environment.NewLine);
                sb.Append(GetIndentation(indentLevel));
                sb.Append(rightStatement.GetTableName());
                sb.Append(Environment.NewLine);
                sb.Append(GetIndentation(indentLevel));
                sb.Append(" ON ");
                sb.Append(GetJoinExpression(leftKeySelector, leftReturnType,
                                            leftStatement.GetTableName()) + " = " +
                          GetJoinExpression(rightKeySelector, rightReturnType,
                                            rightStatement.GetTableName()));
                sb.Append(Environment.NewLine);
                return sb.ToString();
            }

            private StringBuilder GetJoinExpression(LambdaExpressionHandler handler,
                                                    Type type,
                                                    string tableName)
            {

                return handler.GetExpressionAsString(false)
                              .Replace(type.GUID.ToString(), tableName);
            }

            public string ReplaceAliases(string expression)
            {

                StringBuilder sb = new StringBuilder(expression);

                sb.Replace(leftReturnType.GUID.ToString(), leftStatement.GetTableName());

                sb.Replace(rightReturnType.GUID.ToString(), rightStatement.GetTableName());

                sb.Replace(selector.Body.Type.GUID.ToString() + ".", string.Empty);

                sb.Replace(GetTableAlias(indentLevel) + ".", string.Empty);

                return sb.ToString();
            }

        }

        private class CrossJoinHandler
        {

            private readonly SqlExpressionParser outerStatement;

            private readonly SqlExpressionParser leftStatement;

            private readonly SqlExpressionParser rightStatement;

            private readonly LambdaExpression selector;

            private readonly Type leftReturnType = null;

            private readonly Type rightReturnType = null;

            private readonly int indentLevel;

            public LambdaExpression Selector
            {
                get
                {
                    return selector;
                }
            }

            private CrossJoinHandler(SqlExpressionParser outerStatement,
                                     int indentLevel,
                                     MethodCallExpression expression)
            {

                this.outerStatement = outerStatement;

                this.indentLevel = indentLevel;

                selector = GetLambdaExpression(expression.Arguments.Last());

                leftStatement = new SqlExpressionParser(indentLevel + 1, outerStatement);

                leftStatement.Translate(GetLeftSourceExpression(expression));

                rightStatement = new SqlExpressionParser(indentLevel + 1, outerStatement,
                                                         AggregateType.None);

                rightStatement.Translate(GetRightSourceExpression(expression));

                leftReturnType = leftStatement.selectHandler.ReturnType;

                rightReturnType = rightStatement.selectHandler.ReturnType;
            }

            private static Expression GetLeftSourceExpression(MethodCallExpression expression)
            {
                switch (expression.Arguments[0].NodeType)
                {
                    case ExpressionType.Call:
                        return expression.Arguments[0];
                    case ExpressionType.Constant:
                        return GetSourceExpression(expression.Arguments[0].Type.GetGenericArguments()[0]);
                    default:
                        throw new ArgumentException("Node type not supported " + expression.Arguments[0].NodeType);
                }
            }

            private static MethodCallExpression GetRightSourceExpression(MethodCallExpression expression)
            {

                var rightSource = ((LambdaExpression)StripQuotes(expression.Arguments[2]))
                                        .Parameters[1];

                return GetSourceExpression(rightSource.Type);
            }

            private static MethodCallExpression GetSourceExpression(Type sourceType)
            {
                return QueryableMethodsProvider.GetSelectCall(sourceType);
            }

            public static CrossJoinHandler GetCrossJoinHandler(SqlExpressionParser outerStatement,
                                                               int indentLevel,
                                                               MethodCallExpression expression)
            {

                Debug.Assert(expression.Method.Name == "SelectMany");

                Debug.Assert(expression.Arguments.Count == 3);

                return new CrossJoinHandler(outerStatement, indentLevel, expression);
            }

            public string GetCrossJoinClause()
            {

                StringBuilder sb = new StringBuilder();

                sb.Append(GetIndentation(indentLevel));
                sb.Append("(");
                sb.Append(Environment.NewLine);
                sb.Append(leftStatement.GetSQLStatement());
                sb.Append(Environment.NewLine);
                sb.Append(GetIndentation(indentLevel));
                sb.Append(") As leftStatement");
                sb.Append(Environment.NewLine);
                sb.Append(GetIndentation(indentLevel));
                sb.Append(" CROSS JOIN ");
                sb.Append(Environment.NewLine);
                sb.Append(GetIndentation(indentLevel));
                sb.Append("(");
                sb.Append(Environment.NewLine);
                sb.Append(rightStatement.GetSQLStatement());
                sb.Append(Environment.NewLine);
                sb.Append(GetIndentation(indentLevel));
                sb.Append(") As rightStatement");
                sb.Append(Environment.NewLine);

                return sb.ToString();
            }

            public string ReplaceAliases(string expression)
            {

                StringBuilder sb = new StringBuilder(expression);

                sb.Replace(leftReturnType.GUID.ToString(), "leftStatement");

                sb.Replace(rightReturnType.GUID.ToString(), "rightStatement");

                sb.Replace(selector.Body.Type.GUID.ToString() + ".", string.Empty);

                sb.Replace(GetTableAlias(indentLevel) + ".", string.Empty);

                return sb.ToString();
            }

        }

        private class OrderByHandler
        {

            private readonly Type returnType = null;

            private readonly int indentLevel;

            private readonly LambdaExpressionHandler lambdaHandler = null;

            private readonly string orderByDirection = string.Empty;

            public Type ReturnType
            {
                get
                {
                    return returnType;
                }
            }

            private OrderByHandler(int indentLevel,
                                   MethodCallExpression expression)
            {

                if (expression.Method.Name == "OrderByDescending" || expression.Method.Name == "ThenByDescending")
                {
                    orderByDirection = "Desc";
                }

                this.indentLevel = indentLevel;

                returnType = expression.Arguments[0].Type.GetGenericArguments()[0];

                Expression e = StripQuotes(expression.Arguments[1]);

                lambdaHandler = new LambdaExpressionHandler(indentLevel,
                                            (LambdaExpression)StripQuotes(expression.Arguments[1]));

            }

            public static OrderByHandler GetOrderByHandler(int indentLevel,
                                                           MethodCallExpression expression)
            {

                Debug.Assert(expression.Method.Name == "OrderBy" ||
                             expression.Method.Name == "OrderByDescending" ||
                             expression.Method.Name == "ThenBy" ||
                             expression.Method.Name == "ThenByDescending");

                Debug.Assert(expression.Arguments.Count == 2);

                Debug.Assert(expression.Arguments[0].Type.GetGenericArguments().Length == 1);

                var selector = GetLambdaExpression(expression.Arguments[1]).Parameters[0];

                if (selector.Type.Name == "IGrouping`2")
                {
                    return null;
                }

                return new OrderByHandler(indentLevel, expression);
            }

            public string GetOrderByClause()
            {

                return lambdaHandler.GetExpressionAsString(false) + " " + orderByDirection;
            }

        }

        private class LambdaExpressionHandler : ExpressionVisitor
        {

            private readonly LambdaExpression lambdaExpression = null;

            private readonly Guid lambaExpressionId = Guid.Empty;

            private readonly int indentLevel;

            private readonly Dictionary<string, string> aliases = new Dictionary<string, string>();

            private readonly List<string> accessedColumns = new List<string>();

            private readonly Stack<Expression> terms = new Stack<Expression>();

            private StringBuilder sb = new StringBuilder();

            private List<object> constants;

            private int parameterCount = 0;

            public int ParameterCount
            {
                get
                {
                    return parameterCount;
                }
            }

            public LambdaExpressionHandler(int indentLevel, LambdaExpression lambdaExpression)
                : this(indentLevel, lambdaExpression, 0)
            {
            }

            public LambdaExpressionHandler(int indentLevel, LambdaExpression lambdaExpression,
                                           int parameterBaseIndex)
            {

                this.indentLevel = indentLevel;

                this.lambdaExpression = lambdaExpression;

                this.parameterCount = parameterBaseIndex;

                lambaExpressionId = lambdaExpression.Body.Type.GUID;

                this.Visit(lambdaExpression);

                GetExpressionAsString(false);
            }

            protected override Expression VisitMethodCall(MethodCallExpression m)
            {

                this.Visit(m.Object);

                this.VisitExpressionList(m.Arguments);

                terms.Push(m);

                return m;
            }

            protected override Expression VisitUnary(UnaryExpression u)
            {

                if (u.NodeType == ExpressionType.Quote)
                {
                    return this.Visit(StripQuotes(u));
                }
                terms.Push(u);

                return u;
            }

            protected override Expression VisitBinary(BinaryExpression b)
            {

                this.Visit(b.Left);

                this.Visit(b.Right);

                terms.Push(b);

                return b;
            }

            protected override Expression VisitConstant(ConstantExpression c)
            {
                terms.Push(c);
                return c;
            }

            protected override Expression VisitParameter(ParameterExpression p)
            {

                terms.Push(p);

                return p;

            }

            protected override Expression VisitMemberAccess(MemberExpression m)
            {

                // lambdaExpression.Parameters[0].Type may look like
                // <>f__AnonymousType0`2[[Order],[Customer]]
                // as a result of a join 
                // we need to check for generic parameters
                var genericParameters = lambdaExpression.Parameters[0].Type.GetGenericArguments();

                if ((m.Member.DeclaringType == lambdaExpression.Parameters[0].Type ||
                     genericParameters.Contains(m.Member.DeclaringType))
                    && (m.Type.IsValueType || m.Type == typeof(string)))
                {
                    accessedColumns.Add(GetHashedName(m));
                }
                terms.Push(m);
                return m;
            }

            protected override NewExpression VisitNew(NewExpression newExpression)
            {

                foreach (var argument in newExpression.Arguments)
                {
                    this.Visit(argument);
                }

                terms.Push(newExpression);

                return newExpression;
            }

            protected override ElementInit VisitElementInitializer(ElementInit initializer)
            {

                throw new InvalidOperationException();

            }

            protected override Expression VisitTypeIs(TypeBinaryExpression b)
            {

                throw new InvalidOperationException();


            }

            protected override Expression VisitConditional(ConditionalExpression c)
            {

                Debug.Assert(c.Test as ConstantExpression != null);

                if ((bool)(c.Test as ConstantExpression).Value == true)
                {
                    terms.Push(c.IfTrue);
                    return c.IfTrue;
                }

                terms.Push(c.IfFalse);
                return c.IfFalse;
            }

            protected override MemberMemberBinding VisitMemberMemberBinding(MemberMemberBinding binding)
            {

                throw new InvalidOperationException();

            }

            protected override MemberListBinding VisitMemberListBinding(MemberListBinding binding)
            {

                throw new InvalidOperationException();

            }

            protected override IEnumerable<MemberBinding> VisitBindingList(ReadOnlyCollection<MemberBinding> original)
            {

                throw new InvalidOperationException();

            }

            protected override IEnumerable<ElementInit> VisitElementInitializerList(ReadOnlyCollection<ElementInit> original)
            {

                throw new InvalidOperationException();

            }

            protected override Expression VisitMemberInit(MemberInitExpression init)
            {

                throw new InvalidOperationException();

            }

            protected override Expression VisitListInit(ListInitExpression init)
            {

                throw new InvalidOperationException();

            }

            protected override Expression VisitNewArray(NewArrayExpression na)
            {

                throw new InvalidOperationException();

            }

            protected override Expression VisitInvocation(InvocationExpression iv)
            {

                throw new InvalidOperationException();

            }

            public StringBuilder GetExpressionAsString(bool replaceAliases)
            {
                EvaluateTerms();

                var result = sb.ToString();

                return new StringBuilder(ReplaceAliases(result, replaceAliases));
            }

            public object[] GetExpressionConstants()
            {
                EvaluateTerms();

                return constants.ToArray();
            }

            private void EvaluateTerms()
            {
                if (sb.Length > 0)
                {
                    // terms have already been evaluated
                    return;
                }

                Debug.Assert(terms.Count != 0);

                constants = new List<object>();

                while (terms.Count > 0)
                {
                    GetExpression();

                    if (terms.Count == 1 && terms.Peek().NodeType == ExpressionType.Constant)
                    {
                        break;
                    }

                    if (terms.Count > 1 && terms.Peek().NodeType == ExpressionType.Constant)
                    {
                        GetOperandValue();
                    }
                }

                sb = new StringBuilder((terms.Pop() as ConstantExpression).Value.ToString());
            }

            private void GetExpression()
            {

                var op = StripQuotes(terms.Pop());

                switch (op.NodeType)
                {
                    case ExpressionType.And:
                    case ExpressionType.AndAlso:
                        GetBinaryOperation(" AND ");
                        break;
                    case ExpressionType.Or:
                    case ExpressionType.OrElse:
                        GetBinaryOperation(" OR ");
                        break;
                    case ExpressionType.Equal:
                        GetBinaryOperation(" = ");
                        break;
                    case ExpressionType.NotEqual:
                        GetBinaryOperation(" <> ");
                        break;
                    case ExpressionType.LessThan:
                        GetBinaryOperation(" < ");
                        break;
                    case ExpressionType.LessThanOrEqual:
                        GetBinaryOperation(" <= ");
                        break;
                    case ExpressionType.GreaterThan:
                        GetBinaryOperation(" > ");
                        break;
                    case ExpressionType.GreaterThanOrEqual:
                        GetBinaryOperation(" >= ");
                        break;
                    /*case ExpressionType.ExclusiveOr:*/
                    case ExpressionType.Add:
                        GetBinaryOperation(" + ");
                        break;
                    case ExpressionType.Subtract:
                        GetBinaryOperation(" - ");
                        break;
                    case ExpressionType.Multiply:
                        GetBinaryOperation(" * ");
                        break;
                    case ExpressionType.Divide:
                        GetBinaryOperation(@" \ ");
                        break;
                    case ExpressionType.Modulo:
                        GetBinaryOperation(" % ");
                        break;
                    case ExpressionType.Not:
                        GetUnaryExpression(" NOT ");
                        break;
                    /**/
                    /*case ExpressionType.Coalesce:*/
                    case ExpressionType.Convert:
                        GetConversion(op as UnaryExpression);
                        break;
                    case ExpressionType.Lambda:
                        GetLambda(op as LambdaExpression);
                        break;
                    case ExpressionType.New:
                        GetNew(op as NewExpression);
                        break;
                    case ExpressionType.MemberAccess:
                        GetMemberAccess(op as MemberExpression);
                        break;
                    case ExpressionType.Parameter:
                        GetParameterValue(op as ParameterExpression);
                        break;
                    case ExpressionType.Constant:
                        GetConstantValue(op as ConstantExpression);
                        break;
                    case ExpressionType.Call:
                        GetMethodCall(op as MethodCallExpression);
                        break;
                    default:
                        throw new NotSupportedException(
                            string.Format("The operator '{0}' is not supported", op.NodeType));
                }

            }

            private void GetUnaryExpression(string op)
            {
                string unaryOperand = GetUnaryOperand();
                terms.Push(Expression.Constant(
                                new BoxedConstant(op + " (" + unaryOperand + ")"))
                          );
            }

            private void GetBinaryOperation(string op)
            {

                string rightOperand;

                string leftOperand;

                GetBinaryOperands(out rightOperand, out leftOperand);

                terms.Push(Expression.Constant(
                                new BoxedConstant("(" + rightOperand + op + leftOperand + ")")
                           ));
            }

            private void GetLambda(LambdaExpression lambda)
            {

                if (lambda.Body.Type != typeof(void))
                {
                    terms.Push(Expression.Constant(
                                    new BoxedConstant(lambda.ToString()))
                               );
                }
            }

            private void GetConversion(UnaryExpression op)
            {

                switch (op.Type.Name)
                {
                    case "Boolean":
                    case "Char":
                    case "Enum":
                    case "Guid":
                    case "String":
                    case "DateTime":
                    case "Decimal":
                    case "Int16":
                    case "Int32":
                    case "Int64":
                    case "IntPtr":
                    case "UInt16":
                    case "UInt32":
                    case "UInt64":
                    case "UIntPtr":
                    case "Byte":
                    case "SByte":
                    case "Double":
                    case "Single":
                    case "Nullable`1":
                        //wrong
                        terms.Push(op.Operand);
                        break;
                    default:
                        throw new NotSupportedException(
                            string.Format("The conversion to '{0}' is not supported", op.Type.Name));
                }
            }

            private void GetConstantValue(ConstantExpression c)
            {
                if (Type.GetTypeCode(c.Value.GetType()) == TypeCode.Object)
                {
                    if (c.Value.GetType().Name.StartsWith("Query`1"))
                    {
                        terms.Push(Expression.Constant(
                            new BoxedConstant(
                                 GetTableName(c.Value.GetType().GetGenericArguments()[0])
                                            )));
                    }
                    else if (c.Value.GetType() == typeof(BoxedConstant))
                    {
                        terms.Push(Expression.Constant(
                                     ((BoxedConstant)c.Value).Expression));
                        return;
                    }
                }
                else if (c.Value == null)
                {
                    constants.Add("NULL");
                }
                else
                {
                    constants.Add(c.Value);
                }

                //terms.Push(Expression.Constant(new BoxedConstant("@p" +
                //                                                 parameterCount.ToString())));

                terms.Push(Expression.Constant(new BoxedConstant("@" +
                                                                 parameterCount.ToString())));

                parameterCount++;
            }

            private void GetParameterValue(ParameterExpression p)
            {
                terms.Push(Expression.Constant(p.Name));
            }

            private void GetMemberAccess(MemberExpression m)
            {

                if (m.Expression != null)
                {

                    terms.Push(Expression.Constant(
                               new BoxedConstant(GetHashedName(m))));
                    return;
                }

                terms.Push(Expression.Constant(
                               new BoxedConstant(string.Empty)));
            }

            private void GetMethodCall(MethodCallExpression m)
            {

                if (m.Method.DeclaringType == typeof(Queryable) ||
                    m.Method.DeclaringType == typeof(Enumerable))
                {
                    GetQueryableMethodCall(m);
                    return;
                }
                else if (m.Method.DeclaringType == typeof(string))
                {
                    GetStringMethodCall(m);
                    return;
                }

                throw new ArgumentException();
            }

            private void GetQueryableMethodCall(MethodCallExpression m)
            {

                Debug.Assert(m.Method.DeclaringType == typeof(Queryable) ||
                             m.Method.DeclaringType == typeof(Enumerable));

                object value = null;

                string leftOperand;
                string rightOperand;

                switch (m.Method.Name)
                {
                    case "Select":
                    case "Where":
                        GetBinaryOperands(out leftOperand, out rightOperand);
                        value = m.Method.Name.ToUpper();
                        break;
                    case "OrderBy":
                    case "OrderByDescending":
                    case "ThenBy":
                    case "ThenByDescending":
                        GetBinaryOperands(out leftOperand, out rightOperand);
                        value = m.Method.Name.ToUpper();
                        break;
                    /*case "Join":
                    case "SelectMany":
                    */
                    case "Count":
                    case "Average":
                    case "Max":
                    case "Min":
                    case "Sum":
                        var x = GetSourceType(m);

                        if (x == lambdaExpression.Parameters[0].Type)
                        {
                            value = GetAggregate(m);
                        }
                        else
                        {
                            // no send the lamda to another LambdaExpressionHandler
                            value = m.Method.Name.ToUpper();
                        }
                        break;
                    default:
                        for (int i = 0; i < m.Arguments.Count; i++)
                        {
                            GetUnaryOperand();
                        }
                        value = m.Method.Name.ToUpper();
                        break;
                }

                Debug.Assert(value != null);

                terms.Push(Expression.Constant(
                              new BoxedConstant(value.ToString())));
            }

            private void GetStringMethodCall(MethodCallExpression m)
            {

                Debug.Assert(m.Method.DeclaringType == typeof(string));

                string value = string.Empty;

                string left;
                string right;
                string val;

                switch (m.Method.Name)
                {
                    case "StartsWith":
                        GetBinaryOperands(out left, out right);
                        value = left + " Like (" + right + " + '%')";
                        break;
                    case "EndsWith":
                        GetBinaryOperands(out left, out right);
                        value = left + " Like ('%' + " + right + ")";
                        break;
                    case "Contains":
                        GetBinaryOperands(out left, out right);
                        value = left + " Like ('%' + " + right + " + '%')";
                        break;
                    case "Substring":
                        Debug.Assert(m.Arguments.Count == 2); // should throw an error instead
                        GetBinaryOperands(out left, out right);
                        val = GetOperandValue();
                        value = "Substring(" + val + ", " + left + ", " + right + ")";
                        break;
                    case "ToUpper":
                        Debug.Assert(m.Arguments.Count == 0);
                        val = GetOperandValue();
                        value = "Upper(" + val + ")";
                        break;
                    case "ToLower":
                        Debug.Assert(m.Arguments.Count == 0);
                        val = GetOperandValue();
                        value = "Lower(" + val + ")";
                        break;
                    default:
                        throw new ArgumentException();
                }

                terms.Push(Expression.Constant(
                               new BoxedConstant(value)));
            }

            private string GetCount(MethodCallExpression method)
            {

                Debug.Assert(method.Arguments.Count == 1);

                GetOperandValue();

                var sourceType = method.Method.GetGenericArguments()[0];

                var declaringType = GetSourceType(method.Arguments[0]);

                var foreignKey = GetForeignKey(declaringType, method.Arguments[0].Type);

                var foreignKeyExpression = Expression.MakeMemberAccess(
                                                Expression.Parameter(sourceType, sourceType.Name),
                                                sourceType.GetProperty(foreignKey));

                var whereCondition = Expression.Equal(foreignKeyExpression,
                                        Expression.Constant(
                                            new BoxedConstant(GetTableAlias(indentLevel) + "." +
                                                              GetPrimaryKey(declaringType))));

                var whereCall = QueryableMethodsProvider.GetWhereCall(sourceType, "source", whereCondition);

                var selectCall = QueryableMethodsProvider.GetSelectCall(whereCall);

                SqlExpressionParser projector = new SqlExpressionParser(indentLevel + 1, null,
                                                         GetAggregateTypeFromName(method.Method.Name));

                projector.Translate(selectCall);

                accessedColumns.Add(GetProjectionSql(indentLevel, projector));

                return GetProjectionSql(indentLevel + 1, projector);
            }

            private string GetAggregate(MethodCallExpression method)
            {

                if (method.Arguments.Count == 1)
                {
                    return GetCount(method);
                }

                Debug.Assert(method.Arguments.Count == 2);

                GetOperandValue();
                GetOperandValue();

                var accessLambda = (LambdaExpression)method.Arguments[1];

                var sourceType = accessLambda.Parameters[0].Type;

                if (sourceType != lambdaExpression.Parameters[0].Type
                    && accessLambda.Body.NodeType == ExpressionType.Call)
                {
                    return GetNestedAggregate(method);
                }

                var selectorParam = Expression.Parameter(sourceType,
                                                         accessLambda.Parameters[0].Name);

                var projectionSelector = Expression.Lambda(accessLambda.Body, selectorParam);

                var whereCall = GetCorrelation(method, sourceType);

                var selectCall = QueryableMethodsProvider.GetSelectCall(whereCall, projectionSelector);

                SqlExpressionParser projector =
                    new SqlExpressionParser(indentLevel + 1, null,
                                         GetAggregateTypeFromName(method.Method.Name));

                projector.Translate(selectCall);

                accessedColumns.Add(GetProjectionSql(indentLevel, projector));

                return GetProjectionSql(indentLevel + 1, projector);
            }

            private string GetNestedAggregate(MethodCallExpression method)
            {

                var accessLambda = (LambdaExpression)method.Arguments[1];

                var sourceType = accessLambda.Parameters[0].Type;

                Debug.Assert(sourceType != lambdaExpression.Parameters[0].Type);

                var whereCall = GetCorrelation(method, sourceType);

                var sumCall = Expression.Call(
                                typeof(Queryable).GetMethods()
                                    .Where(m => m.Name == method.Method.Name &&
                                                m.ReturnType == method.Type &&
                                                m.GetParameters().Length == 2)
                                    .Single().MakeGenericMethod(sourceType),
                                    whereCall, accessLambda);

                var foreignKey = ((StripQuotes(whereCall.Arguments[1]) as LambdaExpression).Body
                                   as BinaryExpression).Left;

                var keyValueType = typeof(KeyValuePair<int, int>)
                                    .GetGenericTypeDefinition()
                                    .MakeGenericType(foreignKey.Type,
                                                     accessLambda.Body.Type);

                var keyValueConstructor =
                    keyValueType.GetConstructor(new Type[]{foreignKey.Type,
                                                            accessLambda.Body.Type});

                var newKeyValue = Expression.New(keyValueConstructor,
                                                 new Expression[]{foreignKey,
                                                                   sumCall
                                                                   },
                                                 new PropertyInfo[]{
                                                          keyValueType.GetProperty("Key"),
                                                          keyValueType.GetProperty("Value")
                                                      });

                var selectorParam = Expression.Parameter(sourceType, "source");

                var projectionSelector = Expression.Lambda(newKeyValue, selectorParam);

                var aggregateSelect = QueryableMethodsProvider.GetSelectCall(
                                            whereCall, projectionSelector);

                SqlExpressionParser projector =
                    new SqlExpressionParser(indentLevel + 1, null,
                                            AggregateType.None);

                projector.Translate(aggregateSelect);

                accessedColumns.Add(GetProjectionSql(indentLevel, projector));

                return GetProjectionSql(indentLevel, projector);
            }

            private MethodCallExpression GetCorrelation(MethodCallExpression method, Type sourceType)
            {

                var declaringType = lambdaExpression.Parameters[0].Type;

                BinaryExpression whereCondition = null;

                // if for example the declaring type looks like
                // <>f__AnonymousType0`2[[Order],[Customer]]
                // as a result of a join 
                // we need to correlate both order and customer
                var genericArguments = declaringType.GetGenericArguments();

                if (genericArguments.Length == 0)
                {

                    whereCondition = GetCorrelationCondition(method, sourceType, declaringType,
                                                             GetTableAlias(indentLevel) + ".");
                }
                else
                {
                    var theType = genericArguments
                                     .Where(t => t.GetProperties()
                                     .Any(p => p.PropertyType == method.Arguments[0].Type))
                                     .Single();

                    whereCondition = GetCorrelationCondition(method, sourceType, theType,
                                        GetTableAlias(indentLevel) + "." + theType.GUID + ".");
                }

                var whereCall = QueryableMethodsProvider.GetWhereCall(sourceType, "source", whereCondition);

                return whereCall;
            }

            private BinaryExpression GetCorrelationCondition(MethodCallExpression method,
                                                             Type sourceType,
                                                             Type declaringType,
                                                             string tableAlias)
            {

                var foreignKey = GetForeignKey(declaringType, method.Arguments[0].Type);

                var foreignKeyExpression = Expression.MakeMemberAccess(
                                                Expression.Parameter(sourceType, sourceType.Name),
                                                sourceType.GetProperty(foreignKey));

                var whereCondition = Expression.Equal(foreignKeyExpression,
                                        Expression.Constant(
                                            new BoxedConstant(tableAlias +
                                                              GetPrimaryKey(declaringType))));
                return whereCondition;
            }

            private AggregateType GetAggregateTypeFromName(string name)
            {

                switch (name)
                {
                    case "Count":
                        return AggregateType.Count;
                    case "Sum":
                        return AggregateType.Sum;
                    case "Min":
                        return AggregateType.Min;
                    case "Max":
                        return AggregateType.Max;
                    case "Average":
                        return AggregateType.Average;
                }
                throw new ArgumentException();
            }

            private void GetNew(NewExpression newExpression)
            {

                foreach (var argument in newExpression.Arguments)
                {
                    GetOperandValue();
                }

                var args = newExpression.Arguments;

                var members = newExpression.Members;

                if (newExpression.Type != lambdaExpression.Body.Type)
                {

                    var lambdaHandler = new LambdaExpressionHandler(indentLevel + 1,
                                            Expression.Lambda(newExpression,
                                                Expression.Parameter(
                                                    lambdaExpression.Parameters[0].Type,
                                                    "source")));


                    foreach (var column in lambdaHandler.aliases)
                    {
                        aliases[lambaExpressionId + "." + column.Key] = column.Value;
                        aliases[column.Key] = column.Value;
                    }
                }
                else
                {

                    for (int i = 0; i < args.Count; i++)
                    {

                        if (args[i].NodeType != ExpressionType.MemberAccess ||
                            //hack - should check if MemberAccess has a corresponding column
                            // in db
                            !(args[i].Type.IsValueType || args[i].Type == typeof(string)))
                        {
                            continue;
                        }

                        string memberName = null;

                        if (newExpression.Members[i].Name.StartsWith("get_"))
                        {
                            memberName = newExpression.Members[i].Name.Substring(4);
                        }
                        else
                        {
                            memberName = newExpression.Members[i].Name;
                        }

                        var key = lambaExpressionId + "." + memberName;

                        aliases[key] = GetHashedName((args[i] as MemberExpression));
                    }
                }

                terms.Push(Expression.Constant(
                                new BoxedConstant(newExpression.ToString())));
            }

            private void GetBinaryOperands(out string rightOperand, out string leftOperand)
            {

                Debug.Assert(terms.Count > 1);

                leftOperand = GetOperandValue();

                rightOperand = GetOperandValue();
            }

            private string GetUnaryOperand()
            {

                Debug.Assert(terms.Count > 0);

                return GetOperandValue();
            }

            private string GetOperandValue()
            {

                while (terms.Peek().Type != typeof(BoxedConstant))
                {
                    GetExpression();
                }

                var result = terms.Pop();

                return (result as ConstantExpression).Value.ToString();
            }

            private string GetProjectionSql(int indentLevel, SqlExpressionParser project)
            {

                return Environment.NewLine +
                    GetIndentation(indentLevel) +
                    "(" +
                    Environment.NewLine +
                    project.GetSQLStatement() +
                    GetIndentation(indentLevel) +
                    ")" +
                    Environment.NewLine;
            }

            private string GetHashedName(MemberExpression m)
            {

                string memberName = null;

                if (m.Type == typeof(string) || m.Type.IsValueType)
                {
                    memberName = m.Member.Name;
                }
                else
                {
                    memberName = m.Type.GUID.ToString();
                }

                if (m.Expression.NodeType == ExpressionType.MemberAccess)
                {
                    return GetHashedName((MemberExpression)m.Expression) + "." + memberName;
                }

                return m.Expression.Type.GUID.ToString() + "." + memberName;
            }

            private Type GetSourceType(Expression expression)
            {

                switch (expression.NodeType)
                {
                    case ExpressionType.MemberAccess:
                        return GetSourceType(
                            (expression as MemberExpression).Expression);
                    case ExpressionType.Convert:
                    case ExpressionType.ConvertChecked:
                        return GetSourceType((expression as UnaryExpression).Operand);
                    case ExpressionType.Constant:
                    case ExpressionType.Parameter:
                        return expression.Type;
                    case ExpressionType.Call:
                        var method = expression as MethodCallExpression;

                        Debug.Assert(method.Method.DeclaringType == typeof(Queryable) ||
                                     method.Method.DeclaringType == typeof(Enumerable));

                        return GetSourceType(method.Arguments[0]);
                    default:
                        throw new ArgumentException();
                }
            }

            private string GetPrimaryKey(Type sourceType)
            {
                var primaryKeyAttribute = typeof(PrimaryKeyAttribute);

                var primaryKey = (from attribute in sourceType.GetCustomAttributes(primaryKeyAttribute, false)
                                  select ((PrimaryKeyAttribute)attribute).Value).First();

                return primaryKey;
            }

            private string GetForeignKey(Type sourceType, Type memberType)
            {
                throw new NotImplementedException();
            }

            public string[] GetAccessedFields()
            {

                return accessedColumns.Distinct().ToArray();
            }

            public string ReplaceAliases(string expression)
            {

                return ReplaceAliases(expression, true);
            }

            public string ReplaceAliases(string expression, bool replaceAliases)
            {

                if (!replaceAliases)
                {
                    return expression;
                }

                var result = new StringBuilder(expression);

                foreach (var column in aliases)
                {
                    result.Replace(column.Key, column.Value);
                }

                return result.ToString();
            }

        }

        private class BoxedConstant
        {

            private string expression = null;

            public BoxedConstant(string expression)
            {
                this.expression = expression;
            }

            public string Expression
            {
                get
                {
                    return expression;
                }
            }

            public static bool operator ==(string s, BoxedConstant bc)
            {
                throw new InvalidOperationException();
            }

            public static bool operator !=(string s, BoxedConstant bc)
            {
                throw new InvalidOperationException();
            }

            public static bool operator ==(int i, BoxedConstant bc)
            {
                throw new InvalidOperationException();
            }

            public static bool operator !=(int i, BoxedConstant bc)
            {
                throw new InvalidOperationException();
            }

            public override string ToString()
            {
                return expression;
            }
        }

        private static class QueryableMethodsProvider
        {

            private static readonly MethodInfo[] queryableMethods = typeof(Queryable).GetMethods();

            private static readonly MethodInfo selectMethod =
                                            (from q in queryableMethods
                                             where q.Name == "Select" && q.GetGenericArguments().Length == 2
                                             select q.GetGenericMethodDefinition()).First();

            private static readonly MethodInfo whereMethod =
                                            (from q in queryableMethods
                                             where q.Name == "Where" && q.GetGenericArguments().Length == 1
                                             select q.GetGenericMethodDefinition()).First();

            private static readonly Type queryableType = typeof(System.Linq.IQueryable<IQueryable<int>>)
                                                        .GetGenericTypeDefinition();

            public static MethodCallExpression GetSelectCall(Type sourceType)
            {

                var queryableType = QueryableMethodsProvider.GetQueryableType(sourceType);

                var sourceParam = Expression.Parameter(queryableType, "source");

                var selectorParam = Expression.Parameter(sourceType, "param");

                var projectionSelector = Expression.Lambda(selectorParam, selectorParam);

                return GetSelectCall(sourceParam, projectionSelector);
            }

            public static MethodCallExpression GetSelectCall(Expression source)
            {

                var sourceType = source.Type.GetGenericArguments()[0];

                var queryableType = QueryableMethodsProvider.GetQueryableType(sourceType);

                var sourceParam = Expression.Parameter(queryableType, "source");

                var selectorParam = Expression.Parameter(sourceType, "param");

                var projectionSelector = Expression.Lambda(selectorParam, selectorParam);

                return GetSelectCall(source, projectionSelector);
            }

            public static MethodCallExpression GetSelectCall(Expression source, LambdaExpression projectionSelector)
            {

                var selectQuery = QueryableMethodsProvider
                                        .GetSelectMethod(source.Type.GetGenericArguments()[0],
                                                         projectionSelector.Type.GetGenericArguments()[1]);

                return Expression.Call(selectQuery, source, Expression.Constant(projectionSelector));
            }

            public static MethodCallExpression GetSelectCall(Type sourceType, LambdaExpression projectionSelector)
            {

                var queryableType = QueryableMethodsProvider.GetQueryableType(sourceType);

                var sourceParam = Expression.Parameter(queryableType, "source");

                return GetSelectCall(sourceParam, projectionSelector);
            }

            public static MethodCallExpression GetWhereCall(Type sourceType, string sourceName, BinaryExpression condition)
            {

                var queryableType = QueryableMethodsProvider.GetQueryableType(sourceType);

                var whereLambda = Expression.Lambda(condition, Expression.Parameter(sourceType, sourceName));

                var whereQuery = QueryableMethodsProvider.GetWhereMethod(sourceType);

                var queryableSource = Expression.Parameter(queryableType, "source");

                var whereCall = Expression.Call(whereQuery, queryableSource, whereLambda);

                return whereCall;
            }

            private static MethodInfo GetSelectMethod(Type tableType, Type projectionSelectorType)
            {
                return selectMethod.MakeGenericMethod(tableType, projectionSelectorType); ;
            }

            private static MethodInfo GetWhereMethod(Type tableType)
            {
                return whereMethod.MakeGenericMethod(tableType); ;
            }

            public static Type GetQueryableType(Type tableType)
            {
                return queryableType.MakeGenericType(tableType);
            }
        }
    }
}