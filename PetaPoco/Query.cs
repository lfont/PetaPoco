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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace PetaPoco
{
    public class Query<T> : IOrderedQueryable<T>
    {
        QueryProvider provider;
        Expression expression;

        public Query(QueryProvider provider)
        {
            if (provider == null)
            {
                throw new ArgumentNullException("provider");
            }
            this.provider = provider;
            this.expression = Expression.Constant(this);
        }

        public Query(QueryProvider provider, Expression expression)
        {
            if (provider == null)
            {
                throw new ArgumentNullException("provider");
            }
            if (expression == null)
            {
                throw new ArgumentNullException("expression");
            }
            if (!typeof(IQueryable<T>).IsAssignableFrom(expression.Type))
            {
                throw new ArgumentOutOfRangeException("expression");
            }
            this.provider = provider;
            this.expression = expression;
        }

        Expression IQueryable.Expression
        {
            get { return this.expression; }
        }

        Type IQueryable.ElementType
        {
            get { return typeof(T); }
        }

        IQueryProvider IQueryable.Provider
        {
            get { return this.provider; }
        }

        public IEnumerator<T> GetEnumerator()
        {
            return this.provider.Execute<T>(this.expression).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.provider.Execute<T>(this.expression).GetEnumerator();
        }

        public override string ToString()
        {
            return this.provider.GetQueryText(this.expression).SQL;
        }
    }

    public abstract class QueryProvider : IQueryProvider
    {
        protected QueryProvider()
        {
        }

        IQueryable<TElement> IQueryProvider.CreateQuery<TElement>(Expression expression)
        {
            return new Query<TElement>(this, expression);
        }

        IQueryable IQueryProvider.CreateQuery(Expression expression)
        {
            Type elementType = TypeSystem.GetElementType(expression.Type);
            try
            {
                return (IQueryable)Activator.CreateInstance(typeof(Query<>).MakeGenericType(elementType), new object[] { this, expression });
            }
            catch (TargetInvocationException tie)
            {
                throw tie.InnerException;
            }
        }

        TResult IQueryProvider.Execute<TResult>(Expression expression)
        {
            IEnumerable<TResult> results = Execute<TResult>(expression);
            return results.FirstOrDefault();
        }

        object IQueryProvider.Execute(Expression expression)
        {
            Type elementType = TypeSystem.GetElementType(expression.Type);
            MethodInfo method = GetType().GetMethod("Execute");
            MethodInfo generic = method.MakeGenericMethod(elementType);
            return generic.Invoke(this, new object[] { expression });
        }

        public abstract Sql GetQueryText(Expression expression);

        public abstract IEnumerable<T> Execute<T>(Expression expression);
    }
}