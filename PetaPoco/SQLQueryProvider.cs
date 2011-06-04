﻿#region Copyright (c) 2008 by Jahmani Muigai Mwaura and Community
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

using System.Collections.Generic;
using System.Linq.Expressions;

namespace PetaPoco
{
    public class SqlQueryProvider : QueryProvider
    {
        private readonly Database database;

        public SqlQueryProvider(Database database)
        {
            this.database = database;
        }

        public override Sql GetQueryText(Expression expression)
        {
            return SqlExpressionParser.TranslateExpression(expression);
        }

        public override IEnumerable<T> Execute<T>(Expression expression)
        {
            Sql sql = GetQueryText(expression);
            return database.Query<T>(sql);
        }
    }
}