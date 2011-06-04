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

namespace PetaPoco
{
    internal static class TypeSystem
    {
        internal static Type GetElementType(Type seqType)
        {
            Type ienum = FindIEnumerable(seqType);

            if (ienum == null) return seqType;

            return ienum.GetGenericArguments()[0];
        }

        private static Type FindIEnumerable(Type seqType)
        {
            if (seqType == null || seqType == typeof(string))
                return null;

            if (seqType.IsArray)
                return typeof(IEnumerable<>).MakeGenericType(seqType.GetElementType());

            if (seqType.IsGenericType)
            {
                foreach (Type arg in seqType.GetGenericArguments())
                {
                    Type ienum = typeof(IEnumerable<>).MakeGenericType(arg);

                    if (ienum.IsAssignableFrom(seqType))
                    {
                        return ienum;
                    }
                }
            }

            Type[] ifaces = seqType.GetInterfaces();

            if (ifaces != null && ifaces.Length > 0)
            {
                foreach (Type iface in ifaces)
                {
                    Type ienum = FindIEnumerable(iface);

                    if (ienum != null)
                    {
                        return ienum;
                    }
                }
            }

            if (seqType.BaseType != null && seqType.BaseType != typeof(object))
            {
                return FindIEnumerable(seqType.BaseType);
            }

            return null;
        }
    }
}