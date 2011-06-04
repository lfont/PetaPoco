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
using System.Threading;

namespace PetaPoco
{
    public class ThreadSafeCache<TKey, TValue>
    {
        private readonly Dictionary<TKey, TValue> theDictionary = new Dictionary<TKey, TValue>();
        private readonly ReaderWriterLock rwl = new ReaderWriterLock();
        private readonly TimeSpan readerLockTimeout = TimeSpan.FromSeconds(1);
        private readonly TimeSpan writerLockTimeout = TimeSpan.FromSeconds(2);

        public ThreadSafeCache()
        {
        }

        public TValue this[TKey key]
        {
            get
            {
                try
                {
                    rwl.AcquireReaderLock(readerLockTimeout);
                    return theDictionary[key];
                }
                finally
                {
                    rwl.ReleaseReaderLock();
                }
            }
        }

        public ICollection<TKey> Keys
        {
            get
            {
                try
                {
                    rwl.AcquireReaderLock(readerLockTimeout);
                    return theDictionary.Keys;
                }
                finally
                {
                    rwl.ReleaseReaderLock();
                }
            }
        }

        public Dictionary<TKey, TValue>.ValueCollection Values
        {
            get
            {
                try
                {
                    rwl.AcquireReaderLock(readerLockTimeout);
                    return theDictionary.Values;
                }
                finally
                {
                    rwl.ReleaseReaderLock();
                }
            }
        }

        public bool ContainsKey(TKey key)
        {
            try
            {
                rwl.AcquireReaderLock(readerLockTimeout);
                return theDictionary.ContainsKey(key);
            }
            finally
            {
                rwl.ReleaseReaderLock();
            }
        }

        public bool Remove(TKey key)
        {
            try
            {
                rwl.AcquireWriterLock(writerLockTimeout);
                return theDictionary.Remove(key);
            }
            finally
            {
                rwl.ReleaseWriterLock();
            }
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            try
            {
                rwl.AcquireReaderLock(readerLockTimeout);
                return theDictionary.TryGetValue(key, out value);
            }
            finally
            {
                rwl.ReleaseReaderLock();
            }
        }

        public bool TryAdd(TKey key, TValue value)
        {
            try
            {
                rwl.AcquireWriterLock(writerLockTimeout);

                if (theDictionary.ContainsKey(key))
                {
                    return false;
                }

                theDictionary.Add(key, value);
                return true;
            }
            finally
            {
                rwl.ReleaseWriterLock();
            }
        }

        public void Clear()
        {
            try
            {
                rwl.AcquireWriterLock(writerLockTimeout);
                theDictionary.Clear();
            }
            finally
            {
                rwl.ReleaseWriterLock();
            }
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {

            foreach (KeyValuePair<TKey, TValue> pair in theDictionary)
            {
                yield return pair;
            }
        }
    }
}