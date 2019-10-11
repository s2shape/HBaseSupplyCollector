using System;
using System.Collections.Generic;
using System.Text;

namespace HBaseSupplyCollector
{
    public static class StringHelpers
    {
        public static string ToUtf8String(this byte[] bytes) {
            return Encoding.UTF8.GetString(bytes);
        }

        public static byte[] ToUtf8Bytes(this string str) {
            return Encoding.UTF8.GetBytes(str);
        }
    }
}
