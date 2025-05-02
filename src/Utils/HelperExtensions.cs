namespace codecrafters_sqlite.Utils;

public static class HelperExtensions
{
    private static object ReadColumnData(this Stream dbStream, long serialTypeCode)
    {
        var dbEncoding = Helpers.GetDbEncoding(dbStream);
        switch(serialTypeCode)  
        {
            case 0:
                return new object();
            case 1:
                return (sbyte)dbStream.ReadByte();
            case 2:
            {
                var buffer = new byte[2];
                dbStream.ReadExactly(buffer, 0, 2);
                return Helpers.ReadInt16BigEndian(buffer);
            }
            case 3:
            {
                var buffer = new byte[3];
                dbStream.ReadExactly(buffer, 0, 3);
                return Helpers.ReadInt24BigEndian(buffer);
            }
            case 4:
            {
                var buffer = new byte[4];
                dbStream.ReadExactly(buffer, 0, 4);
                return Helpers.ReadInt32BigEndian(buffer);
            }
            case 5:
            {
                var buffer = new byte[6];
                dbStream.ReadExactly(buffer, 0, 6);
                return Helpers.ReadInt48BigEndian(buffer);
            }
            case 6:
            {
                var buffer = new byte[8];
                dbStream.ReadExactly(buffer, 0, 8);
                return Helpers.ReadInt64BigEndian(buffer);
            }
            case 7:
            {
                var buffer = new byte[8];
                dbStream.ReadExactly(buffer, 0, 8);
                if (BitConverter.IsLittleEndian) Array.Reverse(buffer);
                return BitConverter.ToDouble(buffer, 0);
            }
            case 8:
                return 0L;
            case 9:
                return 1L;
            default:
                if (serialTypeCode < 12)
                {
                    Console.WriteLine($"Not supported type {serialTypeCode}");
                    return new object();
                }

                long dataSize;
                bool isText;
                if (serialTypeCode % 2 == 0)
                {
                    dataSize = (serialTypeCode - 12) / 2;
                    isText = false;
                }
                else
                {
                    dataSize = (serialTypeCode - 13) / 2;
                    isText = true;
                }

                if (dataSize == 0)
                {
                    return isText ? (object)"" : (object)Array.Empty<byte>();
                }
                
                byte[] dataBuffer = new byte[dataSize];
                dbStream.ReadExactly(dataBuffer, 0, dataBuffer.Length);

                if (isText)
                {
                    return dbEncoding.GetString(dataBuffer);
                }
                else
                {
                    return dataBuffer;
                }
        }
    }
    
    public static List<object> ReadRecord(this Stream dbStream)
    {
        var (totalHeaderSize, headerSizeBytesRead) = Helpers.ReadVarint(dbStream);
        var totalBytesReadInHeader = headerSizeBytesRead;
        var serialTypeCodes = new List<long>();
        while(totalBytesReadInHeader < totalHeaderSize)
        {
            var (typeSerialCode, typeCodeBytes) = Helpers.ReadVarint(dbStream);
            serialTypeCodes.Add(typeSerialCode);
            totalBytesReadInHeader += typeCodeBytes;

            if (totalBytesReadInHeader > totalHeaderSize)
            {
                throw new InvalidDataException($"Se leyeron más bytes ({totalBytesReadInHeader}]) para la cabecera de los indicados ({totalHeaderSize}");
            }
            
        }
        
        var recordData = serialTypeCodes.Select(dbStream.ReadColumnData).ToList();
        return recordData;
    }
}