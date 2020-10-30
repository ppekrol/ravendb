using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Extensions;

namespace Sparrow.Json
{
    public class AsyncBlittableJsonTextWriter : AbstractBlittableJsonTextWriter
    {
        private readonly Stream _outputStream;

        public AsyncBlittableJsonTextWriter(JsonOperationContext context, Stream stream)
            : base(context, context.CheckoutMemoryStream())
        {
            _outputStream = stream;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<int> MaybeOuterFlushAsync(CancellationToken token = default)
        {
            var innerStream = _stream as MemoryStream;
            if (innerStream == null)
                ThrowInvalidTypeException(_stream?.GetType());
            if (innerStream.Length * 2 <= innerStream.Capacity)
                return 0;

            await FlushAsync(token).ConfigureAwait(false);
            return await OuterFlushAsync(token).ConfigureAwait(false);
        }

        public async ValueTask<int> OuterFlushAsync(CancellationToken token = default)
        {
            var innerStream = _stream as MemoryStream;
            if (innerStream == null)
                ThrowInvalidTypeException(_stream?.GetType());
            await FlushAsync(token).ConfigureAwait(false);
            innerStream.TryGetBuffer(out var bytes);
            var bytesCount = bytes.Count;
            if (bytesCount == 0)
                return 0;
            await _outputStream.WriteAsync(bytes.Array, bytes.Offset, bytesCount, token).ConfigureAwait(false);
            innerStream.SetLength(0);
            return bytesCount;
        }

        public override async ValueTask DisposeAsync()
        {
            await base.DisposeAsync().ConfigureAwait(false);
            await OuterFlushAsync().ConfigureAwait(false);
            _context.ReturnMemoryStream((MemoryStream)_stream);
        }

        private void ThrowInvalidTypeException(Type typeOfStream)
        {
            throw new ArgumentException($"Expected stream to be MemoryStream, but got {(typeOfStream == null ? "null" : typeOfStream.ToString())}.");
        }
    }

    public abstract class AbstractBlittableJsonTextWriter : IAsyncDisposable
    {
        protected readonly JsonOperationContext _context;
        protected readonly Stream _stream;
        private const byte StartObject = (byte)'{';
        private const byte EndObject = (byte)'}';
        private const byte StartArray = (byte)'[';
        private const byte EndArray = (byte)']';
        private const byte Comma = (byte)',';
        private const byte Quote = (byte)'"';
        private const byte Colon = (byte)':';
        public static readonly byte[] NaNBuffer = { (byte)'"', (byte)'N', (byte)'a', (byte)'N', (byte)'"' };

        public static readonly byte[] PositiveInfinityBuffer =
        {
            (byte)'"', (byte)'I', (byte)'n', (byte)'f', (byte)'i', (byte)'n', (byte)'i', (byte)'t', (byte)'y', (byte)'"'
        };

        public static readonly byte[] NegativeInfinityBuffer =
        {
            (byte)'"', (byte)'-', (byte)'I', (byte)'n', (byte)'f', (byte)'i', (byte)'n', (byte)'i', (byte)'t', (byte)'y', (byte)'"'
        };

        public static readonly byte[] NullBuffer = { (byte)'n', (byte)'u', (byte)'l', (byte)'l', };
        public static readonly byte[] TrueBuffer = { (byte)'t', (byte)'r', (byte)'u', (byte)'e', };
        public static readonly byte[] FalseBuffer = { (byte)'f', (byte)'a', (byte)'l', (byte)'s', (byte)'e', };

        private static readonly byte[] EscapeCharacters;
        public static readonly byte[][] ControlCodeEscapes;

        private readonly RavenMemory _buffer;
        private readonly RavenMemory _auxiliarBuffer;

        private int _pos;
        private JsonOperationContext.MemoryBuffer.ReturnBuffer _returnBuffer;
        private readonly JsonOperationContext.MemoryBuffer _pinnedBuffer;
        private readonly AllocatedMemoryData _returnAuxiliarBuffer;

        static AbstractBlittableJsonTextWriter()
        {
            ControlCodeEscapes = new byte[32][];

            for (int i = 0; i < 32; i++)
            {
                ControlCodeEscapes[i] = Encodings.Utf8.GetBytes(i.ToString("X4"));
            }

            EscapeCharacters = new byte[256];
            for (int i = 0; i < 32; i++)
                EscapeCharacters[i] = 0;

            for (int i = 32; i < EscapeCharacters.Length; i++)
                EscapeCharacters[i] = 255;

            EscapeCharacters[(byte)'\b'] = (byte)'b';
            EscapeCharacters[(byte)'\t'] = (byte)'t';
            EscapeCharacters[(byte)'\n'] = (byte)'n';
            EscapeCharacters[(byte)'\f'] = (byte)'f';
            EscapeCharacters[(byte)'\r'] = (byte)'r';
            EscapeCharacters[(byte)'\\'] = (byte)'\\';
            EscapeCharacters[(byte)'/'] = (byte)'/';
            EscapeCharacters[(byte)'"'] = (byte)'"';
        }

        protected AbstractBlittableJsonTextWriter(JsonOperationContext context, Stream stream)
        {
            _context = context;
            _stream = stream;

            _returnBuffer = context.GetMemoryBuffer(out _pinnedBuffer);
            _buffer = _pinnedBuffer.Memory;

            _returnAuxiliarBuffer = context.GetMemory(32);
            _auxiliarBuffer = _returnAuxiliarBuffer.Memory;
        }

        public int Position => _pos;

        public override string ToString()
        {
            return Encodings.Utf8.GetString(_buffer.Memory.Span.Slice(0, _pos));
        }

        public async ValueTask WriteObjectAsync(BlittableJsonReaderObject obj, CancellationToken token = default)
        {
            if (obj == null)
            {
                await WriteNullAsync(token).ConfigureAwait(false);
                return;
            }

            await WriteStartObjectAsync(token).ConfigureAwait(false);

            var prop = new BlittableJsonReaderObject.PropertyDetails();
            using (var buffer = obj.GetPropertiesByInsertionOrder())
            {
                var props = buffer.Properties;
                for (int i = 0; i < props.Count; i++)
                {
                    if (i != 0)
                    {
                        await WriteCommaAsync(token).ConfigureAwait(false);
                    }

                    obj.GetPropertyByIndex(props.Array[i + props.Offset], ref prop);
                    await WritePropertyNameAsync(prop.Name, token).ConfigureAwait(false);

                    await WriteValueAsync(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value, token).ConfigureAwait(false);
                }
            }

            await WriteEndObjectAsync(token).ConfigureAwait(false);
        }

        private async ValueTask WriteArrayToStreamAsync(BlittableJsonReaderArray array, CancellationToken token = default)
        {
            await WriteStartArrayAsync(token).ConfigureAwait(false);
            var length = array.Length;
            for (var i = 0; i < length; i++)
            {
                var propertyValueAndType = array.GetValueTokenTupleByIndex(i);

                if (i != 0)
                {
                    await WriteCommaAsync(token).ConfigureAwait(false);
                }
                // write field value
                await WriteValueAsync(propertyValueAndType.Item2, propertyValueAndType.Item1, token).ConfigureAwait(false);
            }
            await WriteEndArrayAsync(token).ConfigureAwait(false);
        }

        public async ValueTask WriteValueAsync(BlittableJsonToken jsonToken, object val, CancellationToken token = default)
        {
            switch (jsonToken)
            {
                case BlittableJsonToken.String:
                    await WriteStringAsync((LazyStringValue)val, token: token).ConfigureAwait(false);
                    break;

                case BlittableJsonToken.Integer:
                    await WriteIntegerAsync((long)val, token).ConfigureAwait(false);
                    break;

                case BlittableJsonToken.StartArray:
                    await WriteArrayToStreamAsync((BlittableJsonReaderArray)val, token).ConfigureAwait(false);
                    break;

                case BlittableJsonToken.EmbeddedBlittable:
                case BlittableJsonToken.StartObject:
                    var blittableJsonReaderObject = (BlittableJsonReaderObject)val;
                    await WriteObjectAsync(blittableJsonReaderObject, token).ConfigureAwait(false);
                    break;

                case BlittableJsonToken.CompressedString:
                    await WriteStringAsync((LazyCompressedStringValue)val, token).ConfigureAwait(false);
                    break;

                case BlittableJsonToken.LazyNumber:
                    await WriteDoubleAsync((LazyNumberValue)val, token).ConfigureAwait(false);
                    break;

                case BlittableJsonToken.Boolean:
                    await WriteBoolAsync((bool)val, token).ConfigureAwait(false);
                    break;

                case BlittableJsonToken.Null:
                    await WriteNullAsync(token).ConfigureAwait(false);
                    break;

                case BlittableJsonToken.RawBlob:
                    var blob = (BlittableJsonReaderObject.RawBlob)val;
                    await WriteRawStringAsync(blob.Ptr.Memory, blob.Length, token).ConfigureAwait(false);
                    break;

                default:
                    throw new DataMisalignedException($"Unidentified Type {jsonToken}");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WriteDateTimeAsync(DateTime value, bool isUtc, CancellationToken token = default)
        {
            int size = value.GetDefaultRavenFormat(_auxiliarBuffer.Memory.Span, isUtc);

            await WriteRawStringWhichMustBeWithoutEscapeCharsAsync(_auxiliarBuffer.Memory, size, token).ConfigureAwait(false);
        }

        public async ValueTask WriteStringAsync(string str, bool skipEscaping = false, CancellationToken token = default)
        {
            using (var lazyStr = _context.GetLazyString(str))
            {
                await WriteStringAsync(lazyStr, skipEscaping, token).ConfigureAwait(false);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WriteStringAsync(LazyStringValue str, bool skipEscaping = false, CancellationToken token = default)
        {
            if (str == null)
            {
                await WriteNullAsync(token).ConfigureAwait(false);
                return;
            }

            var size = str.Size;

            if (size == 1 && str.IsControlCodeCharacter(out var b))
            {
                await WriteStringAsync($@"\u{b:X4}", skipEscaping: true, token).ConfigureAwait(false);
                return;
            }

            var strBuffer = str.MemoryBuffer;
            var escapeSequencePos = size;
            var numberOfEscapeSequences = skipEscaping ? 0 : BlittableJsonReaderBase.ReadVariableSizeInt(strBuffer.Memory.Span, ref escapeSequencePos);

            // We ensure our buffer will have enough space to deal with the whole string.

            const int NumberOfQuotesChars = 2; // for " "

            int bufferSize = 2 * numberOfEscapeSequences + size + NumberOfQuotesChars;
            if (bufferSize >= JsonOperationContext.MemoryBuffer.Size)
            {
                await UnlikelyWriteLargeStringAsync(strBuffer.Memory, size, numberOfEscapeSequences, escapeSequencePos, token).ConfigureAwait(false); // OK, do it the slow way.
                return;
            }

            await EnsureBufferAsync(size + NumberOfQuotesChars, token).ConfigureAwait(false);
            _buffer.Memory.Span[_pos++] = Quote;

            if (numberOfEscapeSequences == 0)
            {
                // PERF: Fast Path.
                await WriteRawStringAsync(strBuffer.Memory, size, token).ConfigureAwait(false);
            }
            else
            {
                await UnlikelyWriteEscapeSequencesAsync(strBuffer.Memory, size, numberOfEscapeSequences, escapeSequencePos, token).ConfigureAwait(false);
            }

            _buffer.Memory.Span[_pos++] = Quote;
        }

        private async ValueTask UnlikelyWriteEscapeSequencesAsync(Memory<byte> strBuffer, int size, int numberOfEscapeSequences, int escapeSequencePos, CancellationToken token = default)
        {
            // We ensure our buffer will have enough space to deal with the whole string.
            int bufferSize = 2 * numberOfEscapeSequences + size + 1;

            await EnsureBufferAsync(bufferSize, token).ConfigureAwait(false);

            var ptr = strBuffer;
            var buffer = _buffer;
            while (numberOfEscapeSequences > 0)
            {
                numberOfEscapeSequences--;

                var bytesToSkip = BlittableJsonReaderBase.ReadVariableSizeInt(ptr.Span, ref escapeSequencePos);
                if (bytesToSkip > 0)
                {
                    await WriteRawStringAsync(strBuffer, bytesToSkip, token).ConfigureAwait(false);
                    strBuffer = strBuffer.Slice(bytesToSkip);
                    size -= bytesToSkip;
                }

                var escapeCharacter = strBuffer.Span[0];
                strBuffer = strBuffer.Slice(1);

                await WriteEscapeCharacterAsync(buffer.Memory, escapeCharacter, token).ConfigureAwait(false);

                size--;
            }

            Debug.Assert(size >= 0);

            // write remaining (or full string) to the buffer in one shot
            if (size > 0)
                await WriteRawStringAsync(strBuffer, size, token).ConfigureAwait(false);
        }

        private async ValueTask UnlikelyWriteLargeStringAsync(Memory<byte> strBuffer, int size, int numberOfEscapeSequences, int escapeSequencePos, CancellationToken token = default)
        {
            var ptr = strBuffer;

            await EnsureBufferAsync(1, token).ConfigureAwait(false);
            _buffer.Memory.Span[_pos++] = Quote;

            while (numberOfEscapeSequences > 0)
            {
                numberOfEscapeSequences--;
                var bytesToSkip = BlittableJsonReaderBase.ReadVariableSizeInt(ptr.Span, ref escapeSequencePos);

                await UnlikelyWriteLargeRawStringAsync(strBuffer, bytesToSkip, token).ConfigureAwait(false);
                strBuffer = strBuffer.Slice(bytesToSkip);
                size -= bytesToSkip + 1 /*for the escaped char we skip*/;
                var b = strBuffer.Span[0];
                strBuffer = strBuffer.Slice(1);

                await WriteEscapeCharacterAsync(_buffer.Memory, b, token).ConfigureAwait(false);
            }

            // write remaining (or full string) to the buffer in one shot
            await UnlikelyWriteLargeRawStringAsync(strBuffer, size, token).ConfigureAwait(false);

            await EnsureBufferAsync(1, token).ConfigureAwait(false);
            _buffer.Memory.Span[_pos++] = Quote;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async ValueTask WriteEscapeCharacterAsync(Memory<byte> buffer, byte b, CancellationToken token = default)
        {
            byte r = EscapeCharacters[b];
            if (r == 0)
            {
                await EnsureBufferAsync(6, token).ConfigureAwait(false);
                buffer.Span[_pos++] = (byte)'\\';
                buffer.Span[_pos++] = (byte)'u';

                ControlCodeEscapes[b].AsSpan(0, 4).CopyTo(buffer.Span.Slice(_pos));

                _pos += 4;
                return;
            }

            if (r != 255)
            {
                await EnsureBufferAsync(2, token).ConfigureAwait(false);
                buffer.Span[_pos++] = (byte)'\\';
                buffer.Span[_pos++] = r;
                return;
            }

            ThrowInvalidEscapeCharacter(b);
        }

        private void ThrowInvalidEscapeCharacter(byte b)
        {
            throw new InvalidOperationException("Invalid escape char '" + (char)b + "' numeric value is: " + b);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WriteStringAsync(LazyCompressedStringValue str, CancellationToken token = default)
        {
            var strBuffer = str.DecompressToTempBuffer(out AllocatedMemoryData allocated, _context).Memory;

            try
            {
                var strSrcBuffer = str.MemoryBuffer;

                var size = str.UncompressedSize;
                var escapeSequencePos = str.CompressedSize;
                var numberOfEscapeSequences = BlittableJsonReaderBase.ReadVariableSizeInt(strSrcBuffer.Memory.Span, ref escapeSequencePos);

                // We ensure our buffer will have enough space to deal with the whole string.
                int bufferSize = 2 * numberOfEscapeSequences + size + 2;
                if (bufferSize >= JsonOperationContext.MemoryBuffer.Size)
                    goto WriteLargeCompressedString; // OK, do it the slow way instead.

                await EnsureBufferAsync(bufferSize, token).ConfigureAwait(false);

                _buffer.Memory.Span[_pos++] = Quote;
                while (numberOfEscapeSequences > 0)
                {
                    numberOfEscapeSequences--;
                    var bytesToSkip = BlittableJsonReaderBase.ReadVariableSizeInt(strSrcBuffer.Memory.Span, ref escapeSequencePos);
                    await WriteRawStringAsync(strBuffer, bytesToSkip, token).ConfigureAwait(false);
                    strBuffer = strBuffer.Slice(bytesToSkip);
                    size -= bytesToSkip + 1 /*for the escaped char we skip*/;
                    var b = strBuffer.Span[0];
                    strBuffer = strBuffer.Slice(1);

                    await WriteEscapeCharacterAsync(_buffer.Memory, b, token).ConfigureAwait(false);
                }

                // write remaining (or full string) to the buffer in one shot
                await WriteRawStringAsync(strBuffer, size, token).ConfigureAwait(false);

                _buffer.Memory.Span[_pos++] = Quote;

                return;

            WriteLargeCompressedString:
                await UnlikelyWriteLargeStringAsync(numberOfEscapeSequences, strSrcBuffer.Memory, escapeSequencePos, strBuffer, size, token).ConfigureAwait(false);
            }
            finally
            {
                if (allocated != null) //precaution
                    _context.ReturnMemory(allocated);
            }
        }

        private async ValueTask UnlikelyWriteLargeStringAsync(int numberOfEscapeSequences, Memory<byte> strSrcBuffer, int escapeSequencePos, Memory<byte> strBuffer, int size, CancellationToken token = default)
        {
            await EnsureBufferAsync(1, token).ConfigureAwait(false);
            _buffer.Memory.Span[_pos++] = Quote;
            while (numberOfEscapeSequences > 0)
            {
                numberOfEscapeSequences--;
                var bytesToSkip = BlittableJsonReaderBase.ReadVariableSizeInt(strSrcBuffer.Span, ref escapeSequencePos);
                await WriteRawStringAsync(strBuffer, bytesToSkip, token).ConfigureAwait(false);
                strBuffer = strBuffer.Slice(bytesToSkip);
                size -= bytesToSkip + 1 /*for the escaped char we skip*/;
                var b = strBuffer.Span[0];
                strBuffer = strBuffer.Slice(1);

                await WriteEscapeCharacterAsync(_buffer.Memory, b, token).ConfigureAwait(false);
            }

            // write remaining (or full string) to the buffer in one shot
            await WriteRawStringAsync(strBuffer, size, token).ConfigureAwait(false);

            await EnsureBufferAsync(1, token).ConfigureAwait(false);
            _buffer.Memory.Span[_pos++] = Quote;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WriteRawStringWhichMustBeWithoutEscapeCharsAsync(Memory<byte> buffer, int size, CancellationToken token = default)
        {
            await EnsureBufferAsync(size + 2, token).ConfigureAwait(false);
            _buffer.Memory.Span[_pos++] = Quote;
            await WriteRawStringAsync(buffer, size, token).ConfigureAwait(false);
            _buffer.Memory.Span[_pos++] = Quote;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async ValueTask WriteRawStringAsync(Memory<byte> buffer, int size, CancellationToken token = default)
        {
            if (size < JsonOperationContext.MemoryBuffer.Size)
            {
                await EnsureBufferAsync(size, token).ConfigureAwait(false);
                _buffer.Memory.Span.Slice(0, size).CopyTo(_buffer.Memory.Span.Slice(_pos));
                _pos += size;
                return;
            }

            await UnlikelyWriteLargeRawStringAsync(buffer, size, token).ConfigureAwait(false);
        }

        private async ValueTask UnlikelyWriteLargeRawStringAsync(Memory<byte> buffer, int size, CancellationToken token = default)
        {
            // need to do this in pieces
            var posInStr = 0;
            while (posInStr < size)
            {
                var amountToCopy = Math.Min(size - posInStr, JsonOperationContext.MemoryBuffer.Size);
                await FlushAsync(token).ConfigureAwait(false);
                buffer.Span.Slice(posInStr, amountToCopy).CopyTo(_buffer.Memory.Span);
                posInStr += amountToCopy;
                _pos = amountToCopy;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WriteStartObjectAsync(CancellationToken token = default)
        {
            await EnsureBufferAsync(1, token).ConfigureAwait(false);
            _buffer.Memory.Span[_pos++] = StartObject;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WriteEndArrayAsync(CancellationToken token = default)
        {
            await EnsureBufferAsync(1, token).ConfigureAwait(false);
            _buffer.Memory.Span[_pos++] = EndArray;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WriteStartArrayAsync(CancellationToken token = default)
        {
            await EnsureBufferAsync(1, token).ConfigureAwait(false);
            _buffer.Memory.Span[_pos++] = StartArray;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WriteEndObjectAsync(CancellationToken token = default)
        {
            await EnsureBufferAsync(1, token).ConfigureAwait(false);
            _buffer.Memory.Span[_pos++] = EndObject;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async ValueTask EnsureBufferAsync(int len, CancellationToken token = default)
        {
            if (len >= JsonOperationContext.MemoryBuffer.Size)
                ThrowValueTooBigForBuffer(len);
            if (_pos + len < JsonOperationContext.MemoryBuffer.Size)
                return;

            await FlushAsync(token).ConfigureAwait(false);
        }

        public async ValueTask FlushAsync(CancellationToken token = default)
        {
            if (_stream == null)
                ThrowStreamClosed();
            if (_pos == 0)
                return;
            await _stream.WriteAsync(_buffer.Memory.Slice(0, _pos), token).ConfigureAwait(false);
            _pos = 0;
        }

        private static void ThrowValueTooBigForBuffer(int len)
        {
            // ReSharper disable once NotResolvedInText
            throw new ArgumentOutOfRangeException("len", len, "Length value too big: " + len);
        }

        private void ThrowStreamClosed()
        {
            throw new ObjectDisposedException("The stream was closed already.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WriteNullAsync(CancellationToken token = default)
        {
            await EnsureBufferAsync(4, token).ConfigureAwait(false);
            for (int i = 0; i < 4; i++)
            {
                _buffer.Memory.Span[_pos++] = NullBuffer[i];
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WriteBoolAsync(bool val, CancellationToken token = default)
        {
            await EnsureBufferAsync(5, token).ConfigureAwait(false);
            var buffer = val ? TrueBuffer : FalseBuffer;
            for (int i = 0; i < buffer.Length; i++)
            {
                _buffer.Memory.Span[_pos++] = buffer[i];
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WriteCommaAsync(CancellationToken token = default)
        {
            await EnsureBufferAsync(1, token).ConfigureAwait(false);
            _buffer.Memory.Span[_pos++] = Comma;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WritePropertyNameAsync(LazyStringValue prop, CancellationToken token = default)
        {
            await WriteStringAsync(prop, token: token).ConfigureAwait(false);
            await EnsureBufferAsync(1, token).ConfigureAwait(false);
            _buffer.Memory.Span[_pos++] = Colon;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WritePropertyNameAsync(string prop, CancellationToken token = default)
        {
            var lazyProp = _context.GetLazyStringForFieldWithCaching(prop);
            await WriteStringAsync(lazyProp).ConfigureAwait(false);
            await EnsureBufferAsync(1, token).ConfigureAwait(false);
            _buffer.Memory.Span[_pos++] = Colon;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask WritePropertyNameAsync(StringSegment prop, CancellationToken token = default)
        {
            var lazyProp = _context.GetLazyStringForFieldWithCaching(prop);
            await WriteStringAsync(lazyProp).ConfigureAwait(false);
            await EnsureBufferAsync(1, token).ConfigureAwait(false);
            _buffer.Memory.Span[_pos++] = Colon;
        }

        public async ValueTask WriteIntegerAsync(long val, CancellationToken token = default)
        {
            if (val == 0)
            {
                await EnsureBufferAsync(1, token).ConfigureAwait(false);
                _buffer.Memory.Span[_pos++] = (byte)'0';
                return;
            }

            var localBuffer = _auxiliarBuffer;

            int idx = 0;
            var negative = false;
            var isLongMin = false;
            if (val < 0)
            {
                negative = true;
                if (val == long.MinValue)
                {
                    isLongMin = true;
                    val = long.MaxValue;
                }
                else
                    val = -val; // value is positive now.
            }

            do
            {
                var v = val % 10;
                if (isLongMin)
                {
                    isLongMin = false;
                    v += 1;
                }

                localBuffer.Memory.Span[idx++] = (byte)('0' + v);
                val /= 10;
            }
            while (val != 0);

            if (negative)
                localBuffer.Memory.Span[idx++] = (byte)'-';

            await EnsureBufferAsync(idx, token).ConfigureAwait(false);

            var buffer = _buffer;
            int auxPos = _pos;

            do
                buffer.Memory.Span[auxPos++] = localBuffer.Memory.Span[--idx];
            while (idx > 0);

            _pos = auxPos;
        }

        public async ValueTask WriteDoubleAsync(LazyNumberValue val, CancellationToken token = default)
        {
            if (val.IsNaN())
            {
                await WriteBufferForAsync(NaNBuffer, token).ConfigureAwait(false);
                return;
            }

            if (val.IsPositiveInfinity())
            {
                await WriteBufferForAsync(PositiveInfinityBuffer, token).ConfigureAwait(false);
                return;
            }

            if (val.IsNegativeInfinity())
            {
                await WriteBufferForAsync(NegativeInfinityBuffer, token).ConfigureAwait(false);
                return;
            }

            var lazyStringValue = val.Inner;
            await EnsureBufferAsync(lazyStringValue.Size, token).ConfigureAwait(false);
            await WriteRawStringAsync(lazyStringValue.MemoryBuffer.Memory, lazyStringValue.Size, token).ConfigureAwait(false);
        }

        public async ValueTask WriteBufferForAsync(byte[] buffer, CancellationToken token = default)
        {
            await EnsureBufferAsync(buffer.Length, token).ConfigureAwait(false);
            for (int i = 0; i < buffer.Length; i++)
            {
                _buffer.Memory.Span[_pos++] = buffer[i];
            }
        }

        public async ValueTask WriteDoubleAsync(double val, CancellationToken token = default)
        {
            if (double.IsNaN(val))
            {
                await WriteBufferForAsync(NaNBuffer, token).ConfigureAwait(false);
                return;
            }

            if (double.IsPositiveInfinity(val))
            {
                await WriteBufferForAsync(PositiveInfinityBuffer, token).ConfigureAwait(false);
                return;
            }

            if (double.IsNegativeInfinity(val))
            {
                await WriteBufferForAsync(NegativeInfinityBuffer, token).ConfigureAwait(false);
                return;
            }

            using (var lazyStr = _context.GetLazyString(val.ToString(CultureInfo.InvariantCulture)))
            {
                await EnsureBufferAsync(lazyStr.Size, token).ConfigureAwait(false);
                await WriteRawStringAsync(lazyStr.MemoryBuffer.Memory, lazyStr.Size, token).ConfigureAwait(false);
            }
        }

        public virtual async ValueTask DisposeAsync()
        {
            try
            {
                await FlushAsync().ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
                //we are disposing, so this exception doesn't matter
            }
            // TODO: remove when we update to .net core 3
            // https://github.com/dotnet/corefx/issues/36141
            catch (NotSupportedException e)
            {
                throw new IOException("The stream was closed by the peer.", e);
            }
            finally
            {
                _returnBuffer.Dispose();
                _context.ReturnMemory(_returnAuxiliarBuffer);
            }
        }

        public async ValueTask WriteNewLineAsync(CancellationToken token = default)
        {
            await EnsureBufferAsync(2, token).ConfigureAwait(false);
            _buffer.Memory.Span[_pos++] = (byte)'\r';
            _buffer.Memory.Span[_pos++] = (byte)'\n';
        }

        public async ValueTask WriteStreamAsync(Stream stream, CancellationToken token = default)
        {
            await FlushAsync(token).ConfigureAwait(false);

            while (true)
            {
                _pos = stream.Read(_pinnedBuffer.Memory.Memory.Span);
                if (_pos == 0)
                    break;

                await FlushAsync(token).ConfigureAwait(false);
            }
        }

        public async ValueTask WriteMemoryChunkAsync(Memory<byte> ptr, int size, CancellationToken token = default)
        {
            await FlushAsync(token).ConfigureAwait(false);
            var leftToWrite = size;
            var totalWritten = 0;
            while (leftToWrite > 0)
            {
                var toWrite = Math.Min(JsonOperationContext.MemoryBuffer.Size, leftToWrite);
                ptr.Span.Slice(totalWritten, toWrite).CopyTo(_buffer.Memory.Span);
                _pos += toWrite;
                totalWritten += toWrite;
                leftToWrite -= toWrite;
                await FlushAsync(token).ConfigureAwait(false);
            }
        }
    }
}
