using System;
using System.Collections.Generic;
using Corax.Global;
using Sparrow.Server;
using Voron;

namespace Corax.Mappings;

public readonly struct FieldMetadata
{
    public readonly Slice FieldName;
    public readonly Slice TermLengthSumName;
    public readonly int FieldId;
    public readonly FieldIndexingMode Mode;
    public readonly Analyzer Analyzer;
    public readonly bool HasBoost;

    private FieldMetadata(Slice fieldName, Slice termLengthSumName, int fieldId, FieldIndexingMode mode, Analyzer analyzer, bool hasBoost = false)
    {
        TermLengthSumName = termLengthSumName;
        FieldName = fieldName;
        FieldId = fieldId;
        Mode = mode;
        Analyzer = analyzer;
        HasBoost = hasBoost;
    }

    public FieldMetadata GetNumericFieldMetadata<T>(ByteStringContext allocator)
    {
        Slice numericTree = default;

        if (typeof(T) == typeof(Slice))
            return this;

        else if (typeof(T) == typeof(long))
            Slice.From(allocator, $"{FieldName}-L", ByteStringType.Immutable, out numericTree);

        else if (typeof(T) == typeof(double))
            Slice.From(allocator, $"{FieldName}-D", ByteStringType.Immutable, out numericTree);
        else
            throw new NotSupportedException(typeof(T).FullName);
        
        return new FieldMetadata(numericTree, default, FieldId, Mode, Analyzer);
    }
    
    public bool Equals(FieldMetadata other)
    {
        return FieldId == other.FieldId && SliceComparer.CompareInline(FieldName, other.FieldName) == 0;
    }

    public static FieldMetadata Build(ByteStringContext allocator, string fieldName, int fieldId, FieldIndexingMode mode, Analyzer analyzer, bool hasBoost = false)
    {
        Slice.From(allocator, fieldName, ByteStringType.Immutable, out var fieldNameAsSlice);
        Slice sumName = default;
        if (hasBoost)
            Slice.From(allocator, $"{fieldName}-C", ByteStringType.Immutable, out sumName);

        return new(fieldNameAsSlice, sumName, fieldId, mode, analyzer, hasBoost);
    }

    public static FieldMetadata Build(Slice fieldName, Slice termLengthSumName, int fieldId, FieldIndexingMode mode, Analyzer analyzer, bool hasBoost = false) => new(fieldName, termLengthSumName, fieldId, mode, analyzer, hasBoost: hasBoost);

    public FieldMetadata ChangeAnalyzer(FieldIndexingMode mode, Analyzer analyzer = null)
    {
        return new FieldMetadata(FieldName, TermLengthSumName, FieldId, mode, analyzer ?? Analyzer, HasBoost);
    }

    public FieldMetadata ChangeScoringMode(bool hasBoost)
    {
        if (HasBoost == hasBoost) return this;
        
        return new FieldMetadata(FieldName, TermLengthSumName, FieldId, Mode, Analyzer, hasBoost);

    }

    public bool IsDynamic => FieldId == Constants.IndexWriter.DynamicField;
    
    public override string ToString()
    {
        return $"Field name: '{FieldName}' | Field id: {FieldId} | Indexing mode: {Mode} | Analyzer {Analyzer?.GetType()} | Has boost: {HasBoost}";
    }
}


public sealed class FieldMetadataComparer : IEqualityComparer<FieldMetadata>
{
    public static readonly FieldMetadataComparer Instance = new();
    
    public bool Equals(FieldMetadata x, FieldMetadata y)
    {
        return SliceComparer.Equals(x.FieldName, y.FieldName) && x.FieldId == y.FieldId && x.Mode == y.Mode && x.Analyzer == y.Analyzer;
    }

    public int GetHashCode(FieldMetadata obj)
    {
        return HashCode.Combine(obj.FieldName, obj.FieldId, (int)obj.Mode, obj.Analyzer);
    }
}
