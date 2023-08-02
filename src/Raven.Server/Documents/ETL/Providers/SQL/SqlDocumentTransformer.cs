using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime;
using Jint.Runtime.Interop;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.SQL
{
    internal sealed class SqlDocumentTransformer : EtlTransformer<ToSqlItem, SqlTableWithRecords, EtlStatsScope, EtlPerformanceOperation>
    {
        private static readonly JsValue DefaultVarCharSize = 50;
        
        private readonly Transformation _transformation;
        private readonly SqlEtlConfiguration _config;
        private readonly Dictionary<string, SqlTableWithRecords> _tables;
        private Dictionary<string, Queue<Attachment>> _loadedAttachments;
        private readonly List<SqlEtlTable> _tablesForScript;

        private EtlStatsScope _stats;

        public SqlDocumentTransformer(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, SqlEtlConfiguration config)
            : base(database, context, new PatchRequest(transformation.Script, PatchRequestType.SqlEtl), null)
        {
            _transformation = transformation;
            _config = config;

            var destinationTables = transformation.GetCollectionsFromScript();

            LoadToDestinations = destinationTables;

            _tables = new Dictionary<string, SqlTableWithRecords>(destinationTables.Length, StringComparer.OrdinalIgnoreCase);
            _tablesForScript = new List<SqlEtlTable>(destinationTables.Length);

            // ReSharper disable once ForCanBeConvertedToForeach
            for (var i = 0; i < _config.SqlTables.Count; i++)
            {
                var table = _config.SqlTables[i];

                if (destinationTables.Contains(table.TableName, StringComparer.OrdinalIgnoreCase))
                    _tablesForScript.Add(table);
            }

            if (_transformation.IsLoadingAttachments)
               _loadedAttachments = new Dictionary<string, Queue<Attachment>>(StringComparer.OrdinalIgnoreCase);
        }

        public override void Initialize(bool debugMode)
        {
            base.Initialize(debugMode);
            
            DocumentScript.ScriptEngine.SetValue("varchar",
                new ClrFunctionInstance(DocumentScript.ScriptEngine, "varchar", (value, values) => ToVarcharTranslator(VarcharFunctionCall.AnsiStringType, values)));

            DocumentScript.ScriptEngine.SetValue("nvarchar",
                new ClrFunctionInstance(DocumentScript.ScriptEngine, "nvarchar", (value, values) => ToVarcharTranslator(VarcharFunctionCall.StringType, values)));
        }

        protected override string[] LoadToDestinations { get; }

        protected override void LoadToFunction(string tableName, ScriptRunnerResult cols)
        {
            if (tableName == null)
                ThrowLoadParameterIsMandatory(nameof(tableName));

            var result = cols.TranslateToObject(Context);
            var columns = new List<SqlColumn>(result.Count);
            var prop = new BlittableJsonReaderObject.PropertyDetails();

            for (var i = 0; i < result.Count; i++)
            {
                result.GetPropertyByIndex(i, ref prop);

                var sqlColumn = new SqlColumn
                {
                    Id = prop.Name,
                    Value = prop.Value,
                    Type = prop.Token
                };

                if (_transformation.IsLoadingAttachments && 
                    prop.Token == BlittableJsonToken.String && IsLoadAttachment(prop.Value as LazyStringValue, out var attachmentName))
                {
                    var attachment = _loadedAttachments[attachmentName].Dequeue();

                    sqlColumn.Type = 0;
                    sqlColumn.Value = attachment.Stream;

                    _stats.IncrementBatchSize(attachment.Stream.Length);
                }

                columns.Add(sqlColumn);
            }

            GetOrAdd(tableName).Inserts.Add(new ToSqlItem(Current)
            {
                Columns = columns
            });

            _stats.IncrementBatchSize(result.Size);
        }

        private static unsafe bool IsLoadAttachment(LazyStringValue value, out string attachmentName)
        {
            if (value.Length <= Transformation.AttachmentMarker.Length)
            {
                attachmentName = null;
                return false;
            }

            var buffer = value.Buffer;

            if (*(long*)buffer != 7883660417928814884 || // $attachm
                *(int*)(buffer + 8) != 796159589) // ent/
            {
                attachmentName = null;
                return false;
            }

            attachmentName = value;
            return true;
        }

        protected override void AddLoadedAttachment(JsValue reference, string name, Attachment attachment)
        {
            var strReference = reference.ToString();
            if (_loadedAttachments.TryGetValue(strReference, out var loadedAttachments) == false)
            {
                loadedAttachments = new Queue<Attachment>();
                _loadedAttachments.Add(strReference, loadedAttachments);
            }

            loadedAttachments.Enqueue(attachment);
        }

        protected override void AddLoadedCounter(JsValue reference, string name, long value)
        {
            throw new NotSupportedException("Counters aren't supported by SQL ETL");
        }

        protected override void AddLoadedTimeSeries(JsValue reference, string name, IEnumerable<SingleResult> entries)
        {
            throw new NotSupportedException("Time series aren't supported by SQL ETL");
        }

        private SqlTableWithRecords GetOrAdd(string tableName)
        {
            if (_tables.TryGetValue(tableName, out SqlTableWithRecords table) == false)
            {
                var sqlEtlTable = _config.SqlTables.Find(x => x.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase));

                if (sqlEtlTable == null)
                    ThrowTableNotDefinedInConfig(tableName);

                _tables[tableName] =
                    table = new SqlTableWithRecords(sqlEtlTable);
            }

            return table;
        }

        [DoesNotReturn]
        private static void ThrowTableNotDefinedInConfig(string tableName)
        {
            throw new InvalidOperationException($"Table '{tableName}' was not defined in the configuration of SQL ETL task");
        }

        public override IEnumerable<SqlTableWithRecords> GetTransformedResults()
        {
            return _tables.Values;
        }

        public override void Transform(ToSqlItem item, EtlStatsScope stats, EtlProcessState state)
        {
            _stats = stats;

            if (item.IsDelete == false)
            {
                Current = item;

                DocumentScript.Run(Context, Context, "execute", new object[] { Current.Document }).Dispose();
            }

            // ReSharper disable once ForCanBeConvertedToForeach
            for (int i = 0; i < _tablesForScript.Count; i++)
            {
                // delete all the rows that might already exist there

                var sqlTable = _tablesForScript[i];

                if (sqlTable.InsertOnlyMode)
                    continue;

                GetOrAdd(sqlTable.TableName).Deletes.Add(item);
            }
        }

        private JsValue ToVarcharTranslator(JsValue type, JsValue[] args)
        {
            if (args[0].IsString() == false)
                throw new InvalidOperationException("varchar() / nvarchar(): first argument must be a string");

            var sizeSpecified = args.Length > 1;

            if (sizeSpecified && args[1].IsNumber() == false)
                throw new InvalidOperationException("varchar() / nvarchar(): second argument must be a number");

            var item = new JsObject(DocumentScript.ScriptEngine);

            item.FastSetDataProperty(nameof(VarcharFunctionCall.Type), type);
            item.FastSetDataProperty(nameof(VarcharFunctionCall.Value), args[0]);
            item.FastSetDataProperty(nameof(VarcharFunctionCall.Size), sizeSpecified ? args[1] : DefaultVarCharSize);

            return item;
        }

        internal sealed class VarcharFunctionCall
        {
            public static JsValue AnsiStringType = DbType.AnsiString.ToString();
            public static JsValue StringType = DbType.String.ToString();

            public DbType Type { get; set; }
            public object Value { get; set; }
            public int Size { get; set; }

            private VarcharFunctionCall()
            {

            }
        }
    }
}
