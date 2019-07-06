/// <reference path="../../../../typings/tsd.d.ts"/>
import configuration = require("configuration");
import restorePoint = require("models/resources/creation/restorePoint");
import clusterNode = require("models/database/cluster/clusterNode");
import getRestorePointsCommand = require("commands/resources/getRestorePointsCommand");
import generalUtils = require("common/generalUtils");
import recentError = require("common/notifications/models/recentError");
import validateNameCommand = require("commands/resources/validateNameCommand");
import validateOfflineMigration = require("commands/resources/validateOfflineMigration");
import storageKeyProvider = require("common/storage/storageKeyProvider");
import setupEncryptionKey = require("viewmodels/resources/setupEncryptionKey");
import licenseModel = require("models/auth/licenseModel");

class databaseCreationModel {
    static unknownDatabaseName = "Unknown Database";
    
    static storageExporterPathKeyName = storageKeyProvider.storageKeyFor("storage-exporter-path");

    readonly configurationSections: Array<availableConfigurationSection> = [
        {
            name: "Data source",
            id: "legacyMigration",
            alwaysEnabled: true,
            disableToggle: ko.observable<boolean>(false),
            enabled: ko.observable<boolean>(true)
        },
        {
            name: "Backup source",
            id: "restore",
            alwaysEnabled: true,
            disableToggle: ko.observable<boolean>(false),
            enabled: ko.observable<boolean>(true)
        },
        {
            name: "Encryption",
            id: "encryption",
            alwaysEnabled: false,
            disableToggle: ko.observable<boolean>(false),
            enabled: ko.observable<boolean>(false)
        },
        {
            name: "Replication",
            id: "replication",
            alwaysEnabled: true,
            disableToggle: ko.observable<boolean>(false),
            enabled: ko.observable<boolean>(true)
        },
        {
            name: "Path",
            id: "path",
            alwaysEnabled: true,
            disableToggle: ko.observable<boolean>(false),
            enabled: ko.observable<boolean>(true)
        }
    ];

    spinners = {
        fetchingRestorePoints: ko.observable<boolean>(false)
    };
    
    lockActiveTab = ko.observable<boolean>(false);

    name = ko.observable<string>("");

    creationMode: dbCreationMode = null;
    isFromBackupOrFromOfflineMigration: boolean;
    canCreateEncryptedDatabases: KnockoutObservable<boolean>;

    restore = {
        source: ko.observable<restoreSource>("serverLocal"),
        cloudCredentials: ko.observable<string>(),
        backupDirectory: ko.observable<string>().extend({ throttle: 500 }),
        backupDirectoryError: ko.observable<string>(null),
        lastFailedBackupDirectory: null as string,
        selectedRestorePoint: ko.observable<restorePoint>(),
        selectedRestorePointText: ko.pureComputed<string>(() => {
            const restorePoint = this.restore.selectedRestorePoint();
            if (!restorePoint) {
                return null;
            }

            const text: string = `${restorePoint.dateTime}, ${restorePoint.backupType()} Backup`;
            return text;
        }),
        restorePoints: ko.observable<Array<{ databaseName: string, databaseNameTitle: string, restorePoints: restorePoint[] }>>([]),
        isFocusOnBackupDirectory: ko.observable<boolean>(),
        restorePointsCount: ko.observable<number>(0),
        disableOngoingTasks: ko.observable<boolean>(false),
        skipIndexes: ko.observable<boolean>(false),
        requiresEncryption: undefined as KnockoutComputed<boolean>,
        backupEncryptionKey: ko.observable<string>(),
        decodedS3Credentials: ko.observable<Raven.Client.Documents.Operations.Backups.S3Settings>()
    };
    
    restoreValidationGroup = ko.validatedObservable({ 
        selectedRestorePoint: this.restore.selectedRestorePoint,
        backupDirectory: this.restore.backupDirectory,
        backupEncryptionKey: this.restore.backupEncryptionKey
    });
    
    legacyMigration = {
        showAdvanced: ko.observable<boolean>(false),
        
        isEncrypted: ko.observable<boolean>(false),
        isCompressed: ko.observable<boolean>(false),

        dataDirectory: ko.observable<string>(),
        dataDirectoryHasFocus: ko.observable<boolean>(false),
        dataExporterFullPath: ko.observable<string>(),
        dataExporterFullPathHasFocus: ko.observable<boolean>(false),
        
        batchSize: ko.observable<number>(),
        sourceType: ko.observable<legacySourceType>(),
        journalsPath: ko.observable<string>(),
        journalsPathHasFocus: ko.observable<boolean>(false),
        encryptionKey: ko.observable<string>(),
        encryptionAlgorithm: ko.observable<string>(),
        encryptionKeyBitsSize: ko.observable<number>()
    };
    
    legacyMigrationValidationGroup = ko.validatedObservable({
        dataDirectory: this.legacyMigration.dataDirectory,
        dataExporterFullPath: this.legacyMigration.dataExporterFullPath,
        sourceType: this.legacyMigration.sourceType,
        journalsPath: this.legacyMigration.journalsPath,
        encryptionKey: this.legacyMigration.encryptionKey,
        encryptionAlgorithm: this.legacyMigration.encryptionAlgorithm,
        encryptionKeyBitsSize: this.legacyMigration.encryptionKeyBitsSize
    });
    
    replication = {
        replicationFactor: ko.observable<number>(2),
        manualMode: ko.observable<boolean>(false),
        dynamicMode: ko.observable<boolean>(true),
        nodes: ko.observableArray<clusterNode>([])
    };

    replicationValidationGroup = ko.validatedObservable({
        replicationFactor: this.replication.replicationFactor,
        nodes: this.replication.nodes
    });

    path = {
        dataPath: ko.observable<string>(),
        dataPathHasFocus: ko.observable<boolean>(false)
    };

    pathValidationGroup = ko.validatedObservable({
        dataPath: this.path.dataPath,
    });

    encryption = {
        key: ko.observable<string>(),
        confirmation: ko.observable<boolean>(false)
    };
   
    encryptionValidationGroup = ko.validatedObservable({
        key: this.encryption.key,
        confirmation: this.encryption.confirmation
    });

    globalValidationGroup = ko.validatedObservable({
        name: this.name,
    });

    constructor(mode: dbCreationMode, canCreateEncryptedDatabases: KnockoutObservable<boolean>) {
        this.creationMode = mode;
        this.canCreateEncryptedDatabases = canCreateEncryptedDatabases;
        this.isFromBackupOrFromOfflineMigration = mode !== "newDatabase";
        
        const legacyMigrationConfig = this.configurationSections.find(x => x.id === "legacyMigration");
        legacyMigrationConfig.validationGroup = this.legacyMigrationValidationGroup;
        
        const restoreConfig = this.configurationSections.find(x => x.id === "restore");
        restoreConfig.validationGroup = this.restoreValidationGroup;
        
        const encryptionConfig = this.getEncryptionConfigSection();
        encryptionConfig.validationGroup = this.encryptionValidationGroup;

        const replicationConfig = this.configurationSections.find(x => x.id === "replication");
        replicationConfig.validationGroup = this.replicationValidationGroup;

        const pathConfig = this.configurationSections.find(x => x.id === "path");
        pathConfig.validationGroup = this.pathValidationGroup;

        encryptionConfig.enabled.subscribe(() => {
            if (this.creationMode === "newDatabase") {
                this.replication.replicationFactor(this.replication.nodes().length);
            }
        });
        
        this.replication.nodes.subscribe(nodes => {
            this.replication.replicationFactor(nodes.length);
        });

        this.replication.replicationFactor.subscribe(factor => {
            if (factor === 1) {
                this.replication.dynamicMode(false);
            }
        });

        this.restore.backupDirectory.subscribe(() => {
            if (this.restore.source() === "serverLocal") {
                this.fetchRestorePoints(true);
            }
        });
        
        this.restore.decodedS3Credentials.subscribe((credentials) => {
            if (this.restore.source() === "cloud" && credentials) {
                this.fetchRestorePoints(false);
            }
        });
        
        this.restore.cloudCredentials.subscribe((credentials) => {
            this.tryDecodeS3Credentials(credentials);
        });

        let isFirst = true;
        this.restore.isFocusOnBackupDirectory.subscribe(hasFocus => {
            if (isFirst) {
                isFirst = false;
                return;
            }

            if (this.creationMode !== "restore")
                return;

            if (hasFocus)
                return;

            const backupDirectory = this.restore.backupDirectory();
            if (!this.restore.backupDirectory.isValid() &&
                backupDirectory === this.restore.lastFailedBackupDirectory)
                return;

            if (!backupDirectory)
                return;

            this.fetchRestorePoints(false);
        });
        
        this.restore.selectedRestorePoint.subscribe(restorePoint => {
            const canCreateEncryptedDbs = this.canCreateEncryptedDatabases();
            
            const encryptionSection = this.getEncryptionConfigSection();
            this.lockActiveTab(true);
            try {
                if (restorePoint) {
                    if (restorePoint.isEncrypted) {
                        
                        if (restorePoint.isSnapshotRestore) {
                            // encrypted snapshot - we are forced to encrypt newly created database 
                            // it requires license and https
                            
                            encryptionSection.enabled(true);
                            encryptionSection.disableToggle(true);
                        } else {
                            // encrypted backup - we need license and https for encrypted db
                            
                            encryptionSection.enabled(canCreateEncryptedDbs);
                            encryptionSection.disableToggle(!canCreateEncryptedDbs);
                        }
                    } else { //backup is not encrypted
                        if (restorePoint.isSnapshotRestore) {
                            // not encrypted snapshot - we can not create encrypted db
                            
                            encryptionSection.enabled(false);
                            encryptionSection.disableToggle(true);
                        } else {
                            // not encrypted backup - we need license and https for encrypted db
                            
                            encryptionSection.enabled(false);
                            encryptionSection.disableToggle(!canCreateEncryptedDbs); 
                        }
                    }
                } else {
                    encryptionSection.disableToggle(false);
                }
            } finally {
                this.lockActiveTab(false);    
            }
        });
        
        _.bindAll(this, "useRestorePoint", "dataPathHasChanged", "backupPathHasChanged", 
            "legacyMigrationDataDirectoryHasChanged", "dataExporterPathHasChanged", "journalsPathHasChanged");
    }
    
    dataPathHasChanged(value: string) {
        this.path.dataPath(value);
        
        // try to continue autocomplete flow
        this.path.dataPathHasFocus(true);
    }

    dataExporterPathHasChanged(value: string) {
        this.legacyMigration.dataExporterFullPath(value);
        
        //try to continue autocomplete flow
        this.legacyMigration.dataExporterFullPathHasFocus(true);
    }
    
    backupPathHasChanged(value: string) {
        this.restore.backupDirectory(value);
        
        // try to continue autocomplete flow
        this.restore.isFocusOnBackupDirectory(true);
    }

    legacyMigrationDataDirectoryHasChanged(value: string) {
        this.legacyMigration.dataDirectory(value);
        
        //try to continue autocomplete flow
        this.legacyMigration.dataDirectoryHasFocus(true);
    }
    
    journalsPathHasChanged(value: string) {
        this.legacyMigration.journalsPath(value);
        
        //try to continue autocomplete flow
        this.legacyMigration.journalsPathHasFocus(true);
    }

    private tryDecodeS3Credentials(credentials: string) {
        try {
            const decoded = atob(credentials);
            const json = JSON.parse(decoded);
            
            //TODO: do some duck typing to check if we have correct format
            
            this.restore.decodedS3Credentials(json);
        } catch (e) {
            console.warn(e);
        }
    }

    private createRestorePointCommand(skipReportingError: boolean) {
        switch (this.restore.source()) {
            case "serverLocal":
                return getRestorePointsCommand.forServerLocal(this.restore.backupDirectory(), skipReportingError);
            case "cloud":
                return getRestorePointsCommand.forS3Backup(this.restore.decodedS3Credentials(), skipReportingError);
        }
    }
    
    private fetchRestorePoints(skipReportingError: boolean) {
        if (!skipReportingError) {
            this.spinners.fetchingRestorePoints(true);
        }

        this.createRestorePointCommand(skipReportingError)
            .execute()
            .done((restorePoints: Raven.Server.Documents.PeriodicBackup.Restore.RestorePoints) => {
                const groups: Array<{ databaseName: string, databaseNameTitle: string, restorePoints: restorePoint[] }> = [];
                restorePoints.List.forEach(rp => {
                    const databaseName = rp.DatabaseName = rp.DatabaseName ? rp.DatabaseName : databaseCreationModel.unknownDatabaseName;
                    if (!groups.find(x => x.databaseName === databaseName)) {
                        const title = databaseName !== databaseCreationModel.unknownDatabaseName ? "Database Name" : "Unidentified folder format name";
                        groups.push({ databaseName: databaseName, databaseNameTitle: title, restorePoints: [] });
                    }

                    const group = groups.find(x => x.databaseName === databaseName);
                    group.restorePoints.push(new restorePoint(rp));
                });

                this.restore.restorePoints(groups);
                this.restore.selectedRestorePoint(null);
                this.restore.backupEncryptionKey("");
                this.restore.backupDirectoryError(null);
                this.restore.lastFailedBackupDirectory = null;
                this.restore.restorePointsCount(restorePoints.List.length);
            })
            .fail((response: JQueryXHR) => {
                const messageAndOptionalException = recentError.tryExtractMessageAndException(response.responseText);
                this.restore.backupDirectoryError(generalUtils.trimMessage(messageAndOptionalException.message));
                this.restore.lastFailedBackupDirectory = this.restore.backupDirectory();
                this.restore.restorePoints([]);
                this.restore.backupEncryptionKey("");
                this.restore.restorePointsCount(0);
            })
            .always(() => this.spinners.fetchingRestorePoints(false));
    }

    getEncryptionConfigSection() {
        return this.configurationSections.find(x => x.id === "encryption");
    }

    protected setupPathValidation(observable: KnockoutObservable<string>, name: string) {
        const maxLength = 248;

        const rg1 = /^[^*?"<>\|]*$/; // forbidden characters * ? " < > |
        const rg3 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names
        const invalidPrefixCheck = (dbName: string) => {
            const dbToLower = dbName ? dbName.toLocaleLowerCase() : "";
            return !dbToLower.startsWith("~") && !dbToLower.startsWith("$home") && !dbToLower.startsWith("appdrive:");
        };

        observable.extend({
            maxLength: {
                params: maxLength,
                message: `Path name for '${name}' can't exceed ${maxLength} characters!`
            },
            validation: [{
                validator: (val: string) => rg1.test(val),
                message: `{0} path can't contain any of the following characters: * ? " < > |`,
                params: name
            },
            {
                validator: (val: string) => !rg3.test(val),
                message: `The name {0} is forbidden for use!`,
                params: this.name
            }, 
            {
                validator: (val: string) => invalidPrefixCheck(val),
                message: "The path is illegal! Paths in RavenDB can't start with 'appdrive:', '~' or '$home'"
            }]
        });
    }

    setupValidation(databaseDoesntExist: (name: string) => boolean, maxReplicationFactor: number) {
        this.setupPathValidation(this.path.dataPath, "Data");

        const checkDatabaseName = (val: string,
                                   params: any,
                                   callback: (currentValue: string, result: string | boolean) => void) => {
            new validateNameCommand('Database', val)
                .execute()
                .done((result) => {
                    if (result.IsValid) {
                        callback(this.name(), true);
                    } else {
                        callback(this.name(), result.ErrorMessage);
                    }
                })
        };
        
        this.name.extend({
            required: true,
            validation: [
                {
                    validator: (name: string) => databaseDoesntExist(name),
                    message: "Database already exists"
                },
                {
                    async: true,
                    validator: generalUtils.debounceAndFunnel(checkDatabaseName)
                }]
        });
        
        this.setupReplicationValidation(maxReplicationFactor);
        this.setupEncryptionValidation();
        
        if (this.creationMode === "restore") {
            this.setupRestoreValidation();
        }
        if (this.creationMode === "legacyMigration") {
            this.setupLegacyMigrationValidation();    
        }
        
    }
    
    private setupReplicationValidation(maxReplicationFactor: number) {
        this.replication.nodes.extend({
            validation: [{
                validator: (val: Array<clusterNode>) => !this.replication.manualMode() || this.replication.replicationFactor() > 0,
                message: `Please select at least one node.`
            }]
        });

        this.replication.replicationFactor.extend({
            required: true,
            validation: [
                {
                    validator: (val: number) => val >= 1 || this.replication.manualMode(),
                    message: `Replication factor must be at least 1.`
                },
                {
                    validator: (val: number) => val <= maxReplicationFactor,
                    message: `Max available nodes: {0}`,
                    params: maxReplicationFactor
                }
            ],
            digit: true
        });
    }
    
    private setupRestoreValidation() {
        this.restore.backupEncryptionKey.extend({
            required: {
                onlyIf: () => {
                    const restorePoint = this.restore.selectedRestorePoint();
                    return restorePoint ? restorePoint.isEncrypted : false;
                }
            },
            base64: true
        });
        
        this.restore.source.extend({
            required: true
        });
        
        this.restore.cloudCredentials.extend({
            required: {
                onlyIf: () => this.restore.source() === "cloud"
            }
        });
        
        this.restore.backupDirectory.extend({
            required: {
                onlyIf: () => this.creationMode === "restore" 
                    && this.restore.restorePoints().length === 0
                    && this.restore.source() !== "cloud"
            },
            validation: [
                {
                    validator: (_: string) => {
                        return this.creationMode === "restore" && !this.restore.backupDirectoryError();
                    },
                    message: "Couldn't fetch restore points, {0}",
                    params: this.restore.backupDirectoryError
                }
            ]
        });

        this.restore.selectedRestorePoint.extend({
            required: {
                onlyIf: () => this.creationMode === "restore"
            },
            validation: [
                {
                    validator: (restorePoint: restorePoint) => {
                        const isEncryptedSnapshot = restorePoint.isEncrypted && restorePoint.isSnapshotRestore;
                        if (isEncryptedSnapshot) {
                            // check if license supports that
                            return licenseModel.licenseStatus() && licenseModel.licenseStatus().HasEncryption;
                        }
                        return true;
                    },
                    message: "License doesn't support storage encryption"
                }
            ]
        });
    }
    
    private setupEncryptionValidation() {
        setupEncryptionKey.setupKeyValidation(this.encryption.key);
        setupEncryptionKey.setupConfirmationValidation(this.encryption.confirmation);
    }
    
    private getSavedDataExporterPath() {
        return localStorage.getItem(databaseCreationModel.storageExporterPathKeyName);
    }
    
    private setupLegacyMigrationValidation() {
        const migration = this.legacyMigration;

        const checkDataExporterFullPath = (val: string, params: any, callback: (currentValue: string, result: string | boolean) => void) => {
            validateOfflineMigration.validateMigratorPath(migration.dataExporterFullPath())
                .execute()
                .done((response: Raven.Server.Web.Studio.StudioTasksHandler.OfflineMigrationValidation) => {
                    callback(migration.dataExporterFullPath(), response.IsValid || response.ErrorMessage);
                });
        };

        migration.dataExporterFullPath.extend({
            required: true,
            validation: {
                async: true,
                validator: generalUtils.debounceAndFunnel(checkDataExporterFullPath)
            }
        });
        
        const savedPath = this.getSavedDataExporterPath();
        if (savedPath) {
            migration.dataExporterFullPath(savedPath);
        }
        
        migration.dataExporterFullPath.subscribe(path => {
            localStorage.setItem(databaseCreationModel.storageExporterPathKeyName, path);
        });
        
        const checkDataDir = (val: string, params: any, callback: (currentValue: string, result: string | boolean) => void) => {
            validateOfflineMigration.validateDataDir(migration.dataDirectory())
                .execute()
                .done((response: Raven.Server.Web.Studio.StudioTasksHandler.OfflineMigrationValidation) => {
                    callback(migration.dataDirectory(), response.IsValid || response.ErrorMessage);
                });
        };

        migration.dataDirectory.extend({
            required: true,
            validation: {
                async: true,
                validator: generalUtils.debounceAndFunnel(checkDataDir)
            }
        });

        migration.sourceType.extend({
            required: true
        });

        migration.encryptionKey.extend({
            required: {
                onlyIf: () => migration.isEncrypted()
            }
        });

        migration.encryptionAlgorithm.extend({
            required: {
                onlyIf: () => migration.isEncrypted()
            }
        });

        migration.encryptionKeyBitsSize.extend({
            required: {
                onlyIf: () => migration.isEncrypted()
            }
        });
    }

    private topologyToDto(): Raven.Client.ServerWide.DatabaseTopology {
        const topology = {
            DynamicNodesDistribution: this.replication.dynamicMode()
        } as Raven.Client.ServerWide.DatabaseTopology;

        if (this.replication.manualMode()) {
            const nodes = this.replication.nodes();
            topology.Members = nodes.map(node => node.tag());
        }
        return topology;
    }

    useRestorePoint(restorePoint: restorePoint) {
        this.restore.selectedRestorePoint(restorePoint);
    }

    getRestorePointTitle(restorePoint: restorePoint) {
        return restorePoint.dateTime;
    }

    toDto(): Raven.Client.ServerWide.DatabaseRecord {
        const settings: dictionary<string> = {};
        const dataDir = _.trim(this.path.dataPath());

        if (dataDir) {
            settings[configuration.core.dataDirectory] = dataDir;
        }

        return {
            DatabaseName: this.name(),
            Settings: settings,
            Disabled: false,
            Encrypted: this.getEncryptionConfigSection().enabled(),
            Topology: this.topologyToDto()
        } as Raven.Client.ServerWide.DatabaseRecord;
    }

    toRestoreDocumentDto(): Raven.Client.Documents.Operations.Backups.RestoreBackupConfigurationBase {
        const dataDirectory = _.trim(this.path.dataPath()) || null;

        const restorePoint = this.restore.selectedRestorePoint();
        const encryptDb = this.getEncryptionConfigSection().enabled();
        
        let encryptionSettings = null as Raven.Client.Documents.Operations.Backups.BackupEncryptionSettings;
        let databaseEncryptionKey = null;
        
        if (restorePoint.isEncrypted) {
            if (restorePoint.isSnapshotRestore) {
                if (encryptDb) {
                    encryptionSettings = {
                        EncryptionMode: "UseDatabaseKey",
                        Key: null
                    };
                    databaseEncryptionKey = this.restore.backupEncryptionKey();
                }
            } else { // backup of type backup
                encryptionSettings = {
                    EncryptionMode: "UseProvidedKey",
                    Key: this.restore.backupEncryptionKey()
                };
                
                if (encryptDb) {
                    databaseEncryptionKey = this.encryption.key();
                }
            }
        } else { // backup is not encrypted
            if (!restorePoint.isSnapshotRestore && encryptDb) {
                databaseEncryptionKey = this.encryption.key();
            }
        }
        
        const baseConfiguration = {
            DatabaseName: this.name(),
            DisableOngoingTasks: this.restore.disableOngoingTasks(),
            SkipIndexes: this.restore.skipIndexes(),
            LastFileNameToRestore: restorePoint.fileName,
            DataDirectory: dataDirectory,
            EncryptionKey: databaseEncryptionKey,
            BackupEncryptionSettings: encryptionSettings
        } as Raven.Client.Documents.Operations.Backups.RestoreBackupConfigurationBase;

        switch (this.restore.source()) {
            case "serverLocal" :
                const localConfiguration = baseConfiguration as Raven.Client.Documents.Operations.Backups.RestoreBackupConfiguration;
                localConfiguration.BackupLocation = restorePoint.location;
                (localConfiguration as any as restoreTypeAware).Type = "Local" as Raven.Client.Documents.Operations.Backups.RestoreType; 
                return localConfiguration;
            case "cloud":
                const s3Configuration = baseConfiguration as Raven.Client.Documents.Operations.Backups.RestoreFromS3Configuration;
                s3Configuration.Settings = this.restore.decodedS3Credentials();
                (s3Configuration as any as restoreTypeAware).Type = "S3" as Raven.Client.Documents.Operations.Backups.RestoreType;
                return s3Configuration;
            default:
                throw new Error("Unhandled source: " + this.restore.source());
        }
    }
    
    toOfflineMigrationDto(): Raven.Client.ServerWide.Operations.Migration.OfflineMigrationConfiguration {
        const migration = this.legacyMigration;
        return {
            DataDirectory: migration.dataDirectory(),
            DataExporterFullPath: migration.dataExporterFullPath(),
            BatchSize: migration.batchSize() || null,
            IsRavenFs: migration.sourceType() === "ravenfs",
            IsCompressed: migration.isCompressed(),
            JournalsPath: migration.journalsPath(),
            DatabaseRecord: this.toDto(),
            EncryptionKey: migration.isEncrypted() ? migration.encryptionKey() : undefined,
            EncryptionAlgorithm: migration.isEncrypted() ? migration.encryptionAlgorithm() : undefined,
            EncryptionKeyBitsSize: migration.isEncrypted() ? migration.encryptionKeyBitsSize() : undefined,
            OutputFilePath: null
        } as Raven.Client.ServerWide.Operations.Migration.OfflineMigrationConfiguration;
    }
}

export = databaseCreationModel;
