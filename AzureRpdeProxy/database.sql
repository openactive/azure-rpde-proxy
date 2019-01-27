IF EXISTS ( SELECT * 
            FROM   sysobjects 
            WHERE  id = object_id(N'[dbo].[UPDATE_ITEM]') 
                   and OBJECTPROPERTY(id, N'IsProcedure') = 1 )
            BEGIN
               PRINT '    Dropping stored procedure'
                        DROP PROCEDURE [dbo].[UPDATE_ITEM]
END

GO

IF EXISTS ( SELECT * 
            FROM   sysobjects 
            WHERE  id = object_id(N'[dbo].[UPDATE_ITEM_BATCH]') 
                   and OBJECTPROPERTY(id, N'IsProcedure') = 1 )
            BEGIN
               PRINT '    Dropping stored procedure'
                        DROP PROCEDURE [dbo].[UPDATE_ITEM_BATCH]
END

GO


IF EXISTS ( SELECT * 
            FROM   sysobjects 
            WHERE  id = object_id(N'[dbo].[READ_ITEM_PAGE]') 
                   and OBJECTPROPERTY(id, N'IsProcedure') = 1 )
            BEGIN
               PRINT '    Dropping stored procedure'
                        DROP PROCEDURE [dbo].[READ_ITEM_PAGE]
END


GO

IF EXISTS ( SELECT * 
            FROM   sysobjects 
            WHERE  id = object_id(N'[dbo].[PRUNE_STALE_ITEMS]') 
                   and OBJECTPROPERTY(id, N'IsProcedure') = 1 )
            BEGIN
               PRINT '    Dropping stored procedure'
                        DROP PROCEDURE [dbo].[PRUNE_STALE_ITEMS]
END

GO

IF EXISTS ( SELECT * 
            FROM   sysobjects 
            WHERE  id = object_id(N'[dbo].[DELETE_SOURCE]') 
                   and OBJECTPROPERTY(id, N'IsProcedure') = 1 )
            BEGIN
               PRINT '    Dropping stored procedure'
                        DROP PROCEDURE [dbo].[DELETE_SOURCE]
END

GO

IF OBJECT_ID('[dbo].[feeds]') IS NOT NULL DROP TABLE [dbo].[feeds]
CREATE TABLE [dbo].[feeds]
(
	[source] varchar (50) NOT NULL,
    [url] nvarchar(max) NOT NULL,
    [datasetUrl] varchar(max) NOT NULL,
	[initialFeedState] varchar(max) NOT NULL,
	PRIMARY KEY NONCLUSTERED ([source])
)

GO

IF OBJECT_ID('[dbo].[items]') IS NOT NULL DROP TABLE [dbo].[items]
CREATE TABLE [dbo].[items]
(
	[source] varchar (50) NOT NULL,
    [id] nvarchar(50) NOT NULL,
    [modified] bigint NOT NULL,
	[kind] varchar(50) NOT NULL,
	[deleted] bit NOT NULL,
	[data] varchar(max) NULL,
	[expiry] DateTime NULL,
	PRIMARY KEY NONCLUSTERED ([source], [id])
)

CREATE NONCLUSTERED INDEX IX_items_modified ON [dbo].[items]([source] ASC, [modified] ASC, [id] ASC) INCLUDE
    ([kind]
    ,[deleted]
	,[data]
	);

CREATE NONCLUSTERED INDEX IX_items_updated ON [dbo].[items]([expiry] ASC) INCLUDE
    ([source]
	,[id]
	);

GO

IF type_id('[dbo].[ItemTableType]') IS NOT NULL
        DROP TYPE [dbo].[ItemTableType];
	
GO

CREATE TYPE ItemTableType AS TABLE 
(   [source] varchar (50),
    [id] nvarchar(50),
    [modified] bigint,
	[kind] varchar(50),
	[deleted] bit,
	[data] varchar(max),
	[expiry] datetime );

GO


CREATE PROCEDURE [dbo].[READ_DATASETS]
AS
BEGIN
	SELECT DISTINCT [datasetUrl] from [dbo].[feeds]
END

GO

CREATE PROCEDURE [dbo].[UPDATE_ITEM]
(
	@source varchar (50),
    @id nvarchar(50),
    @modified bigint,
	@kind varchar(50),
	@deleted bit,
	@data varchar(max),
	@expiry datetime
)
AS
BEGIN
	IF EXISTS (SELECT 1 FROM [dbo].[items] t WHERE t.[source] = @source AND t.[id] = @id)
		UPDATE [dbo].[items] SET
			[modified] = @modified,
			[kind] = @kind,
			[deleted] = @deleted,
			[data] = @data,
			[expiry] = @expiry
			WHERE [source] = @source AND [id] = @id AND @modified >= [modified]
	ELSE
		INSERT INTO [dbo].[items] ([source], [id], [modified], [kind], [deleted], [data], [expiry]) 
		VALUES (@source, @id, @modified, @kind, @deleted, @data, @expiry);
END

GO

CREATE PROCEDURE [dbo].[UPDATE_ITEM_BATCH]
(
	@Tvp as ItemTableType READONLY
)
AS
BEGIN
	BEGIN TRAN
	--EXEC sp_getapplock @Resource='BatchUpdateNonParallelLock', @LockMode='Exclusive', @LockOwner='Transaction', @LockTimeout = 60000
	MERGE INTO [dbo].[items] AS t
	USING (SELECT [source], [id], [modified], [kind], [deleted], [data], [expiry] FROM @Tvp)
		AS s ([source], [id], [modified], [kind], [deleted], [data], [expiry])
		ON t.[source] = s.[source] AND t.[id] = s.[id]
	WHEN MATCHED AND s.[modified] >= t.[modified] THEN  
		UPDATE SET
			[modified] = s.[modified],
			[kind] = s.[kind],
			[deleted] = s.[deleted],
			[data] = s.[data],
			[expiry] = s.[expiry]
	WHEN NOT MATCHED THEN
		INSERT ([source], [id], [modified], [kind], [deleted], [data], [expiry]) 
		VALUES (s.[source], s.[id], s.[modified], s.[kind], s.[deleted], s.[data], s.[expiry]);
	COMMIT
END

GO

CREATE PROCEDURE [dbo].[READ_ITEM_PAGE]
(
	@source varchar (50),
    @id nvarchar(50) = NULL,
    @modified bigint = NULL
)
AS
BEGIN
	-- This query will return one additional record (from the previous page) to check that the provided "source" value is valid (note >= instead of >)
	SELECT TOP 500 [data], [modified], [id] from [dbo].[items] u WHERE source = @source AND ((modified = @modified AND id >= @id) OR (modified > @modified)) ORDER BY modified, id;
END

GO

CREATE PROCEDURE [dbo].[DELETE_SOURCE]
(
	@source varchar (200)
)
AS
BEGIN
	BEGIN TRAN
	EXEC sp_getapplock @Resource='BatchUpdateNonParallelLock', @LockMode='Exclusive', @LockOwner='Transaction', @LockTimeout = 60000
	DELETE TOP (1000) FROM [dbo].[items] WHERE [source] = @source;
	COMMIT
	RETURN @@rowcount;
END

GO

CREATE PROCEDURE [dbo].[PRUNE_STALE_ITEMS]
AS
BEGIN
	DECLARE @deletedRows INT;
	DECLARE @now datetime;
	SET @deletedRows = 1;
    SET @now = GETUTCDATE();

	WHILE (@deletedRows > 0)
	  BEGIN
	     -- Delete some small number of rows at a time, and this sproc is run frequently, to avoid deadlock
		 DELETE TOP (1000) FROM [dbo].[items] WHERE [expiry] < @now
	  SET @deletedRows = @@ROWCOUNT;
	END
END

   
    
