USE [master]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[sp_ChequeoInstancia]
(
	@ayuda INT = 0,
	@removerCache INT = 0
) WITH RECOMPILE
AS
/* **********************************************************************
Autor:  Oscar Echeverri - ARUS
Fecha creación: 2024/08/10
Descripción: Procedimiento almacenado para realizar un chequeo de rutina de operación en la instancia
Versión: 1.1
*************************************************************************
Ejemplo
======
EXEC sp_ChequeoInstancia

Permisos
======
GRANT EXECUTE ON sp_InfoInstancia TO [USUARIO]
********************************************************************** */
BEGIN
	SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    SET QUOTED_IDENTIFIER ON;
    SET ANSI_PADDING ON;
    SET CONCAT_NULL_YIELDS_NULL ON;
    SET ANSI_WARNINGS ON;
    SET NUMERIC_ROUNDABORT OFF;
    SET ARITHABORT ON;

	IF ((SELECT CASE WHEN CONVERT(VARCHAR(2), SERVERPROPERTY('PRODUCTVERSION')) LIKE '8%' THEN 0
					WHEN CONVERT(VARCHAR(2), SERVERPROPERTY('PRODUCTVERSION')) LIKE '9%' THEN 0
					WHEN CONVERT(VARCHAR(2), SERVERPROPERTY('PRODUCTVERSION')) LIKE '11%' THEN 0
					WHEN CONVERT(VARCHAR(2), SERVERPROPERTY('PRODUCTVERSION')) LIKE '12%' THEN 0
					ELSE 1 END) = 0)
	BEGIN
		PRINT('Esta versión del sp_InfoInstancia NO funciona en versiones 2014 o inferior.');
		RETURN;
	END;

	IF(@ayuda = 1)
	BEGIN
		PRINT('Procedimiento almacenado sp_ChequeoInstancia por Oscar Echeverri - Administrador base de datos ARUS

Este procedimiento realiza un chequeo general de la instancia de base de datos y muestra un resultset con una serie 
de recomendaciones que debemos tener en cuenta.
Se puede utilizar como rutina de operación en el día a día y nos da una visión general sobre la instancia de base de datos.

Puede ejecutarse en cualquier momento y no debe generar afectación de servicio ya que éste utiliza las DMVs y los
DMF de SQL.

Ejecutar en la master.')
	RETURN
	END;

	IF(@removerCache = 0)
	BEGIN
		PRINT('Chequeando nivel de compatibilidad de las bases de datos');
		IF OBJECT_ID('tempdb..#resultadoChequeo') IS NOT NULL
			 DROP TABLE #resultadoChequeo;

		CREATE TABLE #resultadoChequeo
		(
			ID INT IDENTITY(1, 1) ,
			BaseDatos NVARCHAR(128) ,
			Prioridad TINYINT ,
			TipoHallazgo VARCHAR(50) ,
			Descripcion VARCHAR(1000) ,
			URL VARCHAR(200)
		);

		IF((SELECT 1 WHERE EXISTS(SELECT compatibility_level FROM sys.databases WHERE compatibility_level < 130)) > 0)
			INSERT INTO #resultadoChequeo (BaseDatos,Prioridad,TipoHallazgo,Descripcion,URL)
				SELECT name,
						4,
						'Informativo',
						'La base de datos ' + name + ' está en un nivel de compatibilidad ' +
							CASE compatibility_level WHEN 120 THEN 'SQL Server 2014'
								WHEN 110 THEN 'SQL Server 2012'
								WHEN 100 THEN 'SQL Server 2008 R2'
								WHEN 90 THEN 'SQL Server 2005'
								WHEN 80 THEN 'SQL Server 2000'
							END + '. Versión obsoleta y sin soporte.',
						'https://www.sqlskills.com/blogs/glenn/database-compatibility-level-in-sql-server/'
				FROM sys.databases
				WHERE compatibility_level < 130;

		PRINT('Chequeando procedimientos sin plan cache');

		DECLARE @sql VARCHAR(4000);

		IF OBJECT_ID('tempdb..#tempCache') IS NOT NULL
			DROP TABLE #tempCache;

		CREATE TABLE #tempCache
		(
			base_datos VARCHAR(50) NULL,
			procedimiento NVARCHAR(128) NULL,
			query_plan XML NULL,
			quitar_plan VARCHAR(149) NULL,
			quitar_sql VARCHAR(149) NULL
		);

		SET @sql = '
		IF ''?'' NOT IN(''master'', ''model'', ''msdb'', ''tempdb'') 
		BEGIN 
		USE [?]
		SELECT	DB_NAME(),
				OBJECT_NAME(eps.object_id),
				qp.query_plan,
				''DBCC FREEPROCCACHE('' + CONVERT(VARCHAR(128), eps.plan_handle, 1) + '');'',
				''DBCC FREEPROCCACHE('' + CONVERT(VARCHAR(128), eps.plan_handle, 1) + '');''
		FROM	sys.dm_exec_procedure_stats AS eps
		CROSS APPLY sys.dm_exec_query_plan(plan_handle) AS qp
		WHERE	eps.database_id = DB_ID()
			AND qp.query_plan IS NULL;
		END';

		INSERT INTO #tempCache EXEC sp_MSForeachdb @sql;

		IF((SELECT COUNT(1) FROM #tempCache) > 0)
			INSERT INTO #resultadoChequeo (BaseDatos,Prioridad,TipoHallazgo,Descripcion,URL)
				SELECT base_datos,
						3,
						'Informativo',
						'El procedimiento almacenado ' + procedimiento + ' de la base_datos ' + base_datos +
						' no cuenta con un plan de ejecución en cache. Ejectua la siguiente sentencia para remover el ' +
						'plan_hande y el sql_handle de la cache - EXEC sp_ChequeoInstancia @removerCache = 1',
						'https://dba.stackexchange.com/questions/167178/missing-execution-plans-for-stored-procedures'
				FROM	#tempCache;

		IF((SELECT TOP(1)
				CONVERT(DECIMAL(18,2), vs.available_bytes * 1. / vs.total_bytes * 100.)
			FROM   sys.master_files AS mf WITH(NOLOCK)
				CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.[file_id]) AS vs) < 20)
			PRINT('Chequeando discos con menos del 20% de espacio disponible');
			INSERT INTO #resultadoChequeo (BaseDatos,Prioridad,TipoHallazgo,Descripcion,URL)
				SELECT	DISTINCT
						'master',
						3,
						'Advertencia',
						'Se observa que el disco ' + CONVERT(VARCHAR, vs.volume_mount_point) + CONVERT(VARCHAR, vs.logical_volume_name) +
						' solo tiene un ' + CONVERT(VARCHAR, CONVERT(DECIMAL(18,2), vs.available_bytes * 1. / vs.total_bytes * 100.)) +
						'% de espacio disponible. Se recomienda revisar.',
						'https://dba.stackexchange.com/questions/30070/how-to-prevent-sql-server-database-server-from-running-out-of-disk-space'
				FROM   sys.master_files AS mf WITH(NOLOCK)
					CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.[file_id]) AS vs
				WHERE CONVERT(DECIMAL(18,2), vs.available_bytes * 1. / vs.total_bytes * 100.) < 20;
		IF((SELECT TOP(1)
				CONVERT(DECIMAL(18,2), vs.available_bytes * 1. / vs.total_bytes * 100.)
			FROM   sys.master_files AS mf WITH(NOLOCK)
				CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.[file_id]) AS vs) < 10)
			PRINT('Chequeando discos con menos del 10% de espacio disponible');
			INSERT INTO #resultadoChequeo (BaseDatos,Prioridad,TipoHallazgo,Descripcion,URL)
				SELECT	DISTINCT
						'master',
						2,
						'Prioridad',
						'Se observa que el disco ' + CONVERT(VARCHAR, vs.volume_mount_point) + CONVERT(VARCHAR, vs.logical_volume_name) +
						' solo tiene un ' + CONVERT(VARCHAR, CONVERT(DECIMAL(18,2), vs.available_bytes * 1. / vs.total_bytes * 100.)) +
						'% de espacio disponible. Se recomienda revisar.',
						'https://dba.stackexchange.com/questions/30070/how-to-prevent-sql-server-database-server-from-running-out-of-disk-space'
				FROM   sys.master_files AS mf WITH(NOLOCK)
					CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.[file_id]) AS vs
				WHERE CONVERT(DECIMAL(18,2), vs.available_bytes * 1. / vs.total_bytes * 100.) < 10;
		
		SELECT  *
		FROM #resultadoChequeo
		ORDER BY Prioridad ASC;

		DROP TABLE #resultadoChequeo;
		DROP TABLE #tempCache;
	END

	IF(@removerCache = 1)
	BEGIN
		DECLARE @sqlCache VARCHAR(4000);

		IF OBJECT_ID('tempdb..#tempPlanCache') IS NOT NULL
			DROP TABLE #tempPlanCache;

		CREATE TABLE #tempPlanCache
		(
			base_datos VARCHAR(50) NULL,
			procedimiento NVARCHAR(128) NULL,
			query_plan XML NULL,
			quitar_plan VARCHAR(149) NULL,
			quitar_sql VARCHAR(149) NULL
		);

		SET @sqlCache = '
		IF ''?'' NOT IN(''master'', ''model'', ''msdb'', ''tempdb'') 
		BEGIN 
		USE [?]
		SELECT	DB_NAME(),
				OBJECT_NAME(eps.object_id),
				qp.query_plan,
				''DBCC FREEPROCCACHE('' + CONVERT(VARCHAR(128), eps.plan_handle, 1) + '');'',
				''DBCC FREEPROCCACHE('' + CONVERT(VARCHAR(128), eps.plan_handle, 1) + '');''
		FROM	sys.dm_exec_procedure_stats AS eps
		CROSS APPLY sys.dm_exec_query_plan(plan_handle) AS qp
		WHERE	eps.database_id = DB_ID()
			AND qp.query_plan IS NULL;
		END';

		INSERT INTO #tempPlanCache EXEC sp_MSForeachdb @sqlCache;
	
		SELECT *
		FROM	#tempPlanCache;

		DROP TABLE #tempPlanCache;
	END;
END