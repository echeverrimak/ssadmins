USE [master]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
 
ALTER PROCEDURE [dbo].[sp_InfoInstancia] (
	@ayuda INT = 0,
	@propiedades INT = 0,
	@baseDatos NVARCHAR(50) = '',
	@servicios INT = 0,
	@estado INT = 0,
	@discos INT = 0,
	@latencia_discos INT = 0,
	@uso_cpu INT = 0,
	@uso_discos INT = 0,
	@uso_memoria INT = 0,
	@alwayson INT = 0,
	@archivosBDs INT = 0,
	@jobs INT = 0,
	@bloqueos INT = 0,
	@indices INT = 0,
	@estadisticas INT = 0,
	@jobsEnEjecucion INT = 0,
	@historialRespaldo INT = 0,
	@tiempoRespaldo INT = 0,
	@tiempoRecovery INT = 0,
	@tempdb INT = 0,
	@indicesNoUsados INT = 0,
	@matarSesiones INT = 0,
    	@deshabilitarReplica INT = 0,
    	@habilitarReplica INT = 0
	) WITH RECOMPILE
AS
/* **********************************************************************
Autor:  Oscar Echeverri - ARUS
Fecha creación: 2020/01/02
Descripción: Procedimiento almacenado para retornar información básica de la instancia y bases de datos
Versión: 1.2
*************************************************************************
Ejemplo
======
EXEC sp_InfoInstancia @estado = 1, @servicios = 1

Permisos
======
GRANT EXECUTE ON sp_InfoInstancia TO [USUARIO]
 
Historial de cambios
----------------------------------------------------------------------------------
FECHA				USUARIO				DESCRIPCION
2020/01/02			Oscar Echeverri		Creación procedimiento
2020/01/05			Oscar Echeverri		Incluír documentación del procedimiento con el parámetro @ayuda
2021/10/12			Oscar Echeverri		Adición de consulta para devolver información del cluster de alwayson
2021/10/13			Oscar Echeverri		Resultados por defecto en caso de que no se pase ningún parámetro
2021/01/02			Oscar Echeverri		Actualización de la consulta para el parámetro @servicios
2022/05/28			Oscar Echeverri		Modificación de la consulta para el parámetro @estado
2022/05/29			Oscar Echeverri		Actualización de las propiedades de la instancia
2022/06/07			Oscar Echeverri		Incluír validación de la base de datos en la sección de respaldos
2022/06/08			Oscar Echeverri		Actualización de la consulta del parámetro @alwayson
2022/07/21			Oscar Echeverri		Actualización del nombre de los campos de los parámetros @uso_cpu, @uso_memoria y @uso_discos
2022/07/21			Oscar Echeverri		Modificación de la consulta del parámetro @uso_memoria
2022/09/28			Oscar Echeverri		Incluír validación de la base de datos en la sección de indices
2022/09/28			Oscar Echeverri		Incluír validación de la base de datos en la sección de estadísticas
2022/09/28			Oscar Echeverri		Incluír validación de la base de datos en la sección de estadísticas
2023/03/07			Oscar Echeverri		Adición de consulta para devolver la información de uso de la tempdb
2023/09/14			Oscar Echeverri		Modificar consulta historial de respaldos
2023/09/14			Oscar Echeverri		Adición consulta tiempo estimado de restauración
2023/09/14			Oscar Echeverri		Adición consulta tiempo estimado de recuperación de la base de datos cuando se inicia la instancia
2023/10/05			Oscar Echeverri		Actualización de la consulta del parámetro @indices. No estaba tomando el nombre de la base de datos
2023/11/09			Oscar Echeverri		Adición consulta de índices sin utilizar en la base de datos. Requiere el parámetro @baseDatos
2024/02/29          Oscar Echeverri     Adición consulta para matar sesiones de forma masiva - TODO: modificar para que solo mate sesión bloqueante o sesiones bloqueadas
2024/02/29          Oscar Echeverri     Adición consulta para deshabilitar replicación de las bases de datos del cluster de alwayson
2024/02/29          Oscar Echeverri     Adición consulta para habilitar replicación de las bases de datos del cluster de alwayson
*/

BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    SET QUOTED_IDENTIFIER ON;
    SET ANSI_PADDING ON;
    SET CONCAT_NULL_YIELDS_NULL ON;
    SET ANSI_WARNINGS ON;
    SET NUMERIC_ROUNDABORT OFF;
    SET ARITHABORT ON;

	IF(@ayuda = 1)
		PRINT '
Procedimiento almacenado sp_InfoInstancia (versión 1.2) por Oscar Echeverri - Administrador base de datos ARUS

Este procedimiento muestra información general sobre la instancia de base de datos.
Puede ejecutarse en cualquier momento y no debe generar afectación de servicio ya que éste utiliza las DMVs y los
DMF de SQL.

Ejecutar en la master.

Ejemplo de uso del procedimiento:

- Para obtener la información completa de la instancia: exec sp_InfoInstancia

- Para obtener la inromación de uno de los parámetros se debe utilizar la siguiente sintaxis:
	exec sp_InfoInstancia @parámetro = 1
	ejemplo: exec sp_InfoInstancia @uso_discos

- Para obtener la información de varios parámetros se debe utilizar la siguiente sintaxis:
	exec sp_InfoInstancia @parámetro1 = 1, @parámetro2 = 1... @parámetroN = 1
	ejemplo : exec sp_InfoInstancia @propiedades = 1, @servicios = 1, @alwayson = 1

Parámetros:
@propiedades - Muestra las propiedades de la instancia de base de datos como
			   Nombre máquina
			   Nombre instancia
			   Edición
			   Service Pack
			   Actualización
			   Versión del motor de base de datos
			   Referencia actualización
			   Colación 
			   ¿cluster de alwayson?

@servicios - Muestra el estado de los servicios de base de datos
		     Instancia,
		     Estado,
		     Tipo inicio (manual, automático, deshabilitado)
		     Fecha de inicio del servicio
		     Cuenta del servicio

@estado - Muestra el estado de las bases de datos de la instancia
		  ONLINE
		  SINGLE_USER / MULTI_USER
					    
@discos - Muestra información del tamaño de los discos
		  Punto de montaje
		  Sistema archivos
		  Nombre lógico
		  Tamaño en GB
		  Espacio disponible en GB
		  Espacio dispobible en porcentaje

@latencia_discos - Muestra la latencia en disco generada por cada base de datos 

@uso_cpu - Muestra el uso de CPU por cada base de datos

@uso_discos - Muestra el I/O por cada base de datos

@uso_memoria - Muestra información básica y el uso de la memoria por cada base de datos

@alwayson - Muestra si la instancia de base de datos está habilitada como nodo del cluster de alwayson
			   y el estado de la sincronización 

@archivosBDs - Muestra información sobre los archivos .mdf, .ndf y .ldf de las bases de datos

@jobs - Muestra el detalle de los jobs fallidos en la instancia de base de datos

@bloqueos - Muestra si actualmente existen bloqueos en la instancia de base de datos

@indices - Muestra información sobre el nivel de fragmentación de los índices de las bases de datos y genera
		   el script de recreación y/o reorganización de los índices, según si porcentaje de fragmentación.
		   Se requiere pasar tambíén el nombre de la base de datos en el parámetro @baseDatos

@estadisticas - Muestra información sobre las estadísticas de las tablas de las bases de datos y genera el 
				script de actualización de cada estadística. Se debe validar la última fecha de actualización 

@version int - Muestra la versión del motor de base de datos

@historialRespaldo int - Muestra el historial de respaldos de las bases de datos

@jobs_ejecucion - Muestra los jobs que actualmente se están ejecutando en la instancia'

	IF (SELECT	CASE WHEN CONVERT(NVARCHAR(128), SERVERPROPERTY('PRODUCTVERSION')) LIKE '8%' THEN 0
					 WHEN CONVERT(NVARCHAR(128), SERVERPROPERTY('PRODUCTVERSION')) LIKE '9%' THEN 0
					 WHEN CONVERT(NVARCHAR(128), SERVERPROPERTY('PRODUCTVERSION')) LIKE '11%' THEN 0
				ELSE 1 END) = 0
	BEGIN
		DECLARE @mensaje VARCHAR(80); 
		SELECT @mensaje = 'Esta versión del sp_InfoInstancia NO funciona en versiones 2012 o inferior.';
		PRINT @mensaje;
		RETURN;
	END;
	
	IF(@propiedades = 1)
	BEGIN
		SELECT	SERVERPROPERTY('MachineName') AS nombre_maquina, 
				SERVERPROPERTY('ServerName') AS nombre_instancia,
				LEFT(@@VERSION, 25) AS version_sql,
				SERVERPROPERTY('Edition') AS edicion, 
				SERVERPROPERTY('ProductLevel') AS servicePack,
				SERVERPROPERTY('ProductUpdateLevel') AS actualizacion,
				SERVERPROPERTY('ProductVersion') AS version_producto,
				SERVERPROPERTY('ProductUpdateReference') AS referencia_actualizacion,
				SERVERPROPERTY('Collation') Colacion, 
				CASE WHEN SERVERPROPERTY('IsHadrEnabled') = 1 THEN 'La instancia está habilitada como cluster de alwayson'
					ELSE 'La instancia NO está habilitada como cluster de alwayson' END AS habilitado_HADR,
				CASE SERVERPROPERTY('HadrManagerStatus') WHEN 0 THEN 'No iniciado, pendiente comunicación'
														 WHEN 1 THEN 'Iniciado y corriendo'
														 WHEN 2 THEN 'No iniciado y fallido' END AS estado_HADR,
				CASE WHEN SERVERPROPERTY('InstanceDefaultBackupPath') IS NULL THEN 'Ruta de respaldo por VERITAS'
																 ELSE SERVERPROPERTY('InstanceDefaultBackupPath') END AS ruta_respaldos,
				SERVERPROPERTY('InstanceDefaultDataPath') AS ruta_datos,
				SERVERPROPERTY('InstanceDefaultLogPath') AS ruta_logs,
				CASE SERVERPROPERTY('IsIntegratedSecurityOnly') WHEN 0 THEN 'Autenticación mixta, Windows y SQL'
																WHEN 1 THEN 'Autenticación integrada (Autenticación Windows)'
																WHEN NULL THEN 'No aplica' END AS tipo_autenticacion,
				CONNECTIONPROPERTY('net_transport') AS net_transport,
				CONNECTIONPROPERTY('protocol_type') AS protocol_type,
				CONNECTIONPROPERTY('auth_scheme') AS auth_scheme,
				CONNECTIONPROPERTY('local_net_address') AS local_net_address,
				CONNECTIONPROPERTY('local_tcp_port') AS local_tcp_port
	END;

    IF(@servicios = 1)
    BEGIN
        SELECT	SERVERPROPERTY('MachineName') AS nombre_maquina,
				CONNECTIONPROPERTY('local_net_address') AS direccion_ip,
				CONNECTIONPROPERTY('local_tcp_port') AS puerto

		DECLARE @sqlcmd NVARCHAR(max), @params NVARCHAR(600), @sqlmajorver INT
		DECLARE @UpTime VARCHAR(12),@StartDate DATETIME

		SELECT @sqlmajorver = CONVERT(INT, (@@MICROSOFTVERSION / 0x1000000) & 0xff);

		IF @sqlmajorver < 10
		BEGIN
			SET @sqlcmd = N'SELECT @UpTimeOUT = DATEDIFF(mi, login_time, GETDATE()), @StartDateOUT = login_time FROM master..sysprocesses (NOLOCK) WHERE spid = 1';
		END
		ELSE
		BEGIN
			SET @sqlcmd = N'SELECT @UpTimeOUT = DATEDIFF(mi,sqlserver_start_time,GETDATE()), @StartDateOUT = sqlserver_start_time FROM sys.dm_os_sys_info (NOLOCK)';
		END

		SET @params = N'@UpTimeOUT VARCHAR(12) OUTPUT, @StartDateOUT DATETIME OUTPUT';

		EXECUTE sp_executesql @sqlcmd, @params, @UpTimeOUT = @UpTime OUTPUT, @StartDateOUT = @StartDate OUTPUT;

		SELECT GETDATE() AS fecha_actual, @StartDate AS ultimo_inicio, CONVERT(VARCHAR(4), @UpTime/60/24) + 'd ' + CONVERT(VARCHAR(4), @UpTime/60%24) + 'hr ' + CONVERT(VARCHAR(4), @UpTime%60) + 'min' AS tiempo_actividad

		SELECT	servicename AS nombre_servicio,
				status_desc AS estado_servicio,
				startup_type_desc tipo_inicio,
				service_account AS cuenta_servicio,
				GETDATE() AS fecha_actual
		FROM	sys.dm_server_services;
    END;

    IF(@estado = 1)
    BEGIN
		IF (SELECT COUNT(1) FROM sys.databases WHERE state_desc NOT IN('ONLINE')
				OR user_access_desc IN('SINGLE_USER','RESTRICTED_USER')) > 0
				SELECT	name AS base_datos,
						state_desc AS estado,
						user_access_desc AS acceso,
						GETDATE() AS fecha_actual
				FROM	sys.databases
				WHERE	state_desc NOT IN('ONLINE')
					OR user_access_desc IN('SINGLE_USER','RESTRICTED_USER')
		ELSE
				SELECT 'Todas las bases de datos se encuentran en modo ONLINE y MULTI_USER' AS estado_BDs;
    END;

	IF(@discos = 1)
	BEGIN
		SELECT DISTINCT vs.volume_mount_point AS punto_montaje,
			   vs.file_system_type AS sistema_archivos,
			   vs.logical_volume_name AS nombre_logico,
			   CONVERT(DECIMAL(18,2), vs.total_bytes/1073741824.0) AS tamano_GB,
			   CONVERT(DECIMAL(18,2), vs.available_bytes/1073741824.0) AS espacio_disponible_GB,  
			   CONVERT(DECIMAL(18,2), vs.available_bytes * 1. / vs.total_bytes * 100.) AS porcentaje_disponible
		FROM   sys.master_files AS mf WITH(NOLOCK)
			CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.[file_id]) AS vs
		ORDER BY vs.volume_mount_point OPTION(RECOMPILE);
	END;

	IF(@latencia_discos = 1)
	BEGIN
		SELECT  DB_NAME(fs.database_id) AS base_datos,
			    CAST(fs.io_stall_read_ms/(1.0 + fs.num_of_reads) as numeric(10,1)) AS promedio_lectura_ms,
			    CAST(fs.io_stall_write_ms/(1.0 + fs.num_of_writes) as numeric(10,1)) AS promedio_escritura_ms,
			    CAST((fs.io_stall_read_ms + fs.io_stall_write_ms)/(1.0 + fs.num_of_reads + fs.num_of_writes) AS NUMERIC(10,1)) AS promedio_io_ms,
			    CONVERT(DECIMAL(18,2), mf.size/128.0) AS tamano_archivo_MB,
			    mf.physical_name,
			    mf.type_desc,
			    fs.io_stall_read_ms,
			    fs.num_of_reads, 
			    fs.io_stall_write_ms,
			    fs.num_of_writes
		FROM    sys.dm_io_virtual_file_stats(NULL,NULL) AS fs
			JOIN sys.master_files mf WITH(NOLOCK) ON fs.database_id = mf.database_id
                AND fs.[file_id] = mf.[file_id]
		ORDER BY promedio_io_ms DESC OPTION(RECOMPILE)
	END;

	IF(@uso_cpu = 1)
	BEGIN
		WITH estadisticas_cpu AS
		(SELECT pa.idbd,
			    DB_NAME(pa.idbd) AS base_datos,
				SUM(qs.total_worker_time/1000) AS tiempo_cpu_ms
		 FROM   sys.dm_exec_query_stats AS qs WITH(NOLOCK)
			CROSS APPLY(SELECT CONVERT(int, value) AS idbd 
						FROM   sys.dm_exec_plan_attributes(qs.plan_handle)
						WHERE  attribute = N'dbid') AS pa
		 GROUP BY pa.idbd)
		SELECT  ROW_NUMBER() OVER(ORDER BY tiempo_cpu_ms DESC) AS uso_cpu,
			    base_datos,
			    tiempo_cpu_ms, 
			    CAST(tiempo_cpu_ms * 1.0 / SUM(tiempo_cpu_ms) OVER() * 100.0 AS DECIMAL(5, 2)) AS porcentaje_uso_cpu
		FROM    estadisticas_cpu
		WHERE   idbd <> 32767 -- ResourceDB
		ORDER BY uso_cpu OPTION(RECOMPILE)
	END;

	IF(@uso_discos = 1)
	BEGIN
		WITH estadisticas_io AS
		(SELECT DB_NAME(fs.database_id) base_datos,
			    CAST(SUM(fs.num_of_bytes_read + fs.num_of_bytes_written) / 1048576 AS DECIMAL(12, 2)) AS io_mb
		 FROM   sys.dm_io_virtual_file_stats(NULL, NULL) AS fs
		 GROUP BY fs.database_id)
		SELECT ROW_NUMBER() OVER(ORDER BY io_mb DESC) AS uso_disco,
			   base_datos,
			   io_mb io_total_mb,
			   CAST(io_mb / SUM(io_mb) OVER() * 100.0 AS DECIMAL(5,2)) porcentaje_uso_disco
		FROM   estadisticas_io
		ORDER BY uso_disco OPTION(RECOMPILE)
	END;

	IF(@uso_memoria = 1)
	BEGIN
		CREATE TABLE #memoriaInstancia
		(
			nombreInstancia NVARCHAR(50),
			usoMemoriaSQL_mb INT,
			memoriaSQL_mb INT,
			memoriaSO_mb INT,
			memoriaDisponibleSO_mb INT
		)

		DECLARE @nombreInstancia NVARCHAR(50)
		DECLARE @usoMemoriaSQL INT
		DECLARE @memoriaSQL INT
		DECLARE @memoriaSO INT
		DECLARE @usoMemoriaSO INT 
 
		-- SQL memory
		SELECT	@nombreInstancia = CASE WHEN @@SERVICENAME = 'MSSQLSERVER' THEN @@SERVERNAME ELSE @@SERVERNAME + @@SERVICENAME END,
				@usoMemoriaSQL = (committed_kb/1024),
				@memoriaSQL = (committed_target_kb/1024)           
		FROM	sys.dm_os_sys_info;
   
		--OS memory
		SELECT	@memoriaSO = (total_physical_memory_kb/1024),
				@usoMemoriaSO = (available_physical_memory_kb/1024) 
		FROM	sys.dm_os_sys_memory;
   
		INSERT INTO #memoriaInstancia VALUES (@nombreInstancia, @usoMemoriaSQL, @memoriaSQL, @memoriaSO, @usoMemoriaSO);

		SELECT	nombreInstancia,
				memoriaSO_mb,
				memoriaDisponibleSO_mb,
				memoriaSQL_mb,
				usoMemoriaSQL_mb
		FROM	#memoriaInstancia;

		DROP TABLE #memoriaInstancia;

		WITH estadisticas_memoria AS
		(SELECT DB_NAME(database_id) AS base_datos,
				CAST(count(1) * 8 / 1024.0 AS DECIMAL(10,2)) AS tamano_cache
		FROM    sys.dm_os_buffer_descriptors WITH(NOLOCK)
		WHERE   database_id <> 32767 -- ResourceDB
		GROUP BY DB_NAME(database_id))
		SELECT ROW_NUMBER() OVER(ORDER BY tamano_cache DESC) AS uso_memoria,
				base_datos,
				tamano_cache AS memoria_mb,
				CAST(tamano_cache / SUM(tamano_cache) OVER() * 100.0 AS DECIMAL(5,2)) AS porcentaje_uso_memoria
		FROM   estadisticas_memoria
		ORDER BY uso_memoria OPTION(RECOMPILE);
	END;

    IF(@alwayson = 1)
    BEGIN
        IF((SELECT SERVERPROPERTY('IsHadrEnabled')) = 0)
            SELECT 'La instancia no está habilitada como cluster de alwayson'
        ELSE
            SELECT	ag.name AS nombre_grupo,
					adc.database_name AS base_datos,
                    CASE drs.is_local WHEN 1 THEN 'PRIMARIO' ELSE 'SECUNDARIO' END AS nodo,
					ar.replica_server_name AS instancia_replica,
					ar.endpoint_url AS endpoint,
					ar.availability_mode_desc AS modo_sincronizacion,
					ar.failover_mode_desc AS tipo_failover,
					ar.read_only_routing_url AS ruta_solo_lectura,
					ar.primary_role_allow_connections_desc AS conexiones_en_primario,
					ar.secondary_role_allow_connections_desc AS conexiones_en_secundario,
					ar.seeding_mode_desc AS modo_semilla,
					ag.automated_backup_preference_desc AS preferencia_respaldo,
					CASE drs.is_suspended WHEN 0 THEN 'No'
										  WHEN 1 THEN 'Si' END AS suspendida,
					drs.database_state_desc AS estado_bd,
					drs.suspend_reason_desc AS motivo_suspension,
					drs.synchronization_health_desc AS estado_cluster,
					drs.synchronization_state_desc AS estado_sincronizacion,
					drs.secondary_lag_seconds AS segundos_retrazo
			FROM	sys.dm_hadr_database_replica_states AS drs
				JOIN sys.availability_databases_cluster AS adc ON drs.group_id = adc.group_id
					AND drs.group_database_id = adc.group_database_id
				JOIN sys.availability_groups AS ag ON ag.group_id = drs.group_id
				JOIN sys.availability_replicas AS ar ON drs.group_id = ar.group_id
					AND drs.replica_id = ar.replica_id
			WHERE	drs.is_primary_replica = 0
			ORDER BY ag.name, ar.replica_server_name, adc.[database_name] OPTION(RECOMPILE);

			SELECT  ag.name AS grupo_disponibilidad,
					agl.dns_name AS listener,
					agl.port AS puerto,
					agl.ip_configuration_string_FROM_cluster AS cadena_configuracion_del_cluster,
					lip.ip_address AS direccion_IP,
					lip.ip_subnet_mask AS mascara_subred,
					CASE lip.is_dhcp WHEN 0 THEN 'NO' WHEN 1 THEN 'SI' END AS DHCP,
					lip.state_desc AS estado,
					'ALTER AVAILABILITY GROUP ' + ag.name + ' ADD LISTENER ''<--Nombre_listener-->'' (WITH IP((''10.XX.XX.XX'',''255.XX.XX.XX'')), PORT = ' + CONVERT(VARCHAR, agl.port) + ')' AS tsql_listener
			FROM    sys.availability_group_listeners AS agl
				JOIN sys.availability_group_listener_ip_addresses AS lip ON agl.listener_id = lip.listener_id
				JOIN sys.availability_groups AS ag on agl.group_id = ag.group_id;
        END;
    END;

    IF(@archivosBDs = 1)
    BEGIN
        SELECT @@SERVERNAME AS instancia,
               DB_NAME(mf.database_id) AS base_de_datos,
               vs.volume_mount_point AS disco,
               CASE WHEN mf.max_size > -1 THEN (CAST(SUM(mf.max_size)*8./1024 AS DECIMAL(8,0)))
                    ELSE (CAST(vs.total_bytes/1048576.0 AS DECIMAL(8,0))) END AS total_asignado_MB,
               CAST(SUM(mf.size)*8./1024 AS DECIMAL(8,0)) AS actual_MB,
               CAST(vs.total_bytes/1048576.0 AS DECIMAL(8,0)) - CAST(SUM(mf.size)*8./1024 AS DECIMAL(8,0)) AS libre,
               CAST(vs.total_bytes/1048576.0 AS DECIMAL(8,0)) AS total_disco_MB,
               mf.type_desc AS tipo,
               CASE WHEN mf.max_size > -1 THEN CONVERT(VARCHAR, mf.max_size) ELSE 'Ilimitado' END AS tamano_asignado_MB
        FROM   sys.master_files AS mf
			CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.file_id) AS vs
        GROUP BY vs.total_bytes, mf.max_size, DB_NAME(mf.database_id), mf.type_desc, vs.available_bytes, vs.volume_mount_point
        ORDER BY DB_NAME(mf.database_id)
    END;

    IF(@jobs = 1)
    BEGIN
        SELECT  DISTINCT jh.server AS instancia,
                jh.step_id AS numero_paso,
                jh.step_name AS nombre_paso,
                SUBSTRING(j.name,1,140) AS nombre_job,
                msdb.dbo.AGENT_DATETIME(jh.run_date, jh.run_time) AS fecha_ejecucion,
				STUFF(STUFF(STUFF(RIGHT(REPLICATE('0', 8) + CAST(jh.run_duration as varchar(8)), 8), 3, 0, ':'), 6, 0, ':'), 9, 0, ':') AS Duracion,
                jh.run_duration duracion_paso,
                CASE jh.run_status  WHEN 0 THEN 'fallo'
                                    WHEN 1 THEN 'exitoso'
                                    WHEN 2 THEN 'reintento...'
                                    WHEN 3 THEN 'cancelado'
                                    WHEN 4 THEN 'en progreso...'
                END AS estado_ejecucion,
                jh.message AS mensaje_error
        FROM    msdb.dbo.sysjobs AS j
			JOIN msdb.dbo.sysjobhistory AS jh ON jh.job_id = j.job_id
        WHERE   jh.run_status NOT IN(1, 4)
			AND jh.step_id != 0
			AND run_date >= CONVERT(CHAR(8), (SELECT DATEADD(DAY,(-1), GETDATE())), 112)
    END;

    IF(@bloqueos = 1)
    BEGIN
        IF((SELECT TOP 1 wait_time FROM sys.dm_exec_requests WHERE blocking_session_id > 0 AND wait_time > 60000) IS NOT NULL)
            SELECT  es.session_id,
                    DB_NAME(er.database_id) AS data_base,
                    es.login_name AS login_name,
                    es.host_name AS host_name,
                    er.blocking_session_id,
                    er.command,
                    er.status,
                    er.start_time,
                    ws.wait_type,
                    ws.wait_duration_ms
             FROM   sys.dm_exec_sessions AS es
                JOIN sys.dm_exec_requests AS er ON es.session_id = er.session_id
                JOIN sys.dm_os_waiting_tasks AS ws ON es.session_id = ws.session_id
             WHERE  er.blocking_session_id > 0
        ELSE
             SELECT 'No hay bloqueo'
    END;

    IF(@indices = 1)
    BEGIN
		IF(@baseDatos = '' OR @baseDatos IS NULL)		
			RAISERROR ('Por favor ingresar el nombre de la base de datos en el parámetro ''@baseDatos''', 1, 1);
		ELSE
		BEGIN
			CREATE TABLE #temporalIndices
			(
				[base_datos] [sysname] NOT NULL,
				[esquema] [sysname] NOT NULL,
				[tabla] [sysname] NOT NULL,
				[indice] [nvarchar](128) NULL,
				[tipo_datos] [nvarchar](60) NULL,
				[porcentaje] [float] NULL,
				[paginas] [bigint] NULL,
				[sentencia_sql] [nvarchar](413) NULL
			);

			INSERT INTO #temporalIndices EXEC sp_MSforeachdb 'USE [?]
				SELECT	DB_NAME() AS base_datos,
						s.name AS esquema, 
						t.name AS tabla, 
						CASE WHEN ips.index_type_desc = ''HEAP'' THEN ''Tabla sin índice'' ELSE i.name END AS indice,
						ips.index_type_desc AS tipo_datos,
						ips.avg_fragmentation_in_percent AS porcentaje,
						ips.page_count paginas,
						CASE WHEN (ips.avg_fragmentation_in_percent < 30 AND ips.avg_fragmentation_in_percent > 5) THEN
									''ALTER INDEX '' + i.name + '' ON '' + s.name + ''.'' + t.name + '' REORGANIZE;''
							WHEN (ips.avg_fragmentation_in_percent > 30) THEN
									''ALTER INDEX '' + i.name + '' ON '' + s.name + ''.'' + t.name + '' REBUILD;'' ELSE
									''índice con un nivel de fragmentación menor al 5%''
						END AS sentencia_sql
				FROM    sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, NULL) AS ips
					JOIN sys.tables AS t ON t.object_id = ips.object_id
					JOIN sys.schemas AS s ON t.schema_id = s.schema_id
					JOIN sys.indexes AS i ON i.object_id = ips.object_id
						AND ips.index_id = i.index_id
				WHERE DB_ID() NOT IN(1,2,3,4)
					AND t.name <> ''sysdiagrams''
				ORDER BY ips.avg_fragmentation_in_percent DESC';

			SELECT	*
			FROM	#temporalIndices
			WHERE	base_datos = @baseDatos;

			DROP TABLE #temporalIndices;
		END
    END;

    IF(@estadisticas = 1)
    BEGIN
		IF(@baseDatos = '' OR @baseDatos IS NULL)		
			RAISERROR ('Por favor ingresar el nombre de la base de datos en el parámetro ''@baseDatos''', 1, 1);
		ELSE
		BEGIN
			DECLARE @sqlEstadisticas NVARCHAR(700)
			SET @sqlEstadisticas = 'USE ' + @baseDatos
			SET @sqlEstadisticas = @sqlEstadisticas + '
			SELECT	sp.stats_id,
					stat.name AS name,
					stat.filter_definition AS filter_definition,
					sp.last_updated,
					sp.rows,
					sp.rows_sampled,
					sp.steps,
					sp.unfiltered_rows,
					sp.modification_counter,
					''UPDATE STATISTICS '' + OBJECT_SCHEMA_NAME(stat.object_id) + ''.'' + OBJECT_NAME(stat.object_id) + '' '' + stat.name + '';'' AS comando
			FROM	sys.stats AS stat   
				CROSS APPLY sys.dm_db_stats_properties(stat.object_id, stat.stats_id) AS sp
			WHERE last_updated < GETDATE()-7
				OR modification_counter > rows*.10
			ORDER BY sp.last_updated DESC;'

			EXEC (@sqlEstadisticas);
		END
    END;

	IF(@historialRespaldo = 1)
	BEGIN
		IF(@baseDatos = '' OR @baseDatos IS NULL)		
				RAISERROR ('Por favor ingresar el nombre de la base de datos en el parámetro ''@baseDatos''', 1, 1);
			ELSE
			BEGIN
			SELECT  bs.backup_set_id,
					bs.database_name AS base_datos,
					bs.backup_start_date AS inicio_respaldo,
					bs.backup_finish_date AS fin_respaldo,
					CAST(CAST(bs.backup_size/1000000 AS INT) AS VARCHAR(14)) + ' ' + 'MB' AS tamano,
					CAST(datediff(second, bs.backup_start_date, bs.backup_finish_date) AS VARCHAR(4)) + ' ' + 'Segundos' AS tiempo_respaldo,
					CASE bs.type WHEN 'D' THEN 'Full Backup'
								 WHEN 'I' THEN 'Differential Backup'
								 WHEN 'L' THEN 'Transaction Log Backup'
								 WHEN 'F' THEN 'File or filegroup'
								 WHEN 'G' THEN 'Differential file'
								 WHEN 'P' THEN 'Partial'
								 WHEN 'Q' THEN 'Differential Partial'
					END AS tipo_respaldo,
					bmf.physical_device_name AS ruta_respaldo,
					CAST(bs.first_lsn AS VARCHAR(50)) AS first_lsn,
					CAST(bs.last_lsn AS VARCHAR(50)) AS last_lsn,
					bs.server_name,
					bs.recovery_model
			FROM    msdb.dbo.backupset AS bs
				JOIN msdb.dbo.backupmediafamily AS bmf ON bs.media_set_id = bmf.media_set_id
					WHERE bs.database_name = @baseDatos
			ORDER BY bs.backup_start_date DESC
		END
	END;
	
	IF(@tiempoRespaldo = 1)
	BEGIN
		SELECT	session_id AS sesion,
				command AS tipo,
				a.text AS sentencia,
				start_time AS hora_inicio,
				percent_complete AS porcentaje,
				DATEADD(SECOND, estimated_completion_time/1000, GETDATE()) AS hora_estimada_finalizacion
		FROM	sys.dm_exec_requests AS r
			CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS a
		WHERE r.command in ('BACKUP DATABASE','RESTORE DATABASE','BACKUP LOG','RESTORE LOG');
	END;

	IF(@jobsEnEjecucion = 1)
	BEGIN
		SELECT  ja.job_id,
			    j.name AS job_name,
			    ja.start_execution_date,      
			    ISNULL(last_executed_step_id, 0) + 1 AS current_executed_step_id,
			    js.step_name,
			    js.command
		FROM    msdb.dbo.sysjobactivity AS ja
                LEFT JOIN msdb.dbo.sysjobhistory AS jh ON ja.job_history_id = jh.instance_id
                JOIN msdb.dbo.sysjobs AS j ON ja.job_id = j.job_id
                JOIN msdb.dbo.sysjobsteps AS js ON ja.job_id = js.job_id
                AND ISNULL(ja.last_executed_step_id, 0) + 1 = js.step_id
		WHERE   ja.session_id = (SELECT TOP 1 session_id FROM msdb.dbo.syssessions ORDER BY agent_start_date DESC)
            AND start_execution_date IS NOT NULL
            AND stop_execution_date IS NULL
		ORDER BY ja.start_execution_date ASC
	END;

	IF(@tempdb = 1)
	BEGIN
		SELECT	es.session_id AS sesion,
				DB_NAME(spu.database_id) AS base_datos,
				es.host_name AS maquina,
				es.program_name AS programa,
				es.login_name AS usuario,
				es.status AS estado,
				(spu.user_objects_alloc_page_count * 8) / 1024 AS espacio_reservado_usuario_mb,
				(spu.user_objects_dealloc_page_count * 8) / 1024 AS espacio_liberado_usuario_mb,
				(spu.internal_objects_alloc_page_count * 8) / 1024 AS espacio_reservado_interno_mb,
				(spu.internal_objects_dealloc_page_count * 8) / 1024 AS espacio_liberadio_interno_mb,
				es.cpu_time AS tiempo_cpu_ms,
				es.total_scheduled_time AS tiempo_worker_ms,
				es.total_elapsed_time AS tiempo_ejecucion_ms,
				(es.memory_usage * 8) / 1024 AS uso_memoria_MB,
				CASE es.is_user_process WHEN 1 THEN 'user session' ELSE 'system session' END AS tipo_sesion,
				es.row_count AS registros
		FROM	tempdb.sys.dm_db_session_space_usage AS spu
			JOIN sys.dm_exec_sessions AS es ON spu.session_id = es.session_id
		ORDER BY espacio_liberado_usuario_mb DESC
	END;

	IF(@tiempoRecovery = 1)
	BEGIN
		DECLARE @DBName VARCHAR(64) = 'Warehouse'
		DECLARE @ErrorLog AS TABLE([LogDate] CHAR(24), [ProcessInfo] VARCHAR(64), [TEXT] VARCHAR(MAX))

		INSERT INTO @ErrorLog
			EXEC master..sp_readerrorlog 0, 1, 'Recovery of database', @DBName

		INSERT INTO @ErrorLog
			EXEC master..sp_readerrorlog 0, 1, 'Recovery completed', @DBName

		SELECT	TOP 1
				@DBName AS [DBName],
				[LogDate],
				CASE WHEN SUBSTRING([TEXT],10,1) = 'c'
					 THEN '100%'
					 ELSE SUBSTRING([TEXT], CHARINDEX(') is ', [TEXT]) + 4,CHARINDEX(' complete (', [TEXT]) - CHARINDEX(') is ', [TEXT]) - 4)
				END AS PercentComplete,
				CASE WHEN SUBSTRING([TEXT],10,1) = 'c'
					 THEN 0
					 ELSE CAST(SUBSTRING([TEXT], CHARINDEX('approximately', [TEXT]) + 13,CHARINDEX(' seconds remain', [TEXT]) - CHARINDEX('approximately', [TEXT]) - 13) AS FLOAT)/60.0
				END AS MinutesRemaining,
				CASE WHEN SUBSTRING([TEXT],10,1) = 'c'
					 THEN 0
					 ELSE CAST(SUBSTRING([TEXT], CHARINDEX('approximately', [TEXT]) + 13,CHARINDEX(' seconds remain', [TEXT]) - CHARINDEX('approximately', [TEXT]) - 13) AS FLOAT)/60.0/60.0
				END AS HoursRemaining,
				[TEXT]
		FROM	@ErrorLog
		ORDER BY CAST([LogDate] AS DATETIME) DESC, MinutesRemaining;
	END;

	IF(@indicesNoUsados = 1)
	BEGIN
		IF(@baseDatos = '' OR @baseDatos IS NULL)		
			RAISERROR ('Por favor ingresar el nombre de la base de datos en el parámetro ''@baseDatos''', 1, 1);
		ELSE
		BEGIN
			DECLARE @sqlIndices NVARCHAR(2000)
			SET @sqlIndices = 'USE ' + @baseDatos
			SET @sqlIndices = @sqlIndices + '
				IF(SELECT COUNT(1)
				   FROM sys.sql_modules
				   WHERE REPLACE(REPLACE(SUBSTRING([definition], CHARINDEX(''WITH'', [definition], 1), 50)  , '' '', ''''),'' '', '''') LIKE ''%WITH(INDEX%'') > 0
				BEGIN  
					SELECT	OBJECT_NAME([object_id]) objectName,
							SUBSTRING([definition], CHARINDEX(''WITH'', [definition], 1), 50) IndexHint
					FROM	sys.sql_modules
					WHERE	REPLACE(REPLACE(SUBSTRING([definition], CHARINDEX(''WITH'', [definition], 1), 50)  , '' '', ''''),'' '', '''') LIKE ''%WITH(INDEX%''

					RAISERROR(''¡¡PRECAUCIÓN!! - Actualmente existen uno o varios procedimientos almacenados que tienen un INDEX HINT.... Revisa el resultset'', 16, 1) WITH NOWAIT;
				END
				ELSE
					SELECT	TOP 25 o.name AS ObjectName
							,i.name AS IndexName
							,i.index_id AS IndexID
							,dm_ius.user_seeks AS UserSeek
							,dm_ius.user_scans AS UserScans
							,dm_ius.user_lookups AS UserLookups
							,dm_ius.user_updates AS UserUpdates
							,p.TableRows
							,''DROP INDEX '' + QUOTENAME(i.name) + '' ON '' + QUOTENAME(s.name) + ''.'' + QUOTENAME(OBJECT_NAME(dm_ius.OBJECT_ID)) AS ''drop statement''
					FROM	sys.dm_db_index_usage_stats dm_ius
						JOIN sys.indexes i ON i.index_id = dm_ius.index_id
							AND dm_ius.OBJECT_ID = i.OBJECT_ID
						JOIN sys.objects o ON dm_ius.OBJECT_ID = o.OBJECT_ID
						JOIN sys.schemas s ON o.schema_id = s.schema_id
						JOIN (
						SELECT SUM(p.rows) TableRows
							,p.index_id
							,p.OBJECT_ID
						FROM sys.partitions p
						GROUP BY p.index_id
							,p.OBJECT_ID
						) p ON p.index_id = dm_ius.index_id
						AND dm_ius.OBJECT_ID = p.OBJECT_ID
					WHERE OBJECTPROPERTY(dm_ius.OBJECT_ID, ''IsUserTable'') = 1
						AND dm_ius.database_id = DB_ID()
						AND i.type_desc = ''nonclustered''
						AND i.is_primary_key = 0
						AND i.is_unique_constraint = 0
					ORDER BY (dm_ius.user_seeks + dm_ius.user_scans + dm_ius.user_lookups) ASC;'
		END
		EXEC (@sqlIndices)
	END;

	IF(@matarSesiones = 1)
	BEGIN
		-- IF(@baseDatos = '' OR @baseDatos IS NULL)		
		-- 	RAISERROR ('Por favor ingresar el nombre de la base de datos en el parámetro ''@baseDatos''', 1, 1);
		-- ELSE
		-- BEGIN
			DECLARE @sqlKill NVARCHAR(MAX)
			SELECT  @sqlKill = (SELECT 'KILL ' + CONVERT(VARCHAR, session_id) + ';'
							FROM   sys.dm_exec_sessions
							WHERE  is_user_process = 1
								--AND db_name(database_id) = @baseDatos
								AND session_id != @@spid
							FOR XML PATH(''))

			PRINT REPLACE(@sqlKill, ';', CHAR(10))
			EXEC  sys.sp_executesql @sqlKill
		-- END;
	END;

    IF(@deshabilitarReplica = 1)
    BEGIN
        IF((SELECT SERVERPROPERTY('IsHadrEnabled')) = 0)
            SELECT 'La instancia no está habilitada como cluster de alwayson'
        ELSE
        BEGIN
            DECLARE @sqlDeshabilitar NVARCHAR(MAX)
            SELECT  @sqlDeshabilitar = (SELECT	'ALTER DATABASE [' + adc.database_name + '] SET HADR SUSPEND;'
                            FROM	sys.dm_hadr_database_replica_states AS drs
                                JOIN sys.availability_databases_cluster AS adc ON drs.group_id = adc.group_id
                                    AND drs.group_database_id = adc.group_database_id
                                JOIN sys.availability_groups AS ag ON ag.group_id = drs.group_id
                                JOIN sys.availability_replicas AS ar ON drs.group_id = ar.group_id
                                    AND drs.replica_id = ar.replica_id
                            WHERE	drs.is_primary_replica = 0
                            ORDER BY ag.name, ar.replica_server_name, adc.[database_name]
                            FOR XML PATH('')) 

            PRINT REPLACE(@sqlDeshabilitar, ';', CHAR(10))
            EXEC  sys.sp_executesql @sqlDeshabilitar
        END;
    END;

    IF(@habilitarReplica = 1)
    BEGIN
        IF((SELECT SERVERPROPERTY('IsHadrEnabled')) = 0)
            SELECT 'La instancia no está habilitada como cluster de alwayson'
        ELSE
        BEGIN
            DECLARE @sqlHabilitar NVARCHAR(MAX)
            SELECT  @sqlHabilitar = (SELECT	'ALTER DATABASE [' + adc.database_name + '] SET HADR RESUME;'
                            FROM	sys.dm_hadr_database_replica_states AS drs
                                JOIN sys.availability_databases_cluster AS adc ON drs.group_id = adc.group_id
                                    AND drs.group_database_id = adc.group_database_id
                                JOIN sys.availability_groups AS ag ON ag.group_id = drs.group_id
                                JOIN sys.availability_replicas AS ar ON drs.group_id = ar.group_id
                                    AND drs.replica_id = ar.replica_id
                            WHERE	drs.is_primary_replica = 0
                            ORDER BY ag.name, ar.replica_server_name, adc.[database_name]
                            FOR XML PATH('')) 

            PRINT REPLACE(@sqlHabilitar, ';', CHAR(10))
            EXEC  sys.sp_executesql @sqlHabilitar
        END
    END;

    IF(@ayuda = 0 AND
	   @propiedades = 0 AND
	   @servicios = 0 AND
	   @estado = 0 AND
	   @discos = 0 AND
	   @latencia_discos = 0 AND
	   @uso_cpu = 0 AND
	   @uso_discos = 0 AND
	   @uso_memoria = 0 AND
	   @alwayson = 0 AND
	   @archivosBDs = 0 AND
	   @jobs = 0 AND
	   @bloqueos = 0 AND
	   @jobsEnEjecucion = 0 AND
	   @historialRespaldo = 0 AND
	   @tiempoRespaldo = 0 AND
	   @estadisticas = 0 AND
	   @indices = 0 AND
	   @tiempoRecovery = 0 AND
	   @tempdb = 0 AND
	   @indicesNoUsados = 0 AND
       @matarSesiones = 0 AND
       @deshabilitarReplica = 0 AND
       @habilitarReplica = 0)
    BEGIN
        SELECT 	'Estado de los servicios'

        SELECT	SERVERPROPERTY('MachineName') AS nombre_maquina,
				CONNECTIONPROPERTY('local_net_address') AS direccion_ip,
				CONNECTIONPROPERTY('local_tcp_port') AS puerto

		SELECT @sqlmajorver = CONVERT(INT, (@@MICROSOFTVERSION / 0x1000000) & 0xff);

		IF @sqlmajorver < 10
		BEGIN
			SET @sqlcmd = N'SELECT @UpTimeOUT = DATEDIFF(mi, login_time, GETDATE()), @StartDateOUT = login_time FROM master..sysprocesses (NOLOCK) WHERE spid = 1';
		END
		ELSE
		BEGIN
			SET @sqlcmd = N'SELECT @UpTimeOUT = DATEDIFF(mi,sqlserver_start_time,GETDATE()), @StartDateOUT = sqlserver_start_time FROM sys.dm_os_sys_info (NOLOCK)';
		END

		SET @params = N'@UpTimeOUT VARCHAR(12) OUTPUT, @StartDateOUT DATETIME OUTPUT';

		EXECUTE sp_executesql @sqlcmd, @params, @UpTimeOUT = @UpTime OUTPUT, @StartDateOUT = @StartDate OUTPUT;

		SELECT GETDATE() AS fecha_actual, @StartDate AS ultimo_inicio, CONVERT(VARCHAR(4), @UpTime/60/24) + 'd ' + CONVERT(VARCHAR(4), @UpTime/60%24) + 'hr ' + CONVERT(VARCHAR(4), @UpTime%60) + 'min' AS tiempo_actividad

		SELECT	servicename AS nombre_servicio,
				status_desc AS estado_servicio,
				startup_type_desc tipo_inicio,
				service_account AS cuenta_servicio,
				GETDATE() AS fecha_actual
		FROM	sys.dm_server_services;

        SELECT 	'Estado de las bases de datos'

        IF (SELECT COUNT(1) FROM sys.databases WHERE state_desc NOT IN('ONLINE')
				OR user_access_desc IN('SINGLE_USER','RESTRICTED_USER')) > 0
				SELECT	name AS base_datos,
						state_desc AS estado,
						user_access_desc AS acceso,
						GETDATE() AS fecha_actual
				FROM	sys.databases
				WHERE	state_desc NOT IN('ONLINE')
						OR user_access_desc IN('SINGLE_USER','RESTRICTED_USER')
		ELSE
				SELECT 'Todas las bases de datos se encuentran en modo ONLINE y MULTI_USER' AS estado_BDs;

		SELECT 	'Propiedades de la instancia'

		SELECT	SERVERPROPERTY('MachineName') AS nombre_maquina, 
				SERVERPROPERTY('ServerName') AS nombre_instancia,
				LEFT(@@VERSION, 25) AS version_sql,
				SERVERPROPERTY('Edition') AS edicion, 
				SERVERPROPERTY('ProductLevel') AS servicePack,
				SERVERPROPERTY('ProductUpdateLevel') AS actualizacion,
				SERVERPROPERTY('ProductVersion') AS version_producto,
				SERVERPROPERTY('ProductUpdateReference') AS referencia_actualizacion,
				SERVERPROPERTY('Collation') Colacion, 
				CASE WHEN SERVERPROPERTY('IsHadrEnabled') = 1 THEN 'La instancia está habilitada como cluster de alwayson'
					ELSE 'La instancia NO está habilitada como cluster de alwayson' END AS habilitado_HADR,
				CASE SERVERPROPERTY('HadrManagerStatus') WHEN 0 THEN 'No iniciado, pendiente comunicación'
														 WHEN 1 THEN 'Iniciado y corriendo'
														 WHEN 2 THEN 'No iniciado y fallido' END AS estado_HADR,
				CASE WHEN SERVERPROPERTY('InstanceDefaultBackupPath') IS NULL THEN 'Ruta de respaldo por VERITAS'
																 ELSE SERVERPROPERTY('InstanceDefaultBackupPath') END AS ruta_respaldos,
				SERVERPROPERTY('InstanceDefaultDataPath') AS ruta_datos,
				SERVERPROPERTY('InstanceDefaultLogPath') AS ruta_logs,
				CASE SERVERPROPERTY('IsIntegratedSecurityOnly') WHEN 0 THEN 'Autenticación mixta, Windows y SQL'
																WHEN 1 THEN 'Autenticación integrada (Autenticación Windows)'
																WHEN NULL THEN 'No aplica' END AS tipo_autenticacion,
				CONNECTIONPROPERTY('net_transport') AS net_transport,
				CONNECTIONPROPERTY('protocol_type') AS protocol_type,
				CONNECTIONPROPERTY('auth_scheme') AS auth_scheme,
				CONNECTIONPROPERTY('local_net_address') AS local_net_address,
				CONNECTIONPROPERTY('local_tcp_port') AS local_tcp_port

		SELECT 	'Estado de los discos'

		SELECT 	DISTINCT vs.volume_mount_point AS punto_montaje,
			   	vs.file_system_type AS tipo_fielsytem,
			   	vs.logical_volume_name AS nombre_logico,
			   	CONVERT(DECIMAL(18,2), vs.total_bytes/1073741824.0) AS tamano_GB,
			   	CONVERT(DECIMAL(18,2), vs.available_bytes/1073741824.0) AS espacio_disponible_GB,  
			   	CONVERT(DECIMAL(18,2), vs.available_bytes * 1. / vs.total_bytes * 100.) AS porcentaje_disponible
		FROM   	sys.master_files AS mf WITH(NOLOCK)
			   	CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.[file_id]) AS vs
		ORDER BY vs.volume_mount_point OPTION(RECOMPILE);

		SELECT 	'Latencia de los discos'

		SELECT 	DB_NAME(fs.database_id) AS base_datos,
			   	CAST(fs.io_stall_read_ms/(1.0 + fs.num_of_reads) AS NUMERIC(10,1)) AS promedio_lectura_ms,
			   	CAST(fs.io_stall_write_ms/(1.0 + fs.num_of_writes) AS NUMERIC(10,1)) AS promedio_escritura_ms,
			   	CAST((fs.io_stall_read_ms + fs.io_stall_write_ms)/(1.0 + fs.num_of_reads + fs.num_of_writes) AS NUMERIC(10,1)) AS promedio_io_ms,
			   	CONVERT(DECIMAL(18,2), mf.size/128.0) AS tamano_archivo_MB,
			   	mf.physical_name,
			   	mf.type_desc,
			   	fs.io_stall_read_ms,
			   	fs.num_of_reads, 
			   	fs.io_stall_write_ms,
			   	fs.num_of_writes
		FROM   	sys.dm_io_virtual_file_stats(NULL,NULL) AS fs
			JOIN sys.master_files AS mf WITH(NOLOCK) ON fs.database_id = mf.database_id
			   	AND fs.[file_id] = mf.[file_id]
		ORDER BY promedio_io_ms DESC OPTION(RECOMPILE)

		SELECT 	'Estadísticas CPU'

		;WITH estadisticas_cpu AS
		(SELECT pa.idbd,
			    DB_NAME(pa.idbd) AS base_datos,
				SUM(qs.total_worker_time/1000) AS tiempo_cpu_ms
		 FROM   sys.dm_exec_query_stats qs WITH(NOLOCK)
			    CROSS APPLY(SELECT CONVERT(INT, value) AS idbd 
						    FROM   sys.dm_exec_plan_attributes(qs.plan_handle)
							WHERE  attribute = N'dbid') AS pa
		 GROUP BY pa.idbd)
		SELECT 	ROW_NUMBER() OVER(ORDER BY tiempo_cpu_ms DESC) AS uso_cpu,
			   	base_datos,
			   	tiempo_cpu_ms, 
			   	CAST(tiempo_cpu_ms * 1.0 / SUM(tiempo_cpu_ms) OVER() * 100.0 AS DECIMAL(5, 2)) AS porcentaje_uso_cpu
		FROM   	estadisticas_cpu
		WHERE  	idbd <> 32767 -- ResourceDB
		ORDER BY uso_cpu OPTION(RECOMPILE);

		SELECT 	'Estadísticas Discos'

		;WITH estadisticas_io AS
		(SELECT DB_NAME(fs.database_id) AS base_datos,
			    CAST(SUM(fs.num_of_bytes_read + fs.num_of_bytes_written) / 1048576 AS DECIMAL(12, 2)) AS io_mb
		 FROM   sys.dm_io_virtual_file_stats(NULL, NULL) AS fs
		 GROUP BY fs.database_id)
		SELECT ROW_NUMBER() OVER(ORDER BY io_mb DESC) AS uso_disco,
			   base_datos,
			   io_mb AS io_total_mb,
			   CAST(io_mb / SUM(io_mb) OVER() * 100.0 AS DECIMAL(5,2)) AS porcentaje_uso_disco
		FROM   estadisticas_io
		ORDER BY uso_disco OPTION(RECOMPILE);

		SELECT 	'Estadísticas Memoria'

		;WITH estadisticas_memoria AS
		(SELECT DB_NAME(database_id) AS base_datos,
				CAST(count(1) * 8 / 1024.0 AS DECIMAL(10,2)) AS tamano_cache
		FROM    sys.dm_os_buffer_descriptors WITH(NOLOCK)
		WHERE   database_id <> 32767 -- ResourceDB
		GROUP BY DB_NAME(database_id))
		SELECT ROW_NUMBER() OVER(ORDER BY tamano_cache DESC) AS uso_memoria,
				base_datos,
				tamano_cache AS memoria_mb,
				CAST(tamano_cache / SUM(tamano_cache) OVER() * 100.0 AS DECIMAL(5,2)) AS porcentaje_uso_memoria
		FROM   estadisticas_memoria
		ORDER BY uso_memoria OPTION(RECOMPILE);

        SELECT 	'Estado de las bases de datos en Alwayson'
        IF((SELECT SERVERPROPERTY('IsHadrEnabled')) = 0)
            SELECT 'La instancia NO está habilitada como cluster de alwayson'
        ELSE
        SELECT	ag.name AS nombre_grupo,
				adc.database_name AS base_datos,
                CASE drs.is_local WHEN 1 THEN 'PRIMARIO' ELSE 'SECUNDARIO' END AS nodo,
				ar.replica_server_name AS instancia_replica,
				ar.endpoint_url AS endpoint,
				ar.availability_mode_desc AS modo_sincronizacion,
				ar.failover_mode_desc AS tipo_failover,
				ar.read_only_routing_url AS ruta_solo_lectura,
				ar.primary_role_allow_connections_desc AS conexiones_en_primario,
				ar.secondary_role_allow_connections_desc AS conexiones_en_secundario,
				ar.seeding_mode_desc AS modo_semilla,
				ag.automated_backup_preference_desc AS preferencia_respaldo,
				CASE drs.is_suspended WHEN 0 THEN 'No'
										WHEN 1 THEN 'Si' END AS suspendida,
				drs.database_state_desc AS estado_bd,
				drs.suspend_reason_desc AS motivo_suspension,
				drs.synchronization_health_desc AS estado_cluster,
				drs.synchronization_state_desc AS estado_sincronizacion,
				drs.secondary_lag_seconds AS segundos_retrazo
		FROM	sys.dm_hadr_database_replica_states AS drs
			JOIN sys.availability_databases_cluster AS adc ON drs.group_id = adc.group_id
				AND drs.group_database_id = adc.group_database_id
			JOIN sys.availability_groups AS ag ON ag.group_id = drs.group_id
			JOIN sys.availability_replicas AS ar ON drs.group_id = ar.group_id
				AND drs.replica_id = ar.replica_id
		WHERE	drs.is_primary_replica = 0
		ORDER BY ag.name, ar.replica_server_name, adc.[database_name] OPTION(RECOMPILE);

		SELECT  ag.name AS grupo_disponibilidad,
				agl.dns_name AS listener,
				agl.port AS puerto,
				agl.ip_configuration_string_FROM_cluster AS cadena_configuracion_del_cluster,
				lip.ip_address AS direccion_IP,
				lip.ip_subnet_mask AS mascara_subred,
				CASE lip.is_dhcp WHEN 0 THEN 'NO' WHEN 1 THEN 'SI' END AS DHCP,
				lip.state_desc AS estado,
				'ALTER AVAILABILITY GROUP ' + ag.name + ' ADD LISTENER ''<--Nombre_listener-->'' (WITH IP((''10.XX.XX.XX'',''255.XX.XX.XX'')), PORT = ' + CONVERT(VARCHAR, agl.port) + ')' AS tsql_listener
		FROM    sys.availability_group_listeners AS agl
			JOIN sys.availability_group_listener_ip_addresses AS lip ON agl.listener_id = lip.listener_id
			JOIN sys.availability_groups AS ag on agl.group_id = ag.group_id;

        SELECT 	'Tamaño filegropus'

        SELECT 	@@SERVERNAME AS instancia,
               	DB_NAME(mf.database_id) AS base_de_datos,
               	vs.volume_mount_point AS disco,
               	CASE WHEN mf.max_size > -1 THEN	(CAST(SUM(mf.max_size)*8./1024 AS DECIMAL(8,0)))
				     ELSE (CAST(vs.total_bytes/1048576.0 AS DECIMAL(8,0))) END AS total_asignado_MB,
               	CAST(SUM(mf.size)*8./1024 AS DECIMAL(8,0)) AS actual_MB,
               	CAST(vs.total_bytes/1048576.0 AS DECIMAL(8,0)) - CAST(SUM(mf.size)*8./1024 AS DECIMAL(8,0)) AS libre,
               	CAST(vs.total_bytes/1048576.0 AS DECIMAL(8,0)) AS total_disco_MB,
               	mf.type_desc AS tipo,
               	CASE WHEN mf.max_size > -1 THEN CONVERT(VARCHAR, mf.max_size) ELSE 'Ilimitado' END AS tamano_asignado_MB
        FROM   	sys.master_files mf
            CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.file_id) AS vs
        GROUP BY vs.total_bytes, mf.max_size, DB_NAME(mf.database_id), mf.type_desc, vs.available_bytes, vs.volume_mount_point
        ORDER BY DB_NAME(mf.database_id)
    
        SELECT 	'Jobs fallidos'

        SELECT 	DISTINCT jh.server AS instancia,
               	jh.step_id AS numero_paso,
               	jh.step_name AS nombre_paso,
               	substring(j.name,1,140) AS nombre_job,
               	msdb.dbo.agent_datetime(jh.run_date, jh.run_time) AS fecha_ejecucion,
               	jh.run_duration duracion_paso,
               	CASE jh.run_status WHEN 0 THEN 'fallo'
               	                   WHEN 1 THEN 'exitoso'
               	                   WHEN 2 THEN 'reintento...'
               	                   WHEN 3 THEN 'cancelado'
               	                   WHEN 4 THEN 'en progreso...'
               	END AS estado_ejecucion,
               	jh.message AS mensaje_error
        FROM   	msdb.dbo.sysjobs AS j
			JOIN msdb.dbo.sysjobhistory AS jh ON jh.job_id = j.job_id
        WHERE  	jh.run_status NOT IN(1, 4)
			AND jh.step_id != 0
			AND	run_date >= CONVERT(CHAR(8), (SELECT DATEADD(DAY,(-1), GETDATE())), 112)

        SELECT 	'Bloqueos en la instancia'

        IF((SELECT TOP 1 wait_time FROM sys.dm_exec_requests WHERE blocking_session_id > 0 AND wait_time > 60000) IS NOT NULL)
            SELECT 	es.session_id,
                   	DB_NAME(er.database_id) AS data_base,
                   	es.login_name AS login_name,
                   	es.host_name AS host_name,
                   	er.blocking_session_id,
                   	er.command,
                   	er.status,
                   	er.start_time,
                   	ws.wait_type,
                   	ws.wait_duration_ms
            FROM   	sys.dm_exec_sessions AS es
				JOIN sys.dm_exec_requests AS er ON es.session_id = er.session_id
				JOIN sys.dm_os_waiting_tasks ws ON es.session_id = ws.session_id
            WHERE  	er.blocking_session_id > 0
        ELSE
            SELECT 'No hay bloqueo'

        SELECT 'Jobs en ejecución'

		SELECT 	ja.job_id,
			   	j.name AS job_name,
			   	ja.start_execution_date,      
			   	ISNULL(last_executed_step_id, 0) + 1 AS current_executed_step_id,
			   	js.step_name,
			   	js.command
		FROM   	msdb.dbo.sysjobactivity AS ja
			LEFT JOIN msdb.dbo.sysjobhistory AS jh ON ja.job_history_id = jh.instance_id
			JOIN msdb.dbo.sysjobs AS j ON ja.job_id = j.job_id
			JOIN msdb.dbo.sysjobsteps AS js ON ja.job_id = js.job_id
				AND ISNULL(ja.last_executed_step_id, 0) + 1 = js.step_id
		WHERE  	ja.session_id = (SELECT TOP 1 session_id FROM msdb.dbo.syssessions ORDER BY agent_start_date DESC)
			AND	start_execution_date IS NOT NULL
			AND	stop_execution_date IS NULL
		ORDER BY ja.start_execution_date ASC
	END
