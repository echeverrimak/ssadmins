USE [master]
GO
/****** Object:  StoredProcedure [dbo].[sp_InfoInstancia]    Script Date: 16/06/2020 11:29:45 p. m. ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER procedure [dbo].[sp_InfoInstancia] (
@ayuda int = 0,
@propiedades int = 0,
@servicios int = 0,
@estado int = 0,
@discos int = 0,
@latencia_discos int = 0,
@uso_cpu int = 0,
@uso_io int = 0,
@uso_buffer int = 0,
@alwayson int = 0,
@filegroups int = 0,
@jobs int = 0,
@bloqueos int = 0,
@indices int = 0,
@estadisticas int = 0,
@jobs_ejecucion int = 0,
@respaldo int = 0
) with recompile
as
begin
    set nocount on;
    set transaction isolation level read uncommitted;
    set quoted_identifier on;
    set ansi_padding on;
    set concat_null_yields_null on;
    set ansi_warnings on;
    set numeric_roundabort off;
    set arithabort on;

	if(@ayuda = 1)
		print '
Procedimiento almacenado sp_InfoInstancia (versión 1.1) por Oscar Echeverri - Administrador base de datos

Este procedimiento muestra información general sobre la instancia de base de datos.
Puede ejecutarse en cualquier momento y no debe generar afectación de servicio ya que éste utiliza las DMVs y los
DMF de SQL.

Ejecutar en la master.

Ejemplo de uso del procedimiento:

- Para obtener la información completa de la instancia: exec sp_InfoInstancia

- Para obtener la inromación de uno de los parámetros se debe utilizar la siguiente sintaxis:
	exec sp_InfoInstancia @parámetro = 1
	ejemplo: exec sp_InfoInstancia @uso_io

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

@uso_io - Muestra el I/O por cada base de datos

@uso_buffer - Muestra el uso de la memoria por cada base de datos

@alwayson - Muestra si la instancia de base de datos está habilitada como nodo del cluster de alwayson
			   y el estado de la sincronización 

@filegroups - Muestra información sobre los archivos .mdf, .ndf y .ldf de las bases de datos

@jobs - Muestra el detalle de los jobs fallidos en la instancia de base de datos

@bloqueos - Muestra si actualmente existen bloqueos en la instancia de base de datos

@indices - Muestra información sobre el nivel de fragmentación de los índices de las bases de datos y genera
		   el script de recreación y/o reorganización de los índices, según si porcentaje de fragmentación

@estadisticas - Muestra información sobre las estadísticas de las tablas de las bases de datos y genera el 
				script de actualización de cada estadística. Se debe validar la última fecha de actualización 

@version int - Muestra la versión del motor de base de datos

@respaldo int - Muestra el historial de respaldos de las bases de datos

@jobs_ejecucion - Muestra los jobs que actualmente se están ejecutando en la instancia'

	if(@propiedades = 1)
	begin
		SELECT serverproperty('MachineName') nombre_maquina, 
			   serverproperty('ServerName') nombre_instancia, 
			   serverproperty('Edition') edicion, 
			   serverproperty('ProductLevel') servicePack,
			   serverproperty('ProductUpdateLevel') actualizacion,
			   serverproperty('ProductVersion') version_producto,
			   serverproperty('ProductUpdateReference') referencia_actualizacion,
			   serverproperty('Collation') Colacion, 
			   case when serverproperty('IsHadrEnabled') = 1 then 'La instancia está habilitada como cluster de alwayson'
			        else 'La instancia NO está habilitada como cluster de alwayson' end habilitado_hadr
	end;

    if(@servicios = 1)
    begin
        select servicename,
               status_desc,
               startup_type_desc,
               last_startup_time,
               service_account
        from   sys.dm_server_services
    end;

    if(@estado = 1)
    begin
        select convert(nvarchar(20), name) base_datos,
               convert(nvarchar(20), state_desc) estado,
               convert(nvarchar(15), user_access_desc) acceso
        from   sys.databases
    end;

	if(@discos = 1)
	begin
		select distinct vs.volume_mount_point punto_montaje,
			   vs.file_system_type sistema_archivos,
			   vs.logical_volume_name nombre_logico,
			   convert(decimal(18,2), vs.total_bytes/1073741824.0) tamano_GB,
			   convert(decimal(18,2), vs.available_bytes/1073741824.0) espacio_disponible_GB,  
			   convert(decimal(18,2), vs.available_bytes * 1. / vs.total_bytes * 100.) porcentaje_utilizado
		from   sys.master_files mf with(nolock)
			   cross apply sys.dm_os_volume_stats(mf.database_id, mf.[file_id])  vs
		order by vs.volume_mount_point option(recompile);
	end;

	if(@latencia_discos = 1)
	begin
		select db_name(fs.database_id) base_datos,
			   cast(fs.io_stall_read_ms/(1.0 + fs.num_of_reads) as numeric(10,1)) promedio_lectura_ms,
			   cast(fs.io_stall_write_ms/(1.0 + fs.num_of_writes) as numeric(10,1)) promedio_escritura_ms,
			   cast((fs.io_stall_read_ms + fs.io_stall_write_ms)/(1.0 + fs.num_of_reads + fs.num_of_writes) AS NUMERIC(10,1)) promedio_io_ms,
			   convert(decimal(18,2), mf.size/128.0) tamano_archivo_MB,
			   mf.physical_name,
			   mf.type_desc,
			   fs.io_stall_read_ms,
			   fs.num_of_reads, 
			   fs.io_stall_write_ms,
			   fs.num_of_writes
		from   sys.dm_io_virtual_file_stats(null,null) fs join sys.master_files mf with(nolock) on
			   fs.database_id = mf.database_id and
			   fs.[file_id] = mf.[file_id]
		order by promedio_io_ms desc option(recompile)
	end;

	if(@uso_cpu = 1)
	begin
		with estadisticas_cpu as
		(select pa.idbd,
			    db_name(pa.idbd) base_datos,
				sum(qs.total_worker_time/1000) tiempo_cpu_ms
		 from   sys.dm_exec_query_stats qs with(nolock)
			    cross apply(select convert(int, value)  idbd 
						    from   sys.dm_exec_plan_attributes(qs.plan_handle)
							where  attribute = N'dbid') pa
		 group by pa.idbd)
		select row_number() over(order by tiempo_cpu_ms desc) ranking_cpu,
			   base_datos,
			   tiempo_cpu_ms, 
			   cast(tiempo_cpu_ms * 1.0 / sum(tiempo_cpu_ms) over() * 100.0 as decimal(5, 2)) porcentaje_cpu
		from   estadisticas_cpu
		where  idbd <> 32767 -- ResourceDB
		order by ranking_cpu option(recompile)
	end;

	if(@uso_io = 1)
	begin
		with estadisticas_io as
		(select db_name(fs.database_id) base_datos,
			    cast(sum(fs.num_of_bytes_read + fs.num_of_bytes_written) / 1048576 as decimal(12, 2)) io_mb
		 from   sys.dm_io_virtual_file_stats(null, null) fs
		 group by fs.database_id)
		select row_number() over(order by io_mb desc) ranking_io,
			   base_datos,
			   io_mb io_total_mb,
			   cast(io_mb / sum(io_mb) over() * 100.0 as decimal(5,2)) porcentaje_io
		from   estadisticas_io
		order by ranking_io option(recompile)
	end;

	if(@uso_buffer = 1)
	begin
		with estadisticas_buffer as
		(select db_name(database_id) base_datos,
				cast(count(1) * 8 / 1024.0 as decimal(10,2)) tamano_cache
		from    sys.dm_os_buffer_descriptors with(nolock)
		where   database_id <> 32767 -- ResourceDB
		group by db_name(database_id))
		select row_number() over(order by tamano_cache DESC) ranking_buffer,
			   base_datos,
			   tamano_cache buffer_mb,
			   cast(tamano_cache / sum(tamano_cache) over() * 100.0 as decimal(5,2)) porcentaje_buffer
		from   estadisticas_buffer
		order by ranking_buffer option(recompile)
	end;

    if(@alwayson = 1)
    begin
        if((select serverproperty('IsHadrEnabled')) = 0)
            select 'La instancia no está habilitada como cluster de alwayson'
        else
            select distinct convert(nvarchar(30), db.name) base_datos_instancia,
                   convert(nvarchar(20), av.name) grupo_disponibilidad,
                   convert(nvarchar(30), db_name(drs.database_id)) base_datos_AOAG,
                   convert(nvarchar(20), drs.synchronization_state_desc) sincronizacion_AOAG,
                   convert(nvarchar(20), drs.synchronization_health_desc) estado_AOAG
            from   sys.availability_groups av join sys.dm_hadr_database_replica_states drs on
                   av.group_id = drs.group_id right join sys.databases db on
                   drs.database_id = db.database_id
            where  db.database_id not in(1,2,3,4)
        end;
    end;

    if(@filegroups = 1)
    begin
        select convert(nvarchar(20), @@servername) instancia,
               convert(nvarchar(20), db_name(mf.database_id)) base_de_datos,
               convert(nvarchar(5), vs.volume_mount_point) disco,
               case when mf.max_size > -1 then 
               (cast(sum(mf.max_size)*8./1024 as decimal(8,0))) else 
               (cast(vs.total_bytes/1048576.0 as decimal(8,0))) end total_asignado_MB,
               cast(sum(mf.size)*8./1024 as decimal(8,0)) actual_MB,
               cast(vs.total_bytes/1048576.0 as decimal(8,0)) - cast(sum(mf.size)*8./1024 as decimal(8,0)) libre,
               cast(vs.total_bytes/1048576.0 as decimal(8,0)) total_disco_MB,
               convert(nvarchar(10), mf.type_desc) tipo,
               case when mf.max_size > -1 then convert(varchar, mf.max_size) else 'Ilimitado' end tamano_asignado_MB
        from   sys.master_files mf
               cross apply sys.dm_os_volume_stats(mf.database_id, mf.file_id) vs
        group by vs.total_bytes, mf.max_size, db_name(mf.database_id), mf.type_desc, vs.available_bytes, vs.volume_mount_point
        order by db_name(mf.database_id)
    end;

    if(@jobs = 1)
    begin
        select distinct convert(nvarchar(20), jh.server) instancia,
               jh.step_id numero_paso,
               convert(nvarchar(50), jh.step_name) nombre_paso,
               convert(nvarchar(50), substring(j.name,1,140)) nombre_job,
               msdb.dbo.agent_datetime(jh.run_date, jh.run_time) fecha_ejecucion,
               jh.run_duration duracion_paso,
               case jh.run_status when 0 then 'fallo'
                                  when 1 then 'exitoso'
                                  when 2 then 'reintento...'
                                  when 3 then 'cancelado'
                                  when 4 then 'en progreso...'
               end estado_ejecucion,
               convert(nvarchar(500), jh.message) mensaje_error
        from   msdb.dbo.sysjobs j join msdb.dbo.sysjobhistory jh on
               jh.job_id = j.job_id
        where  jh.run_status not in(1, 4) and
               jh.step_id != 0 and
               run_date >= convert(char(8), (select dateadd(day,(-1), getdate())), 112)
    end;

    if(@bloqueos = 1)
    begin
        if((select top 1 wait_time from sys.dm_exec_requests where blocking_session_id > 0 and wait_time > 60000) is not null)
            select es.session_id,
                    convert(nvarchar(20), db_name(er.database_id)) data_base,
                    convert(nvarchar(50), es.login_name) login_name,
                    convert(nvarchar(20), es.host_name) host_name,
                    er.blocking_session_id,
                    er.command,
                    er.status,
                    er.start_time,
                    ws.wait_type,
                    ws.wait_duration_ms
             from   sys.dm_exec_sessions es join sys.dm_exec_requests er on
                    es.session_id = er.session_id join sys.dm_os_waiting_tasks ws on
                    es.session_id = ws.session_id
             where  er.blocking_session_id > 0
        else
             select 'No hay bloqueo'
    end;

    if(@indices = 1)
    begin
        select convert(nvarchar(10), s.name) esquema, 
               convert(nvarchar(20), t.name) tabla, 
               convert(nvarchar(50), case when ips.index_type_desc = 'HEAP' then 'Tabla sin índice' else i.name end) indice,
               convert(nvarchar(20), ips.index_type_desc) tipo_datos,
               ips.avg_fragmentation_in_percent porcentaje,
               ips.page_count paginas,
               case when (ips.avg_fragmentation_in_percent < 30 and ips.avg_fragmentation_in_percent > 5) then
               'alter index ' + i.name + ' on ' + s.name + '.' + t.name + ' reorganize;'
               when (ips.avg_fragmentation_in_percent > 30) then
               'alter index ' + i.name + ' on ' + s.name + '.' + t.name + ' rebuild;' else
               'índice con un nivel de fragmentación menor al 5%' end sentencia_sql
        from   sys.dm_db_index_physical_stats(db_id(), null, null, null, null) ips join sys.tables t on
               t.object_id = ips.object_id join sys.schemas s on
               t.schema_id = s.schema_id join sys.indexes i on
               i.object_id = ips.object_id and ips.index_id = i.index_id
        where  ips.database_id = db_id()      
        order by ips.avg_fragmentation_in_percent desc
    end;

    if(@estadisticas = 1)
    begin
        select sp.stats_id,
               convert(nvarchar(50), stat.name) name,
               convert(nvarchar(50), stat.filter_definition) filter_definition,
               sp.last_updated,
               sp.rows,
               sp.rows_sampled,
               sp.steps,
               sp.unfiltered_rows,
               sp.modification_counter   
        from   sys.stats stat   
               cross apply sys.dm_db_stats_properties(stat.object_id, stat.stats_id) sp 
    end

	if(@respaldo = 1)
	begin
		select bs.backup_set_id,
			   bs.database_name base_datos,
			   bs.backup_start_date inicio_respaldo,
			   bs.backup_finish_date fin_respaldo,
			   cast(cast(bs.backup_size/1000000 as int) as varchar(14)) + ' ' + 'MB' as tamano,
			   cast(datediff(second, bs.backup_start_date, bs.backup_finish_date) as varchar(4)) + ' ' + 'Segundos' tiempo_respaldo,
			   case bs.type when 'D' then 'Full Backup'
							when 'I' then 'Differential Backup'
							when 'L' then 'Transaction Log Backup'
							when 'F' then 'File or filegroup'
							when 'G' then 'Differential file'
							when 'P' then 'Partial'
							when 'Q' then 'Differential Partial'
			   end as tipo_respaldo,
			   bmf.physical_device_name ruta_respaldo,
			   cast(bs.first_lsn as varchar(50)) as first_lsn,
			   cast(bs.last_lsn as varchar(50)) as last_lsn,
			   bs.server_name,
			   bs.recovery_model
		from   msdb.dbo.backupset bs join msdb.dbo.backupmediafamily bmf on
			   bs.media_set_id = bmf.media_set_id
			   ---where bs.database_name='db_Indicadores_VPF'
		order by bs.backup_start_date desc
	end;

	if(@jobs_ejecucion = 1)
	begin
		select ja.job_id,
			   j.name as job_name,
			   ja.start_execution_date,      
			   isnull(last_executed_step_id, 0) + 1 as current_executed_step_id,
			   Js.step_name,
			   js.command
		from   msdb.dbo.sysjobactivity ja left join msdb.dbo.sysjobhistory jh on
			   ja.job_history_id = jh.instance_id join msdb.dbo.sysjobs j on
			   ja.job_id = j.job_id join msdb.dbo.sysjobsteps js on
			   ja.job_id = js.job_id and isnull(ja.last_executed_step_id, 0) + 1 = js.step_id
		where  ja.session_id = (select top 1 session_id from msdb.dbo.syssessions order by agent_start_date desc) and
			   start_execution_date is not null and
			   stop_execution_date is null
		order by ja.start_execution_date asc
	end;

    if(@ayuda = 0 and
	   @propiedades = 0 and
	   @servicios = 0 and
	   @estado = 0 and
	   @discos = 0 and
	   @latencia_discos = 0 and
	   @uso_cpu = 0 and
	   @uso_io = 0 and
	   @uso_buffer = 0 and
	   @alwayson = 0 and
	   @filegroups = 0 and
	   @jobs = 0 and
	   @bloqueos = 0 and
	   @indices = 0 and
	   @estadisticas = 0 and
	   @jobs_ejecucion = 0 and
	   @respaldo = 0)
    begin
        select getdate()

        select 'Estado de los servicios'

        select servicename,
               status_desc,
               startup_type_desc,
               last_startup_time,
               service_account
        from   sys.dm_server_services

        select 'Estado de las bases de datos'

        select convert(nvarchar(20), name) base_datos,
               convert(nvarchar(20), state_desc) estado,
               convert(nvarchar(15), user_access_desc) acceso
        from   sys.databases

		select 'Propiedades de la instancia'

		SELECT serverproperty('MachineName') nombre_maquina, 
			   serverproperty('ServerName') nombre_instancia, 
			   serverproperty('Edition') edicion, 
			   serverproperty('ProductLevel') servicePack,
			   serverproperty('ProductUpdateLevel') actualizacion,
			   serverproperty('ProductVersion') version_producto,
			   serverproperty('ProductUpdateReference') referencia_actualizacion,
			   serverproperty('Collation') Colacion, 
			   case when serverproperty('IsHadrEnabled') = 1 then 'La instancia NO está habilitada como cluster de alwayson'
			        else 'La instancia está habilitada como cluster de alwayson' end habilitado_hadr

		select 'Estado de los discos'

		select distinct vs.volume_mount_point punto_montaje,
			   vs.file_system_type tipo_fielsytem,
			   vs.logical_volume_name nombre_logico,
			   convert(decimal(18,2), vs.total_bytes/1073741824.0) tamano_GB,
			   convert(decimal(18,2), vs.available_bytes/1073741824.0) espacio_disponible_GB,  
			   convert(decimal(18,2), vs.available_bytes * 1. / vs.total_bytes * 100.) dispobible_porcentaje
		from   sys.master_files mf with(nolock)
			   cross apply sys.dm_os_volume_stats(mf.database_id, mf.[file_id])  vs
		order by vs.volume_mount_point option(recompile);

		select 'Latencia de los discos'

		select db_name(fs.database_id) base_datos,
			   cast(fs.io_stall_read_ms/(1.0 + fs.num_of_reads) as numeric(10,1)) promedio_lectura_ms,
			   cast(fs.io_stall_write_ms/(1.0 + fs.num_of_writes) as numeric(10,1)) promedio_escritura_ms,
			   cast((fs.io_stall_read_ms + fs.io_stall_write_ms)/(1.0 + fs.num_of_reads + fs.num_of_writes) AS NUMERIC(10,1)) promedio_io_ms,
			   convert(decimal(18,2), mf.size/128.0) tamano_archivo_MB,
			   mf.physical_name,
			   mf.type_desc,
			   fs.io_stall_read_ms,
			   fs.num_of_reads, 
			   fs.io_stall_write_ms,
			   fs.num_of_writes
		from   sys.dm_io_virtual_file_stats(null,null) fs join sys.master_files mf with(nolock) on
			   fs.database_id = mf.database_id and
			   fs.[file_id] = mf.[file_id]
		order by promedio_io_ms desc option(recompile)

		select 'Estadísticas CPU'

		;with estadisticas_cpu as
		(select pa.idbd,
			    db_name(pa.idbd) base_datos,
				sum(qs.total_worker_time/1000) tiempo_cpu_ms
		 from   sys.dm_exec_query_stats qs with(nolock)
			    cross apply(select convert(int, value)  idbd 
						    from   sys.dm_exec_plan_attributes(qs.plan_handle)
							where  attribute = N'dbid') pa
		 group by pa.idbd)
		select row_number() over(order by tiempo_cpu_ms desc) ranking_cpu,
			   base_datos,
			   tiempo_cpu_ms, 
			   cast(tiempo_cpu_ms * 1.0 / sum(tiempo_cpu_ms) over() * 100.0 as decimal(5, 2)) porcentaje_cpu
		from   estadisticas_cpu
		where  idbd <> 32767 -- ResourceDB
		order by ranking_cpu option(recompile);

		select 'Estadísticas I/O'

		;with estadisticas_io as
		(select db_name(fs.database_id) base_datos,
			    cast(sum(fs.num_of_bytes_read + fs.num_of_bytes_written) / 1048576 as decimal(12, 2)) io_mb
		 from   sys.dm_io_virtual_file_stats(null, null) fs
		 group by fs.database_id)
		select row_number() over(order by io_mb desc) ranking_io,
			   base_datos,
			   io_mb io_total_mb,
			   cast(io_mb / sum(io_mb) over() * 100.0 as decimal(5,2)) porcentaje_io
		from   estadisticas_io
		order by ranking_io option(recompile);

		select 'Estadísticas Buffer'

		;with estadisticas_buffer as
		(select db_name(database_id) base_datos,
				cast(count(1) * 8 / 1024.0 as decimal(10,2)) tamano_cache
		from    sys.dm_os_buffer_descriptors with(nolock)
		where   database_id <> 32767 -- ResourceDB
		group by db_name(database_id))
		select row_number() over(order by tamano_cache DESC) ranking_buffer,
			   base_datos,
			   tamano_cache buffer_mb,
			   cast(tamano_cache / sum(tamano_cache) over() * 100.0 as decimal(5,2)) porcentaje_buffer
		from   estadisticas_buffer
		order by ranking_buffer option(recompile);

        select 'Estado de las bases de datos en Alwayson'
        if((select serverproperty('IsHadrEnabled')) = 0)
            select 'La instancia no está habilitada como cluster de alwayson'
        else
        select distinct convert(nvarchar(30), db.name) base_datos_instancia,
               convert(nvarchar(20), av.name) grupo_disponibilidad,
               convert(nvarchar(30), db_name(drs.database_id)) base_datos_AOAG,
               convert(nvarchar(20), drs.synchronization_state_desc) sincronizacion_AOAG,
               convert(nvarchar(20), drs.synchronization_health_desc) estado_AOAG
        from   sys.availability_groups av join sys.dm_hadr_database_replica_states drs on
               av.group_id = drs.group_id right join sys.databases db on
               drs.database_id = db.database_id
        where  db.database_id not in(1,2,3,4)

        select 'Tamaño filegropus'

        select convert(nvarchar(20), @@servername) instancia,
               convert(nvarchar(20), db_name(mf.database_id)) base_de_datos,
               convert(nvarchar(5), vs.volume_mount_point) disco,
               case when mf.max_size > -1 then 
               (cast(sum(mf.max_size)*8./1024 as decimal(8,0))) else 
               (cast(vs.total_bytes/1048576.0 as decimal(8,0))) end total_asignado_MB,
               cast(sum(mf.size)*8./1024 as decimal(8,0)) actual_MB,
               cast(vs.total_bytes/1048576.0 as decimal(8,0)) - cast(sum(mf.size)*8./1024 as decimal(8,0)) libre,
               cast(vs.total_bytes/1048576.0 as decimal(8,0)) total_disco_MB,
               convert(nvarchar(10), mf.type_desc) tipo,
               case when mf.max_size > -1 then convert(varchar, mf.max_size) else 'Ilimitado' end tamano_asignado_MB
        from   sys.master_files mf
               cross apply sys.dm_os_volume_stats(mf.database_id, mf.file_id) vs
        group by vs.total_bytes, mf.max_size, db_name(mf.database_id), mf.type_desc, vs.available_bytes, vs.volume_mount_point
        order by db_name(mf.database_id)
    
        select 'Jobs fallidos'

        select distinct convert(nvarchar(20), jh.server) instancia,
               jh.step_id numero_paso,
               convert(nvarchar(50), jh.step_name) nombre_paso,
               convert(nvarchar(50), substring(j.name,1,140)) nombre_job,
               msdb.dbo.agent_datetime(jh.run_date, jh.run_time) fecha_ejecucion,
               jh.run_duration duracion_paso,
               case jh.run_status when 0 then 'fallo'
                                  when 1 then 'exitoso'
                                  when 2 then 'reintento...'
                                  when 3 then 'cancelado'
                                  when 4 then 'en progreso...'
               end estado_ejecucion,
               convert(nvarchar(500), jh.message) mensaje_error
        from   msdb.dbo.sysjobs j join msdb.dbo.sysjobhistory jh on
               jh.job_id = j.job_id
        where  jh.run_status not in(1, 4) and
               jh.step_id != 0 and
               run_date >= convert(char(8), (select dateadd(day,(-1), getdate())), 112)

        select 'Bloqueos en la instancia'

        if((select top 1 wait_time from sys.dm_exec_requests where blocking_session_id > 0 and wait_time > 60000) is not null)
            select es.session_id,
                   convert(nvarchar(20), db_name(er.database_id)) data_base,
                   convert(nvarchar(50), es.login_name) login_name,
                   convert(nvarchar(20), es.host_name) host_name,
                   er.blocking_session_id,
                   er.command,
                   er.status,
                   er.start_time,
                   ws.wait_type,
                   ws.wait_duration_ms
            from   sys.dm_exec_sessions es join sys.dm_exec_requests er on
                   es.session_id = er.session_id join sys.dm_os_waiting_tasks ws on
                   es.session_id = ws.session_id
            where  er.blocking_session_id > 0
        else
            select 'No hay bloqueo'

        select 'Fragmentación de índices'

        select convert(nvarchar(10), s.name) esquema, 
               convert(nvarchar(20), t.name) tabla, 
               convert(nvarchar(50), case when ips.index_type_desc = 'HEAP' then 'Tabla sin índice' else i.name end) indice,
               convert(nvarchar(20), ips.index_type_desc) tipo_datos,
               ips.avg_fragmentation_in_percent porcentaje,
               ips.page_count paginas,
               case when (ips.avg_fragmentation_in_percent < 30 and ips.avg_fragmentation_in_percent > 5) then
               'alter index ' + i.name + ' on ' + s.name + '.' + t.name + ' reorganize;'
               when (ips.avg_fragmentation_in_percent > 30) then
               'alter index ' + i.name + ' on ' + s.name + '.' + t.name + ' rebuild;' else
               'índice con un nivel de fragmentación menor al 5%' end sentencia_sql
        from   sys.dm_db_index_physical_stats(db_id(), null, null, null, null) ips join sys.tables t on
               t.object_id = ips.object_id join sys.schemas s on
               t.schema_id = s.schema_id join sys.indexes i on
               i.object_id = ips.object_id and ips.index_id = i.index_id
        where  ips.database_id = db_id()      
        order by ips.avg_fragmentation_in_percent desc

        select 'Estadísticas de la base de datos'

        select sp.stats_id,
               convert(nvarchar(50), stat.name) name,
               convert(nvarchar(50), stat.filter_definition) filter_definition,
               sp.last_updated,
               sp.rows,
               sp.rows_sampled,
               sp.steps,
               sp.unfiltered_rows,
               sp.modification_counter   
        from   sys.stats stat   
               cross apply sys.dm_db_stats_properties(stat.object_id, stat.stats_id) sp  
        --where  stat.object_id = object_id('tabla') -- filtrar por objeto
        --where  last_updated < '1800-01-01' -- filtrar por fecha

		select bs.backup_set_id,
			   bs.database_name base_datos,
			   bs.backup_start_date inicio_respaldo,
			   bs.backup_finish_date fin_respaldo,
			   cast(cast(bs.backup_size/1000000 as int) as varchar(14)) + ' ' + 'MB' as tamano,
			   cast(datediff(second, bs.backup_start_date, bs.backup_finish_date) as varchar(4)) + ' ' + 'Segundos' tiempo_respaldo,
			   case bs.type when 'D' then 'Full Backup'
							when 'I' then 'Differential Backup'
							when 'L' then 'Transaction Log Backup'
							when 'F' then 'File or filegroup'
							when 'G' then 'Differential file'
							when 'P' then 'Partial'
							when 'Q' then 'Differential Partial'
			   end as tipo_respaldo,
			   bmf.physical_device_name ruta_respaldo,
			   cast(bs.first_lsn as varchar(50)) as first_lsn,
			   cast(bs.last_lsn as varchar(50)) as last_lsn,
			   bs.server_name,
			   bs.recovery_model
		from   msdb.dbo.backupset bs join msdb.dbo.backupmediafamily bmf on
			   bs.media_set_id = bmf.media_set_id
			   ---where bs.database_name='db_Indicadores_VPF'
		order by bs.backup_start_date desc

		select ja.job_id,
			   j.name as job_name,
			   ja.start_execution_date,      
			   isnull(last_executed_step_id, 0) + 1 as current_executed_step_id,
			   Js.step_name,
			   js.command
		from   msdb.dbo.sysjobactivity ja left join msdb.dbo.sysjobhistory jh on
			   ja.job_history_id = jh.instance_id join msdb.dbo.sysjobs j on
			   ja.job_id = j.job_id join msdb.dbo.sysjobsteps js on
			   ja.job_id = js.job_id and isnull(ja.last_executed_step_id, 0) + 1 = js.step_id
		where  ja.session_id = (select top 1 session_id from msdb.dbo.syssessions order by agent_start_date desc) and
			   start_execution_date is not null and
			   stop_execution_date is null
		order by ja.start_execution_date asc
end