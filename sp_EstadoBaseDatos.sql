﻿USE [master]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER procedure [dbo].[sp_EstadoBaseDatos] as
begin
	set nocount on
	;with estadisticas_cpu as
	(select pa.idbd,
			db_name(pa.idbd) base_datos,
			sum(qs.total_worker_time/1000) tiempo_cpu_ms
	 from   sys.dm_exec_query_stats qs with(nolock)
			cross apply(select convert(int, value)  idbd 
						from   sys.dm_exec_plan_attributes(qs.plan_handle)
						where  attribute = N'dbid') pa
	 group by pa.idbd)
	select @@servername nombre_instancia,
		    base_datos,
		    cast(tiempo_cpu_ms * 1.0 / sum(tiempo_cpu_ms) over() * 100.0 as decimal(5, 2)) porcentaje_cpu
	into   ##tempCPU
	from   estadisticas_cpu
	where  idbd <> 32767 -- ResourceDB
		
	;with estadisticas_io as
	(select db_name(fs.database_id) base_datos,
			cast(sum(fs.num_of_bytes_read + fs.num_of_bytes_written) / 1048576 as decimal(12, 2)) io_mb
	 from   sys.dm_io_virtual_file_stats(null, null) fs
	 group by fs.database_id)
	select @@servername nombre_instancia,
		   base_datos,
		   cast(io_mb / sum(io_mb) over() * 100.0 as decimal(5,2)) porcentaje_io
	into   ##tempIO
	from   estadisticas_io
	
	;with estadisticas_buffer as
	(select db_name(database_id) base_datos,
			cast(count(1) * 8 / 1024.0 as decimal(10,2)) tamano_cache
	 from    sys.dm_os_buffer_descriptors with(nolock)
	 where   database_id <> 32767 -- ResourceDB
	 group by db_name(database_id))
	select @@servername nombre_instancia,
		   base_datos,
		   cast(tamano_cache / sum(tamano_cache) over() * 100.0 as decimal(5,2)) porcentaje_buffer
	into   ##tempBuffer
	from   estadisticas_buffer
	
	select @@servername nombre_instancia,
		   database_id,
		   db_name(database_id) base_datos,
		   cast(sum(size) * 8. / 1024 as decimal(10,0)) tamano_bd
	into   ##tempTamano
	from   sys.master_files
	group by db_name(database_id), database_id
	
	;with estado_servicio as
	(select @@servername nombre_instancia,
			servicename nombre_servicio,
			status_desc estado_servicio,
			startup_type_desc tipo_inicio,
			last_startup_time fecha_inicio,
			service_account cuenta_servicio
	 from    sys.dm_server_services),
	 servicio as
	(select @@servername nombre_instancia,
			name base_datos
	 from    sys.databases)
	select  es.nombre_instancia,
		    es.nombre_servicio,
			es.estado_servicio,
			es.tipo_inicio,
			es.cuenta_servicio,
			s.base_datos
	into	##tempServicio
	from    estado_servicio es join servicio s on
			es.nombre_instancia = s.nombre_instancia
	where   es.nombre_servicio not like 'SQL Server Agent%'

	select c.nombre_instancia,
		   s.nombre_servicio,
		   s.estado_servicio,
		   s.tipo_inicio,
		   s.cuenta_servicio,
		   db.name,
		   db.state_desc,
		   db.user_access_desc,
		   t.tamano_bd,
		   c.porcentaje_cpu,
		   b.porcentaje_buffer
	--into   temporalBD
	from   ##tempCPU c join ##tempIO i on
		   c.base_datos = i.base_datos join ##tempBuffer b on
		   c.base_datos = b.base_datos join ##tempTamano t on
		   c.base_datos = t.base_datos join ##tempServicio s on
		   c.base_datos = s.base_datos join sys.databases db on
		   t.database_id = db.database_id
	
	drop table ##tempBuffer
	drop table ##tempCPU
	drop table ##tempIO
	drop table ##tempTamano
	drop table ##tempServicio
end
