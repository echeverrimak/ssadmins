﻿USE [master]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[sp_Disponibilidad] AS
BEGIN
SET NOCOUNT ON;
SET ANSI_WARNINGS ON;
SET QUOTED_IDENTIFIER ON;

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

IF (SELECT COUNT(1) FROM sys.databases WHERE state_desc NOT IN('ONLINE','OFFLINE','EMERGENCY','RESTORING')
		AND user_access_desc NOT IN('SINGLE_USER','RESTRICTED_USER')) > 0
		SELECT	name AS base_datos,
				state_desc AS estado,
				user_access_desc AS acceso,
				GETDATE() AS fecha_actual
		FROM	sys.databases
		WHERE	state_desc NOT IN('ONLINE','OFFLINE','EMERGENCY','RESTORING')
				AND user_access_desc NOT IN('SINGLE_USER','RESTRICTED_USER')
ELSE
		SELECT 'Todas las bases de datos se encuentran en modo ONLINE' AS estado_BDs;
END
