#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import subprocess
import json
import os
import sys
from datetime import datetime

"""Ejecutar un comando y devolver su salida"""
def EjecutarComando(comando):
    try:
        salida = subprocess.check_output(comando, shell=True, stderr=subprocess.DEVNULL, universal_newlines=True, timeout=10)
        return salida.strip()
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, Exception):
        return None

"""Obtener información de Proxmox VE específica del nodo"""
def ObtenerInfoProxmoxNodo():
    info = {}

    # Obtener nombre del nodo actual y estado
    comando_nodo = "pvesh get /nodes/localhost/status --output-format json 2>/dev/null"
    estadoNodo = EjecutarComando(comando_nodo)
    if estadoNodo:
        try:
            nodo_info = json.loads(estadoNodo)
            info['Nombre_Nodo'] = nodo_info.get('node', 'Desconocido')
            info['Estado_Nodo'] = 'online' if nodo_info.get('status') == 'online' else 'offline'

            # Información de memoria del nodo
            info['Memoria_Total_GB'] = round(nodo_info.get('memory', {}).get('total', 0) / (1024**3), 2)
            info['Memoria_Usada_GB'] = round(nodo_info.get('memory', {}).get('used', 0) / (1024**3), 2)
            if info['Memoria_Total_GB'] > 0:
                info['Memoria_Porcentaje'] = round((info['Memoria_Usada_GB'] / info['Memoria_Total_GB']) * 100, 2)
            else:
                info['Memoria_Porcentaje'] = 0
        except (json.JSONDecodeError, KeyError, AttributeError):
            info['Nombre_Nodo'] = 'Desconocido'
            info['Estado_Nodo'] = 'desconocido'

    # VMs en este nodo
    comando_vms = "pvesh get /nodes/localhost/qemu --output-format json 2>/dev/null"
    vms_nodo = EjecutarComando(comando_vms)
    if vms_nodo:
        try:
            vms = json.loads(vms_nodo)
            info['Num_MV_Nodo'] = len(vms)
            info['Num_MV_Iniciadas_Nodo'] = len([vm for vm in vms if vm.get('status') == 'running'])
            info['Num_MV_Paradas_Nodo'] = len([vm for vm in vms if vm.get('status') == 'stopped'])
        except (json.JSONDecodeError, KeyError):
            info['Num_MV_Nodo'] = 0
            info['Num_MV_Iniciadas_Nodo'] = 0
            info['Num_MV_Paradas_Nodo'] = 0

    # Contenedores en este nodo
    comando_ct = "pvesh get /nodes/localhost/lxc --output-format json 2>/dev/null"
    cts_nodo = EjecutarComando(comando_ct)
    if cts_nodo:
        try:
            cts = json.loads(cts_nodo)
            info['Num_Contenedores_Nodo'] = len(cts)
            info['Num_Contenedores_Iniciados_Nodo'] = len([ct for ct in cts if ct.get('status') == 'running'])
            info['Num_Contenedores_Parados_Nodo'] = len([ct for ct in cts if ct.get('status') == 'stopped'])
        except (json.JSONDecodeError, KeyError):
            info['Num_Contenedores_Nodo'] = 0
            info['Num_Contenedores_Iniciados_Nodo'] = 0
            info['Num_Contenedores_Parados_Nodo'] = 0

    # Información del cluster
    comando = "pvesh get /cluster/status --output-format json 2>/dev/null"
    estadoCluster = EjecutarComando(comando)
    if estadoCluster:
        try:
            estadoCluster = json.loads(estadoCluster)

            # Buscar información del quorum
            quorum_info = next((item for item in estadoCluster if item.get('type') == 'quorum'), None)
            if quorum_info:
                info['cluster_ConQuorum'] = quorum_info.get('quorate', False)
            else:
                info['cluster_ConQuorum'] = False

            # Contar nodos
            nodos = [n for n in estadoCluster if n.get('type') == 'node']
            info['Numero_Nodos_Cluster'] = len(nodos)
            info['Numero_Nodos_Online_Cluster'] = len([n for n in nodos if n.get('online', False)])
            info['Numero_Nodos_Offline_Cluster'] = len([n for n in nodos if not n.get('online', True)])

        except (json.JSONDecodeError, KeyError, StopIteration):
            info['cluster_ConQuorum'] = False
            info['Numero_Nodos_Cluster'] = 0
            info['Numero_Nodos_Online_Cluster'] = 0
            info['Numero_Nodos_Offline_Cluster'] = 0

    return info

"""Obtener información de storages Proxmox en el nodo"""
def ObtenerStoragesProxmox():
    info = {'storages': []}

    comando = "pvesh get /storage --output-format json 2>/dev/null"
    storages = EjecutarComando(comando)
    if not storages:
        return info

    try:
        storages = json.loads(storages)
        for storage in storages:
            storage_name = storage['storage']
            storage_type = storage.get('type', 'desconocido')

            storage_info = {
                'nombre': storage_name,
                'tipo': storage_type,
                'contenido': ', '.join(storage.get('content', [])),
                'activado': storage.get('enabled', 0),
                'shared': storage.get('shared', 0)
            }

            # Solo intentar obtener información de espacio para storages locales
            if storage_type in ['dir', 'lvm', 'lvmthin', 'zfspool', 'zfs']:
                # Usar df directamente para evitar problemas con pvesh
                if os.path.exists(f"/var/lib/vz") and 'local' in storage_name.lower():
                    # Para storage local, usar df en /var/lib/vz
                    comando_df = "df -k /var/lib/vz | tail -1"
                    df_output = EjecutarComando(comando_df)
                    if df_output:
                        parts = df_output.split()
                        if len(parts) >= 5:
                            total_kb = int(parts[1])
                            used_kb = int(parts[2])
                            available_kb = int(parts[3])

                            storage_info['total_gb'] = round(total_kb / (1024*1024), 2)
                            storage_info['used_gb'] = round(used_kb / (1024*1024), 2)
                            storage_info['available_gb'] = round(available_kb / (1024*1024), 2)

                            if total_kb > 0:
                                storage_info['porcentaje_uso'] = round((used_kb / total_kb) * 100, 2)

            info['storages'].append(storage_info)

    except (json.JSONDecodeError, KeyError, ValueError):
        pass

    return info

"""Obtener información básica de Ceph"""
def ObtenerInformacionCeph():
    info = {
        'ceph_installed': False,
        'estadoCeph': 'NO_INSTALADO',
        'osd_total': 0,
        'osd_up': 0,
        'osd_down': 0
    }

    if not os.path.exists('/etc/ceph/ceph.conf'):
        return info

    info['ceph_installed'] = True

    # Estado de Ceph
    comando = "ceph health --format json 2>/dev/null || ceph health 2>/dev/null"
    estadoCeph = EjecutarComando(comando)
    if estadoCeph:
        try:
            # Intentar parsear como JSON
            estado_json = json.loads(estadoCeph)
            info['estadoCeph'] = estado_json.get('status', 'UNKNOWN')
        except json.JSONDecodeError:
            # Si no es JSON, usar el output directo
            info['estadoCeph'] = estadoCeph.split()[0] if estadoCeph else 'UNKNOWN'

    # Información básica de OSDs
    comando_osd = "ceph osd stat 2>/dev/null || echo '0 osds: 0 up, 0 in'"
    osd_stat = EjecutarComando(comando_osd)
    if osd_stat:
        # Parsear formato: "12 osds: 12 up, 12 in"
        try:
            parts = osd_stat.split()
            if len(parts) >= 5:
                info['osd_total'] = int(parts[0])
                info['osd_up'] = int(parts[2].rstrip(','))
                info['osd_down'] = info['osd_total'] - info['osd_up']
        except (ValueError, IndexError):
            pass

    # Información básica de pools
    comando_pools = "ceph osd pool ls 2>/dev/null"
    pools_list = EjecutarComando(comando_pools)
    if pools_list:
        pools = [p.strip() for p in pools_list.split('\n') if p.strip()]
        info['pools'] = pools
        info['num_pools'] = len(pools)

    # Información básica de CephFS
    comando_fs = "ceph fs ls 2>/dev/null"
    fs_info = EjecutarComando(comando_fs)
    if fs_info and "No filesystem" not in fs_info:
        fs_list = [line.strip() for line in fs_info.split('\n') if line.strip()]
        info['fs_list'] = fs_list
        info['num_cephfs'] = len(fs_list)

    return info

"""Verificar estado de los servicios Proxmox en el nodo"""
def VerificarServiciosNodo():
    info = {}

    # Servicios Proxmox
    servicios_proxmox = ['pve-cluster', 'pvedaemon', 'pveproxy', 'pvestatd']
    info['servicios_proxmox'] = {}

    for servicio in servicios_proxmox:
        comando = f"systemctl is-active {servicio} 2>/dev/null"
        estado = EjecutarComando(comando)
        info['servicios_proxmox'][servicio] = estado if estado else 'unknown'

    # Verificar si Ceph está instalado para ver sus servicios
    if os.path.exists('/etc/ceph/ceph.conf'):
        info['servicios_ceph'] = {}
        # Solo verificar servicios básicos de Ceph
        servicios_ceph = ['ceph-mon', 'ceph-mgr', 'ceph-osd']

        for servicio in servicios_ceph:
            # Verificar si algún servicio de este tipo está activo
            comando = f"systemctl list-units --all 'ceph*' 2>/dev/null | grep {servicio} | grep running || echo ''"
            salida = EjecutarComando(comando)
            if salida and 'running' in salida:
                info['servicios_ceph'][servicio] = 'active'
            else:
                info['servicios_ceph'][servicio] = 'inactive'

    return info

"""Generar formato XML para Pandora FMS"""
def generarModuloPandora(name, mtype, description, value, min_warn=None, max_warn=None, min_crit=None, max_crit=None, str_warning=None, str_critical=None, module_group=None):
    module = []
    module.append("<module>")
    module.append(f"<name><![CDATA[{name}]]></name>")
    module.append(f"<type><![CDATA[{mtype}]]></type>")
    module.append(f"<description>{description}</description>")
    module.append(f"<data><![CDATA[{value}]]></data>")

    if module_group:
        module.append(f"<module_group><![CDATA[{module_group}]]></module_group>")

    if min_warn is not None:
        module.append(f"<min_warning>{min_warn}</min_warning>")
    if max_warn is not None:
        module.append(f"<max_warning>{max_warn}</max_warning>")
    if min_crit is not None:
        module.append(f"<min_critical>{min_crit}</min_critical>")
    if max_crit is not None:
        module.append(f"<max_critical>{max_crit}</max_critical>")
    if str_warning is not None:
        module.append(f"<str_warning><![CDATA[{str_warning}]]></str_warning>")
    if str_critical is not None:
        module.append(f"<str_critical><![CDATA[{str_critical}]]></str_critical>")

    module.append("</module>")
    return "\n".join(module)

"""Generar la salida completa para Pandora FMS"""
def generarXMLAgentePandora(data_proxmox, data_storages, data_ceph, data_servicios):
    output = []
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Cabecera del agente
    output.append(f"# Agente: Proxmox_Ceph_Monitor")
    output.append(f"# Versión: 1.1")
    output.append(f"# Fecha: {timestamp}")
    output.append("")

    # Módulos de información del nodo Proxmox
    output.append(generarModuloPandora(
        name="Nombre_Nodo",
        mtype="generic_data_string",
        description="Nombre del nodo Proxmox",
        value=data_proxmox.get('Nombre_Nodo', 'Desconocido'),
        module_group="Proxmox Nodo"
    ))

    output.append(generarModuloPandora(
        name="Estado_Nodo",
        mtype="generic_data_string",
        description="Estado del nodo Proxmox",
        value=data_proxmox.get('Estado_Nodo', 'desconocido'),
        str_warning="!online",
        str_critical="!online",
        module_group="Proxmox Nodo"
    ))

    if 'Memoria_Porcentaje' in data_proxmox:
        output.append(generarModuloPandora(
            name="Memoria_Proxmox_Porcentaje",
            mtype="generic_data",
            description="Porcentaje de memoria usado por Proxmox",
            value=data_proxmox.get('Memoria_Porcentaje', 0),
            max_warn=85,
            max_crit=95,
            module_group="Proxmox Nodo"
        ))

    # VMs en este nodo
    output.append(generarModuloPandora(
        name="VMs_Nodo_Total",
        mtype="generic_data",
        description="Numero total de VMs en este nodo",
        value=data_proxmox.get('Num_MV_Nodo', 0),
        module_group="Proxmox VMs Nodo"
    ))

    output.append(generarModuloPandora(
        name="VMs_Nodo_Running",
        mtype="generic_data",
        description="Numero de VMs ejecutandose en este nodo",
        value=data_proxmox.get('Num_MV_Iniciadas_Nodo', 0),
        module_group="Proxmox VMs Nodo"
    ))

    output.append(generarModuloPandora(
        name="VMs_Nodo_Stopped",
        mtype="generic_data",
        description="Numero de VMs paradas en este nodo",
        value=data_proxmox.get('Num_MV_Paradas_Nodo', 0),
        module_group="Proxmox VMs Nodo"
    ))

    # Contenedores en este nodo
    output.append(generarModuloPandora(
        name="Contenedores_Nodo_Total",
        mtype="generic_data",
        description="Numero total de contenedores en este nodo",
        value=data_proxmox.get('Num_Contenedores_Nodo', 0),
        module_group="Proxmox Contenedores Nodo"
    ))

    output.append(generarModuloPandora(
        name="Contenedores_Nodo_Running",
        mtype="generic_data",
        description="Numero de contenedores ejecutandose en este nodo",
        value=data_proxmox.get('Num_Contenedores_Iniciados_Nodo', 0),
        module_group="Proxmox Contenedores Nodo"
    ))

    output.append(generarModuloPandora(
        name="Contenedores_Nodo_Stopped",
        mtype="generic_data",
        description="Numero de contenedores parados en este nodo",
        value=data_proxmox.get('Num_Contenedores_Parados_Nodo', 0),
        module_group="Proxmox Contenedores Nodo"
    ))

    # Información del cluster
    output.append(generarModuloPandora(
        name="Cluster_ConQuorum",
        mtype="generic_proc",
        description="Estado del quorum del cluster (1=Con quorum, 0=Sin quorum)",
        value=1 if data_proxmox.get('cluster_ConQuorum', False) else 0,
        module_group="Proxmox Cluster"
    ))

    output.append(generarModuloPandora(
        name="Nodos_Cluster_Total",
        mtype="generic_data",
        description="Numero total de nodos en el cluster",
        value=data_proxmox.get('Numero_Nodos_Cluster', 0),
        module_group="Proxmox Cluster"
    ))

    output.append(generarModuloPandora(
        name="Nodos_Cluster_Online",
        mtype="generic_data",
        description="Numero de nodos online en el cluster",
        value=data_proxmox.get('Numero_Nodos_Online_Cluster', 0),
        module_group="Proxmox Cluster"
    ))

    output.append(generarModuloPandora(
        name="Nodos_Cluster_Offline",
        mtype="generic_data",
        description="Numero de nodos offline en el cluster",
        value=data_proxmox.get('Numero_Nodos_Offline_Cluster', 0),
        max_warn=0,
        max_crit=1,
        module_group="Proxmox Cluster"
    ))

    # Servicios Proxmox
    for servicio, estado in data_servicios.get('servicios_proxmox', {}).items():
        output.append(generarModuloPandora(
            name=f"Servicio_{servicio}",
            mtype="generic_data_string",
            description=f"Estado del servicio {servicio}",
            value=estado,
            str_warning="!active",
            str_critical="!active",
            module_group="Proxmox Servicios"
        ))

    # Ceph (si está instalado)
    if data_ceph['ceph_installed']:
        # Estado general de Ceph
        output.append(generarModuloPandora(
            name="Ceph_Estado",
            mtype="generic_data_string",
            description="Estado de servicio Ceph",
            value=data_ceph.get('estadoCeph', 'UNKNOWN'),
            str_warning="HEALTH_WARN",
            str_critical="HEALTH_ERR",
            module_group="Ceph"
        ))

        # Estadísticas de OSDs
        output.append(generarModuloPandora(
            name="Ceph_OSD_Total",
            mtype="generic_data",
            description="Numero total de OSD en Ceph",
            value=data_ceph.get('osd_total', 0),
            module_group="Ceph OSD"
        ))

        output.append(generarModuloPandora(
            name="Ceph_OSD_Up",
            mtype="generic_data",
            description="Numero de OSD activos en Ceph",
            value=data_ceph.get('osd_up', 0),
            module_group="Ceph OSD"
        ))

        output.append(generarModuloPandora(
            name="Ceph_OSD_Down",
            mtype="generic_data",
            description="Numero de OSD inactivos en Ceph",
            value=data_ceph.get('osd_down', 0),
            max_warn=0,
            max_crit=0,
            module_group="Ceph OSD"
        ))

        # Pools de Ceph
        if 'num_pools' in data_ceph:
            output.append(generarModuloPandora(
                name="Ceph_Pools_Total",
                mtype="generic_data",
                description="Numero total de pools en Ceph",
                value=data_ceph.get('num_pools', 0),
                module_group="Ceph Pools"
            ))

        # CephFS
        if 'num_cephfs' in data_ceph and data_ceph['num_cephfs'] > 0:
            output.append(generarModuloPandora(
                name="CephFS_Total",
                mtype="generic_data",
                description="Numero de sistemas CephFS",
                value=data_ceph.get('num_cephfs', 0),
                module_group="CephFS"
            ))

        # Servicios Ceph
        if 'servicios_ceph' in data_servicios:
            for servicio, estado in data_servicios['servicios_ceph'].items():
                output.append(generarModuloPandora(
                    name=f"Servicio_{servicio}",
                    mtype="generic_data_string",
                    description=f"Estado del servicio {servicio} de Ceph",
                    value=estado,
                    str_warning="!active",
                    str_critical="!active",
                    module_group="Ceph Servicios"
                ))

    return "\n".join(output)

def main():
    # Recolectar datos específicos de Proxmox y Ceph
    data_proxmox = ObtenerInfoProxmoxNodo()
    data_storages = ObtenerStoragesProxmox()
    data_ceph = ObtenerInformacionCeph()
    data_servicios = VerificarServiciosNodo()

    # Generar salida
    try:
        agent_output = generarXMLAgentePandora(data_proxmox, data_storages, data_ceph, data_servicios)
        print(agent_output)
    except Exception as e:
        print(f"Error al generar la salida: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()