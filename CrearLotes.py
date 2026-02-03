#!/usr/bin/env python3
"""
Módulo para crear lotes en Lote_sv_business agrupados por business_id
"""

from datetime import datetime
from decimal import Decimal


def verificar_y_agregar_columna_lote_id(cursor, conn, logger):
    """
    Verifica si la columna lote_id existe en LiquidacionesSV y la agrega si no existe
    """
    try:
        cursor.execute("""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'LiquidacionesSV' 
            AND COLUMN_NAME = 'lote_id'
            AND TABLE_SCHEMA = DATABASE()
        """)
        
        column_exists = cursor.fetchone()
        
        if not column_exists:
            cursor.execute("""
                ALTER TABLE LiquidacionesSV 
                ADD COLUMN lote_id BIGINT NULL AFTER business_id,
                ADD INDEX idx_liquidaciones_lote_id (lote_id)
            """)
            conn.commit()
            logger.info("✅ Columna lote_id agregada a la tabla LiquidacionesSV")
            return True
        else:
            logger.info("ℹ️ Columna lote_id ya existe en LiquidacionesSV")
            return True
            
    except Exception as e:
        logger.error(f"❌ Error verificando/agregando columna lote_id: {e}")
        return False


def obtener_o_crear_lote_sv_padre(cursor, conn, fecha_lote, business_id, logger):
    """
    Obtiene o crea un registro en Lote_sv (tabla padre) para la fecha y business_id especificados.
    Retorna el id del lote padre. Si la tabla tiene columna business_id (NOT NULL), se envía
    para cumplir con el esquema y evitar error 1364.
    """
    try:
        # Primero verificar si existe la tabla Lote_sv
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_NAME = 'Lote_sv'
        """)
        
        table_exists = cursor.fetchone()
        
        if not table_exists:
            logger.warning("⚠️ La tabla Lote_sv no existe. Se creará automáticamente.")
            # Crear tabla Lote_sv con business_id para coincidir con esquema de producción
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS `Lote_sv` (
                    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    `fecha_lote` DATE NOT NULL,
                    `business_id` VARCHAR(100) NOT NULL,
                    `total_comercios` INT NOT NULL DEFAULT 0,
                    `total_transacciones` INT NOT NULL DEFAULT 0,
                    `total_monto_deposito` DECIMAL(15,2) DEFAULT 0,
                    `estado` ENUM('pendiente','procesado') DEFAULT 'pendiente',
                    `created_at` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
                    `updated_at` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (`id`),
                    INDEX `idx_fecha_lote` (`fecha_lote`),
                    INDEX `idx_business_id` (`business_id`),
                    UNIQUE KEY `unique_fecha_lote_business` (`fecha_lote`, `business_id`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
            """)
            conn.commit()
            logger.info("✅ Tabla Lote_sv creada")
        
        # Verificar si la tabla tiene columna business_id (esquema de producción)
        cursor.execute("""
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'Lote_sv' AND COLUMN_NAME = 'business_id'
        """)
        tiene_business_id = cursor.fetchone() is not None

        if tiene_business_id:
            # Esquema con business_id: un lote padre por (fecha, business_id)
            cursor.execute("""
                SELECT id FROM Lote_sv WHERE fecha_lote = %s AND business_id = %s LIMIT 1
            """, (fecha_lote, business_id))
            lote_existente = cursor.fetchone()
            if lote_existente:
                logger.info(f"ℹ️ Lote_sv padre ya existe para fecha {fecha_lote} y business_id {business_id} (ID: {lote_existente['id']})")
                return lote_existente['id']
            cursor.execute("""
                INSERT INTO Lote_sv (fecha_lote, business_id, estado)
                VALUES (%s, %s, 'pendiente')
            """, (fecha_lote, business_id))
        else:
            # Esquema antiguo sin business_id: un lote padre por fecha (comportamiento legacy)
            cursor.execute("""
                SELECT id FROM Lote_sv WHERE fecha_lote = %s LIMIT 1
            """, (fecha_lote,))
            lote_existente = cursor.fetchone()
            if lote_existente:
                logger.info(f"ℹ️ Lote_sv padre ya existe para fecha {fecha_lote} (ID: {lote_existente['id']})")
                return lote_existente['id']
            cursor.execute("""
                INSERT INTO Lote_sv (fecha_lote, estado)
                VALUES (%s, 'pendiente')
            """, (fecha_lote,))

        conn.commit()
        lote_id = cursor.lastrowid
        logger.info(f"✅ Lote_sv padre creado para fecha {fecha_lote}" + (f" y business_id {business_id}" if tiene_business_id else "") + f" (ID: {lote_id})")
        return lote_id

    except Exception as e:
        logger.error(f"❌ Error obteniendo/creando Lote_sv padre: {e}")
        return None


def crear_lotes_por_business_id(cursor, conn, logger):
    """
    Agrupa registros de LiquidacionesSV por business_id y fecha_lote,
    crea registros en Lote_sv_business y actualiza LiquidacionesSV con lote_id
    """
    try:
        # Verificar que exista la columna lote_id
        if not verificar_y_agregar_columna_lote_id(cursor, conn, logger):
            return False, 0
        
        # Verificar que exista la tabla Lote_sv_business
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_NAME = 'Lote_sv_business'
        """)
        
        table_exists = cursor.fetchone()
        
        if not table_exists:
            logger.error("❌ La tabla Lote_sv_business no existe. Por favor créala primero.")
            return False, 0
        
        # Obtener registros de LiquidacionesSV que tienen business_id pero no tienen lote_id asignado
        cursor.execute("""
            SELECT 
                business_id,
                DATE(FECHA_TRAN) as fecha_lote,
                COUNT(*) as total_transacciones,
                COALESCE(SUM(MONTO_TRAN), 0) as total_monto_tran,
                COALESCE(SUM(MONTO_AJUS), 0) as total_monto_ajus,
                COALESCE(SUM(MONTO_TEXE), 0) as total_monto_texe,
                COALESCE(SUM(SUBTOTAL), 0) as total_subtotal,
                COALESCE(SUM(MONTO_IVA), 0) as total_monto_iva,
                COALESCE(SUM(COMISIONAB), 0) as total_comisionab,
                COALESCE(SUM(COM_MONTO), 0) as total_com_monto,
                COALESCE(SUM(COM_MTOIVA), 0) as total_com_mtoiva,
                COALESCE(SUM(RETENCION2), 0) as total_retencion2,
                COALESCE(SUM(RETENIDO), 0) as total_retenido,
                COALESCE(SUM(MONTO_DEBI), 0) as total_monto_debi,
                COALESCE(SUM(DEPOSITO), 0) as total_deposito,
                AVG(IVA_PORC) as iva_porc
            FROM LiquidacionesSV
            WHERE business_id IS NOT NULL
            AND lote_id IS NULL
            GROUP BY business_id, DATE(FECHA_TRAN)
        """)
        
        grupos = cursor.fetchall()
        
        if not grupos:
            logger.info("ℹ️ No hay registros para agrupar en lotes (todos ya tienen lote_id o no tienen business_id)")
            return True, 0
        
        logger.info(f"📊 Se encontraron {len(grupos)} grupos de business_id para crear lotes")
        
        lotes_creados = 0
        
        for grupo in grupos:
            business_id = grupo['business_id']
            fecha_lote = grupo['fecha_lote']
            
            # Asegurar que fecha_lote sea un objeto date
            if isinstance(fecha_lote, str):
                fecha_lote = datetime.strptime(fecha_lote, '%Y-%m-%d').date()
            elif isinstance(fecha_lote, datetime):
                fecha_lote = fecha_lote.date()
            
            # Obtener o crear lote padre (con business_id para cumplir NOT NULL en Lote_sv)
            lote_sv_id = obtener_o_crear_lote_sv_padre(cursor, conn, fecha_lote, business_id, logger)
            
            if not lote_sv_id:
                logger.error(f"❌ No se pudo obtener/crear Lote_sv padre para fecha {fecha_lote} y business_id {business_id}")
                continue
            
            # Verificar si ya existe un lote para este business_id y fecha
            cursor.execute("""
                SELECT id FROM Lote_sv_business 
                WHERE business_id = %s AND fecha_lote = %s AND lote_sv_id = %s
                LIMIT 1
            """, (business_id, fecha_lote, lote_sv_id))
            
            lote_existente = cursor.fetchone()
            
            if lote_existente:
                logger.info(f"ℹ️ Lote ya existe para business_id {business_id} y fecha {fecha_lote} (ID: {lote_existente['id']})")
                lote_business_id = lote_existente['id']
            else:
                # Crear nuevo registro en Lote_sv_business
                cursor.execute("""
                    INSERT INTO Lote_sv_business (
                        business_id, lote_sv_id, fecha_lote,
                        total_transacciones, total_monto_tran, total_monto_ajus,
                        total_monto_texe, total_subtotal, total_monto_iva,
                        total_comisionab, total_com_monto, total_com_mtoiva,
                        total_retencion2, total_retenido, total_monto_debi,
                        total_deposito, iva_porc, estado
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pendiente'
                    )
                """, (
                    business_id, lote_sv_id, fecha_lote,
                    int(grupo['total_transacciones']),
                    Decimal(str(grupo['total_monto_tran'] or 0)),
                    Decimal(str(grupo['total_monto_ajus'] or 0)),
                    Decimal(str(grupo['total_monto_texe'] or 0)),
                    Decimal(str(grupo['total_subtotal'] or 0)),
                    Decimal(str(grupo['total_monto_iva'] or 0)),
                    Decimal(str(grupo['total_comisionab'] or 0)),
                    Decimal(str(grupo['total_com_monto'] or 0)),
                    Decimal(str(grupo['total_com_mtoiva'] or 0)),
                    Decimal(str(grupo['total_retencion2'] or 0)),
                    Decimal(str(grupo['total_retenido'] or 0)),
                    Decimal(str(grupo['total_monto_debi'] or 0)),
                    Decimal(str(grupo['total_deposito'] or 0)),
                    Decimal(str(grupo['iva_porc'])) if grupo['iva_porc'] is not None else None
                ))
                conn.commit()
                lote_business_id = cursor.lastrowid
                logger.info(f"✅ Lote creado para business_id {business_id} y fecha {fecha_lote} (ID: {lote_business_id})")
                lotes_creados += 1
            
            # Actualizar LiquidacionesSV con el lote_id
            cursor.execute("""
                UPDATE LiquidacionesSV
                SET lote_id = %s
                WHERE business_id = %s
                AND DATE(FECHA_TRAN) = %s
                AND lote_id IS NULL
            """, (lote_business_id, business_id, fecha_lote))
            
            registros_actualizados = cursor.rowcount
            conn.commit()
            
            if registros_actualizados > 0:
                logger.info(f"✅ Actualizados {registros_actualizados} registros en LiquidacionesSV con lote_id {lote_business_id}")
        
        logger.info(f"📊 Total de lotes creados/actualizados: {lotes_creados}")
        
        # Actualizar los totales del Lote_sv padre sumando todos sus hijos (Lote_sv_business)
        logger.info("🔄 Actualizando totales del Lote_sv padre con sumas de todos los hijos...")
        actualizar_totales_lote_sv_padre(cursor, conn, logger)
        
        return True, lotes_creados
        
    except Exception as e:
        logger.error(f"❌ Error creando lotes por business_id: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False, 0


def actualizar_totales_lote_sv_padre(cursor, conn, logger):
    """
    Actualiza los totales del Lote_sv padre sumando todos los registros de Lote_sv_business
    que pertenecen a cada lote padre
    """
    try:
        # Obtener todos los lotes padre que necesitan actualización
        cursor.execute("""
            SELECT DISTINCT lote_sv_id 
            FROM Lote_sv_business
        """)
        
        lote_sv_ids = cursor.fetchall()
        
        if not lote_sv_ids:
            logger.info("ℹ️ No hay lotes padre para actualizar")
            return
        
        logger.info(f"📊 Actualizando {len(lote_sv_ids)} lotes padre...")
        
        for lote_sv_row in lote_sv_ids:
            lote_sv_id = lote_sv_row['lote_sv_id']
            
            # Sumar todos los totales de los hijos (Lote_sv_business) para este lote padre
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT business_id) as total_comercios,
                    SUM(total_transacciones) as total_transacciones,
                    SUM(total_monto_tran) as total_monto_tran,
                    SUM(total_monto_ajus) as total_monto_ajus,
                    SUM(total_monto_texe) as total_monto_texe,
                    SUM(total_subtotal) as total_subtotal,
                    SUM(total_monto_iva) as total_monto_iva,
                    SUM(total_comisionab) as total_comisionab,
                    SUM(total_com_monto) as total_com_monto,
                    SUM(total_com_mtoiva) as total_com_mtoiva,
                    SUM(total_retencion2) as total_retencion2,
                    SUM(total_retenido) as total_retenido,
                    SUM(total_monto_debi) as total_monto_debi,
                    SUM(total_deposito) as total_deposito,
                    AVG(iva_porc) as iva_porc
                FROM Lote_sv_business
                WHERE lote_sv_id = %s
            """, (lote_sv_id,))
            
            totales = cursor.fetchone()
            
            if totales:
                # Verificar qué columnas tiene la tabla Lote_sv
                cursor.execute("""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = 'Lote_sv' 
                    AND TABLE_SCHEMA = DATABASE()
                """)
                
                columnas_lote_sv = [row['COLUMN_NAME'] for row in cursor.fetchall()]
                
                # Construir la consulta UPDATE dinámicamente según las columnas disponibles
                campos_update = []
                valores_update = []
                
                if 'total_comercios' in columnas_lote_sv:
                    campos_update.append("total_comercios = %s")
                    valores_update.append(int(totales['total_comercios'] or 0))
                
                if 'total_transacciones' in columnas_lote_sv:
                    campos_update.append("total_transacciones = %s")
                    valores_update.append(int(totales['total_transacciones'] or 0))
                
                if 'total_monto_tran' in columnas_lote_sv:
                    campos_update.append("total_monto_tran = %s")
                    valores_update.append(Decimal(str(totales['total_monto_tran'] or 0)))
                
                if 'total_monto_ajus' in columnas_lote_sv:
                    campos_update.append("total_monto_ajus = %s")
                    valores_update.append(Decimal(str(totales['total_monto_ajus'] or 0)))
                
                if 'total_monto_texe' in columnas_lote_sv:
                    campos_update.append("total_monto_texe = %s")
                    valores_update.append(Decimal(str(totales['total_monto_texe'] or 0)))
                
                if 'total_subtotal' in columnas_lote_sv:
                    campos_update.append("total_subtotal = %s")
                    valores_update.append(Decimal(str(totales['total_subtotal'] or 0)))
                
                if 'total_monto_iva' in columnas_lote_sv:
                    campos_update.append("total_monto_iva = %s")
                    valores_update.append(Decimal(str(totales['total_monto_iva'] or 0)))
                
                if 'total_comisionab' in columnas_lote_sv:
                    campos_update.append("total_comisionab = %s")
                    valores_update.append(Decimal(str(totales['total_comisionab'] or 0)))
                
                if 'total_com_monto' in columnas_lote_sv:
                    campos_update.append("total_com_monto = %s")
                    valores_update.append(Decimal(str(totales['total_com_monto'] or 0)))
                
                if 'total_com_mtoiva' in columnas_lote_sv:
                    campos_update.append("total_com_mtoiva = %s")
                    valores_update.append(Decimal(str(totales['total_com_mtoiva'] or 0)))
                
                if 'total_retencion2' in columnas_lote_sv:
                    campos_update.append("total_retencion2 = %s")
                    valores_update.append(Decimal(str(totales['total_retencion2'] or 0)))
                
                if 'total_retenido' in columnas_lote_sv:
                    campos_update.append("total_retenido = %s")
                    valores_update.append(Decimal(str(totales['total_retenido'] or 0)))
                
                if 'total_monto_debi' in columnas_lote_sv:
                    campos_update.append("total_monto_debi = %s")
                    valores_update.append(Decimal(str(totales['total_monto_debi'] or 0)))
                
                if 'total_monto_deposito' in columnas_lote_sv:
                    campos_update.append("total_monto_deposito = %s")
                    valores_update.append(Decimal(str(totales['total_deposito'] or 0)))
                elif 'total_deposito' in columnas_lote_sv:
                    campos_update.append("total_deposito = %s")
                    valores_update.append(Decimal(str(totales['total_deposito'] or 0)))
                
                if 'iva_porc' in columnas_lote_sv and totales['iva_porc'] is not None:
                    campos_update.append("iva_porc = %s")
                    valores_update.append(Decimal(str(totales['iva_porc'])))
                
                valores_update.append(lote_sv_id)
                
                if campos_update:
                    update_sql = f"""
                        UPDATE Lote_sv 
                        SET {', '.join(campos_update)}
                        WHERE id = %s
                    """
                    cursor.execute(update_sql, tuple(valores_update))
                    conn.commit()
                    logger.info(f"✅ Lote_sv padre (ID: {lote_sv_id}) actualizado con totales de {totales['total_comercios']} comercios y {totales['total_transacciones']} transacciones")
                else:
                    logger.warning(f"⚠️ No se encontraron columnas de totales en Lote_sv para actualizar")
        
    except Exception as e:
        logger.error(f"❌ Error actualizando totales del Lote_sv padre: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
