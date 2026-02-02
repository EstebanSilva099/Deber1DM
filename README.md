# Deber1DM - QuickBooks Backfill Pipeline

Estudiante: Esteban Silva  
Curso: Data Mining  
Proyecto: Ingesta histórica desde QuickBooks Online (QBO)  
Orquestación: Mage  
Base de datos: PostgreSQL  
Despliegue: Docker Compose  
Alcance: Backfill histórico hacia esquema raw

---

## Descripción general

Este proyecto implementa un pipeline de backfill histórico que extrae información desde QuickBooks Online (QBO) y la deposita en PostgreSQL dentro de un esquema raw.

El pipeline fue diseñado para:

- Autenticarse mediante OAuth2 con la API de QBO.
- Extraer datos mediante queries SQL-like.
- Manejar paginación automática.
- Implementar reintentos con backoff exponencial ante errores transitorios.
- Persistir los datos en tablas RAW con idempotencia mediante ON CONFLICT.

Actualmente el repositorio contiene un pipeline funcional para Customers y una arquitectura preparada para extenderse a Invoices e Items.

---

## Arquitectura general

QuickBooks Online API  
|  
v  
Mage Loader  
|  
v  
Mage Exporter  
|  
v  
PostgreSQL (schema raw)

---

## Estructura del proyecto

.
docker-compose.yml  
mage/  
- data_loaders/  
- data_exporters/  
- pipelines/  
- utils/  
sql/  
- create_raw_tables.sql  
README.md

---

## Cómo levantar el proyecto

1. Clonar repositorio

git clone https://github.com/EstebanSilva099/Deber1DM.git  
cd Deber1DM  

2. Detener contenedores previos

docker-compose down -v  
docker system prune -f  
docker volume prune -f  

3. Levantar contenedores

docker-compose up -d --build  

Se recomienda abrir los servicios desde Docker Desktop y verificar que Mage y PostgreSQL estén activos.

---

## Gestión de secretos

Las credenciales se gestionan mediante el Secret Manager de Mage.

Se utilizan los siguientes secretos:

QB_CLIENT_ID  
QB_CLIENT_SECRET  
QB_REALM_ID  
QB_REFRESH_TOKEN  

Nunca se deben subir valores reales al repositorio.

---

## Pipeline implementado

qb_customers_backfill

Pipeline de backfill histórico para la entidad Customers.

Bloques:

- Data Loader: extracción desde QuickBooks Online.
- Data Exporter: upsert en PostgreSQL dentro del esquema raw.

---

## Parámetros del trigger

fecha_inicio: inicio del rango histórico  
fecha_fin: fin del rango  
chunk_days: segmentación temporal opcional

---

## Segmentación y control de volumen

Se implementa paginación utilizando startposition y maxresults hasta agotar los registros.

Se manejan reintentos automáticos con un máximo de cinco intentos por request y backoff exponencial de 2^n segundos más jitter.

Errores manejados: 429, 500, 502, 503 y 504.

---

## Esquema raw

Tabla utilizada: raw.qb_customers

Columnas:

id  
payload  
ingested_at_utc  
extract_window_start_utc  
extract_window_end_utc  
page_number  
page_size  
request_payload  

---

## Validaciones y volumetría

Para validar la ejecución:

- Revisar los logs del loader.
- Confirmar el total de filas insertadas.
- Consultar la tabla en PostgreSQL.

Ejemplo:

SELECT COUNT(*) FROM raw.qb_customers;

Si el conteo es mayor a cero, la ingesta fue exitosa.

---

## Troubleshooting

Autenticación:

Verificar que los secretos estén correctamente configurados en Mage.  
Confirmar que el refresh token esté vigente.  
Revisar que el realm_id corresponda al entorno correcto.

Rate limits:

QuickBooks impone límites de peticiones por minuto y día.  
Se utiliza backoff exponencial y reintentos automáticos.

Timezones:

Todos los timestamps se transforman a UTC para consistencia.

## Checklist de aceptación

- [x] Mage y Postgres se comunican por nombre de servicio.
- [x] Todos los secretos (QBO y Postgres) están en Mage Secrets; no hay secretos en el repo/entorno expuesto.
- [~] Pipelines qb_<entidad>_backfill acepta fecha_inicio y fecha_fin (UTC) y segmenta el rango (implementado completamente para Customers).
- [~] Trigger one-time configurado, ejecutado y luego deshabilitado/marcado como completado (ejecutado manualmente, evidencia parcial).
- [x] Esquema raw con tablas por entidad, payload completo y metadatos obligatorios (Customers).
- [~] Idempotencia verificada: reejecución de un tramo no genera duplicados (ON CONFLICT implementado, evidencia parcial).
- [x] Paginación y rate limits manejados y documentados.
- [~] Volumetría y validaciones mínimas registradas y archivadas como evidencia (logs revisados, capturas no archivadas).
- [ ] Runbook de reanudación y reintentos disponible y seguido (no formalizado completamente).




