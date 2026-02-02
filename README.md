# Deber1DM
Estudiante: Esteban Silva
Curso: Data Mining
Proyecto: Ingesta histórica desde QuickBooks Online (QBO)
Orquestación: Mage
Base de datos: PostgreSQL
Despliegue: Docker Compose
Alcance: Backfill histórico hacia esquema raw

Este proyecto implementa un pipeline de backfill histórico que extrae información desde QuickBooks Online (QBO) y la deposita en PostgreSQL dentro de un esquema raw.
El pipeline fue diseñado para:
Autenticarse mediante OAuth2 con la API de QBO.
Extraer datos mediante queries SQL-like.
Manejar paginación automática.
Implementar reintentos con backoff exponencial ante errores transitorios.
Persistir los datos en tablas RAW con idempotencia (ON CONFLICT).
Actualmente el repositorio contiene:
Pipeline funcional para Customers.
Arquitectura preparada para extenderse a Invoices e Items.
Estructura del proyecto:

├── docker-compose.yml
├── mage/
│   ├── data_loaders/
│   ├── data_exporters/
│   ├── pipelines/
│   └── utils/
├── sql/
│   └── create_raw_tables.sql
└── README.md

Pipeline implementado
qb_customers_backfill
Pipeline de backfill histórico para la entidad Customers.
Bloques:
Data Loader: extracción desde QuickBooks Online.
Transformer: 
Data Exporter: upsert en PostgreSQL dentro del esquema raw

Parámetros del trigger
Parámetro	Descripción
fecha_inicio	Inicio del rango histórico
fecha_fin	Fin del rango
chunk_days (opcional)	Segmentación temporal

Reintentos
Máximo 5 intentos por request.
Backoff exponencial (2^n segundos + jitter).
Errores manejados: 429, 500, 502, 503, 504.
