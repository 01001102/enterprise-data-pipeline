# Enterprise Data Pipeline

Pipeline de dados enterprise-grade usando **AWS Kinesis + Apache Airflow + DBT** para processamento em tempo real e analytics avançadas.

## 🏗️ Arquitetura

```
Kinesis Streams → Bronze Layer → Silver Layer → Gold Layer → Analytics
     (Raw)         (Airflow)     (Airflow)      (DBT)      (Grafana)
```

### Camadas de Dados

- **Bronze (Raw)**: Dados brutos do Kinesis → PostgreSQL
- **Silver (Clean)**: Dados limpos e enriquecidos via Airflow
- **Gold (Business)**: Métricas de negócio e analytics via DBT

## 🚀 Características Enterprise

- **Real-time Ingestion**: Kinesis Streams para dados em tempo real
- **Orquestração Robusta**: Airflow com retry, monitoring e alertas
- **Data Quality**: Validações automáticas em cada camada
- **Analytics Engineering**: DBT para transformações SQL complexas
- **Observabilidade**: Grafana + métricas customizadas
- **Escalabilidade**: Arquitetura distribuída e containerizada

## 🛠️ Stack Tecnológica

### Ingestão de Dados
- **AWS Kinesis**: Streaming de dados em tempo real
- **LocalStack**: Simulação AWS local para desenvolvimento
- **Python**: Produtores de dados enterprise

### Orquestração
- **Apache Airflow**: Workflow orchestration
- **PostgreSQL**: Data warehouse e metastore
- **Redis**: Celery backend para Airflow

### Transformação
- **DBT**: Analytics engineering e data modeling
- **SQL**: Transformações complexas e business logic
- **Jinja2**: Templates dinâmicos no DBT

### Monitoramento
- **Grafana**: Dashboards e visualizações
- **Airflow UI**: Monitoring de pipelines
- **DBT Docs**: Documentação automática

## 📊 Dados Simulados

### Streams Kinesis
1. **customer-events**: Cadastros, logins, atualizações
2. **transaction-events**: Transações financeiras completas
3. **product-events**: Interações com produtos
4. **system-logs**: Logs de aplicações e serviços

### Métricas Calculadas
- **Customer Lifetime Value (LTV)**
- **Risk Scoring** com ML features
- **RFM Analysis** (Recency, Frequency, Monetary)
- **Cohort Analysis** e retenção
- **Revenue Attribution** por canal/categoria

## 🚀 Como Executar

### 1. Preparar Ambiente
```bash
# Criar diretórios necessários
mkdir -p airflow/{dags,logs,plugins}
echo -e "AIRFLOW_UID=$(id -u)" > .env

# Subir infraestrutura
docker-compose up -d

# Aguardar inicialização (3-5 minutos)
docker-compose logs -f airflow-init
```

### 2. Configurar Conexões Airflow
```bash
# Acessar Airflow UI: http://localhost:8080 (airflow/airflow)
# Configurar conexão PostgreSQL:
# - Conn Id: postgres_default
# - Host: postgres
# - Schema: airflow
# - Login: airflow
# - Password: airflow
```

### 3. Iniciar Pipeline
```bash
# Terminal 1: Gerar dados Kinesis
cd kinesis
pip install boto3 faker
python data_producer.py

# Terminal 2: Ativar DAG no Airflow UI
# Ou via CLI:
docker exec airflow-scheduler airflow dags unpause enterprise_data_pipeline
```

### 4. Executar DBT
```bash
# Dentro do container Airflow
docker exec -it airflow-scheduler bash
cd /opt/airflow/dbt

# Instalar dependências e executar
dbt deps
dbt run
dbt test
dbt docs generate
```

## 📈 Modelos de Dados

### Bronze Layer
- `bronze_customer_events`: Eventos brutos de clientes
- `bronze_transaction_events`: Transações brutas
- `bronze_product_events`: Interações com produtos
- `bronze_system_logs`: Logs de sistema

### Silver Layer
- `silver_customers`: Clientes consolidados e enriquecidos
- `silver_transactions`: Transações limpas com categorização
- `silver_products`: Catálogo de produtos normalizado
- `silver_sessions`: Sessões de usuário agregadas

### Gold Layer
- `gold_customer_analytics`: Analytics 360° de clientes
- `gold_business_metrics`: KPIs e métricas de negócio
- `gold_cohort_analysis`: Análise de coortes
- `gold_revenue_attribution`: Atribuição de receita

## 🔍 Qualidade de Dados

### Validações Automáticas
- **Completude**: Campos obrigatórios preenchidos
- **Unicidade**: Chaves primárias únicas
- **Consistência**: Relacionamentos válidos
- **Freshness**: Dados atualizados nas últimas 2 horas
- **Volume**: Variação de volume dentro do esperado

### Testes DBT
```sql
-- Exemplo de teste customizado
SELECT COUNT(*) as failed_records
FROM {{ ref('silver_customers') }}
WHERE email NOT LIKE '%@%'
  OR total_spent < 0
  OR created_at > CURRENT_TIMESTAMP
```

## 📊 Dashboards e Métricas

### KPIs Principais
- **Revenue Growth**: Crescimento de receita MoM/YoY
- **Customer Acquisition Cost (CAC)**
- **Monthly Recurring Revenue (MRR)**
- **Churn Rate** por segmento
- **Average Order Value (AOV)**

### Dashboards Grafana
- **Executive Dashboard**: KPIs executivos
- **Operations Dashboard**: Métricas operacionais
- **Data Quality Dashboard**: Saúde dos dados
- **Pipeline Monitoring**: Status dos jobs

## 🔧 Configurações Avançadas

### Airflow
```python
# airflow.cfg customizations
[core]
executor = LocalExecutor
max_active_runs_per_dag = 1
dagbag_import_timeout = 30

[scheduler]
catchup_by_default = False
max_threads = 4
```

### DBT
```yaml
# dbt_project.yml
models:
  enterprise_dw:
    bronze:
      +materialized: table
      +pre-hook: "{{ log('Processing bronze layer', info=True) }}"
    silver:
      +materialized: table
      +post-hook: "{{ run_query('ANALYZE ' ~ this) }}"
    gold:
      +materialized: table
      +indexes:
        - columns: ['customer_id']
          unique: true
```

## 🧪 Testes e Validação

### Testes Unitários
```bash
# Testar modelos DBT
dbt test --models silver_customers
dbt test --models gold_customer_analytics

# Testar DAGs Airflow
python -m pytest tests/test_dags.py
```

### Testes de Integração
```bash
# Validar pipeline end-to-end
./scripts/integration_test.sh
```

## 🚀 Deploy e CI/CD

### Ambientes
- **DEV**: Desenvolvimento local com LocalStack
- **STAGING**: Ambiente de homologação
- **PROD**: Produção com AWS real

### Pipeline CI/CD
```yaml
# .github/workflows/deploy.yml
- name: Test DBT Models
  run: dbt test --profiles-dir ./profiles

- name: Deploy to Staging
  run: dbt run --target staging

- name: Run Data Quality Checks
  run: python scripts/data_quality_check.py
```

## 📚 Tecnologias Demonstradas

### Data Engineering
- **Stream Processing**: Kinesis real-time ingestion
- **Batch Processing**: Airflow orchestration
- **Data Modeling**: DBT dimensional modeling
- **Data Quality**: Automated testing and validation

### DevOps & MLOps
- **Containerization**: Docker multi-service setup
- **Infrastructure as Code**: Docker Compose
- **Monitoring**: Grafana + custom metrics
- **Documentation**: Auto-generated DBT docs

### Analytics Engineering
- **Dimensional Modeling**: Star schema implementation
- **Business Logic**: Complex SQL transformations
- **Performance Optimization**: Indexes and partitioning
- **Data Lineage**: DBT automatic lineage tracking

## 🔄 Próximos Passos

- [ ] Implementar Apache Iceberg para data lakehouse
- [ ] Adicionar Great Expectations para data quality
- [ ] Integrar com Apache Superset para self-service BI
- [ ] Implementar CDC (Change Data Capture)
- [ ] Adicionar machine learning features
- [ ] Configurar alertas inteligentes

---

**Desenvolvido por Ivan de França**

*Pipeline enterprise demonstrando as melhores práticas em Data Engineering, Analytics Engineering e DataOps.*