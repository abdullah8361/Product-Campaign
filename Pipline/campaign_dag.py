from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator

default_args = {
    'start_date': days_ago(1),
}

with DAG('campaign_effectiveness', default_args=default_args, schedule_interval=None, catchup=False) as dag:

    start = DummyOperator(task_id='start')

    with TaskGroup('task_group_1') as task_group_1:
        query_1 = BigQueryExecuteQueryOperator(
            task_id='query_1',
            sql="""
            WITH CustomerInteractions AS (
                SELECT 
                    ci.customer_id,
                    ci.campaign_id,
                    ci.interaction_type,
                    ci.interaction_timestamp,
                    ci.device_type,
                    ci.source
                FROM 
                    CustomerInteraction ci
            ),
            CampaignDetails AS (
                SELECT 
                    c.campaign_id,
                    c.channel,
                    c.campaign_start,
                    c.campaign_end,
                    c.budget,
                    c.target_audience,
                    c.campaign_type
                FROM 
                    Campaign c
            ),
            Conversions AS (
                SELECT 
                    o.order_id,
                    o.customer_id,
                    o.product_id,
                    o.campaign_id,
                    o.conversion_timestamp,
                    o.order_amount
                FROM 
                    Orders o
            )
            SELECT 
                ci.customer_id,
                ci.interaction_type,
                ci.interaction_timestamp,
                ci.device_type,
                ci.source,
                cd.channel,
                cd.campaign_start,
                cd.campaign_end,
                cd.budget,
                cd.target_audience,
                cd.campaign_type,
                co.order_id,
                co.product_id,
                co.conversion_timestamp,
                co.order_amount
            FROM 
                CustomerInteractions ci
            JOIN 
                CampaignDetails cd ON ci.campaign_id = cd.campaign_id
            LEFT JOIN 
                Conversions co ON ci.customer_id = co.customer_id AND ci.campaign_id = co.campaign_id;
            """,
            use_legacy_sql=False,
            destination_dataset_table='project.dataset.table1',
            write_disposition='WRITE_TRUNCATE'
        )

        quality_check_1 = BigQueryExecuteQueryOperator(
            task_id='quality_check_1',
            sql="SELECT COUNT(*) FROM `project.dataset.table1` WHERE interaction_type IS NULL OR channel IS NULL;",
            use_legacy_sql=False
        )

        query_1 >> quality_check_1

    with TaskGroup('task_group_2') as task_group_2:
        query_2 = BigQueryExecuteQueryOperator(
            task_id='query_2',
            sql="""
            WITH InteractionCounts AS (
                SELECT 
                    campaign_id,
                    SUM(CASE WHEN interaction_type = 'click' THEN 1 ELSE 0 END) AS total_clicks,
                    SUM(CASE WHEN interaction_type = 'impression' THEN 1 ELSE 0 END) AS total_impressions
                FROM 
                    joined_campaign_interaction_table
                GROUP BY 
                    campaign_id
            ),
            CTRCalculation AS (
                SELECT 
                    campaign_id,
                    total_clicks,
                    total_impressions,
                    CASE 
                        WHEN total_impressions = 0 THEN 0
                        ELSE (total_clicks::FLOAT / total_impressions) * 100
                    END AS ctr
                FROM 
                    InteractionCounts
            )
            SELECT 
                campaign_id,
                total_clicks,
                total_impressions,
                ctr
            FROM 
                CTRCalculation;
            """,
            use_legacy_sql=False,
            destination_dataset_table='project.dataset.table2',
            write_disposition='WRITE_TRUNCATE'
        )

        quality_check_2 = BigQueryExecuteQueryOperator(
            task_id='quality_check_2',
            sql="SELECT COUNT(*) FROM `project.dataset.table2` WHERE total_clicks < 0 OR total_impressions < 0;",
            use_legacy_sql=False
        )

        query_2 >> quality_check_2

    with TaskGroup('task_group_3') as task_group_3:
        query_3 = BigQueryExecuteQueryOperator(
            task_id='query_3',
            sql="""
            WITH interaction_counts AS (
                SELECT
                    campaign_id,
                    SUM(CASE WHEN interaction_type = 'click' THEN 1 ELSE 0 END) AS total_clicks,
                    SUM(CASE WHEN interaction_type = 'conversion' THEN 1 ELSE 0 END) AS total_conversions
                FROM
                    joined_campaign_interaction_table
                GROUP BY
                    campaign_id
            ),
            conversion_rates AS (
                SELECT
                    campaign_id,
                    total_conversions,
                    total_clicks,
                    CASE WHEN total_clicks > 0 THEN (total_conversions::FLOAT / total_clicks) * 100 ELSE 0 END AS conversion_rate
                FROM
                    interaction_counts
            )
            SELECT
                campaign_id,
                total_conversions,
                total_clicks,
                conversion_rate
            FROM
                conversion_rates;
            """,
            use_legacy_sql=False,
            destination_dataset_table='project.dataset.table3',
            write_disposition='WRITE_TRUNCATE'
        )

        quality_check_3 = BigQueryExecuteQueryOperator(
            task_id='quality_check_3',
            sql="SELECT COUNT(*) FROM `project.dataset.table3` WHERE total_conversions < 0 OR total_clicks < 0;",
            use_legacy_sql=False
        )

        query_3 >> quality_check_3

    start >> task_group_1 >> task_group_2 >> task_group_3