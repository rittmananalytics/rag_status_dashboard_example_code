WITH
  current_clients AS (
    SELECT
      e.company_fk AS company_pk,
      c.company_name,
      e.deal_name,
      e.deal_id,
      e.deal_description,
      string_agg(projects.project_name) project_name,
      e.engagement_start_ts,
      e.engagement_end_ts,
      ed.background,
      ed.requirements, 
      ed.solution, 
      ed.objectives_json, 
      ed.deliverables_json,
      -- Generate a sequence of months for each engagement
      GENERATE_DATE_ARRAY(
        DATE_TRUNC(DATE(e.engagement_start_ts),MONTH),
        CASE WHEN DATE_TRUNC(DATE(e.engagement_end_ts),MONTH) > current_date() THEN DATE_TRUNC(CURRENT_DATE,MONTH) ELSE DATE_TRUNC(DATE(e.engagement_end_ts),MONTH) END,
        INTERVAL 1 MONTH
      ) AS engagement_months
    FROM
      `ra-development`.`analytics`.`timesheet_project_engagements_dim` e      
    LEFT JOIN
      UNNEST(projects) projects
    LEFT JOIN
       `ra-development`.`analytics`.`companies_dim` c ON e.company_fk = c.company_pk
    LEFT JOIN  
       `ra-development`.`analytics_integration`.`int_engagement_details` ed ON e.deal_id = ed.deal_id 
    WHERE 
        DATE(e.engagement_start_ts) >= DATE('2022-01-01')
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      e.engagement_start_ts,
      e.engagement_end_ts,
      ed.background,
      ed.requirements, 
      ed.solution, 
      ed.objectives_json, 
      ed.deliverables_json
  ),
  monthly_clients AS (
    SELECT
      company_pk,
      company_name,
      deal_name,
      deal_description,
      project_name,
      engagement_start_ts,
      engagement_end_ts,
      engagement_month,
      background, 
      requirements, 
      solution, 
      objectives_json, 
      deliverables_json
    FROM
      current_clients
    LEFT JOIN
      UNNEST(engagement_months) AS engagement_month
  ),
  slack_messages AS (
    SELECT
      mc.company_name,
      mc.engagement_month,
      CONCAT(
        '[',
        STRING_AGG(
          TO_JSON_STRING(
            STRUCT(
              FORMAT_TIMESTAMP('%F %T', messages_fact.message_ts) AS messages_fact_message_time,
              message_contacts.contact_name AS message_contacts_contact_name,
              messages_fact.channel_name AS messages_fact_channel_name,
              messages_fact.message_text AS messages_fact_message_text
            )
          )
          ORDER BY
            FORMAT_TIMESTAMP('%F %T', messages_fact.message_ts) DESC
        ),
        ']'
      ) AS all_messages
    FROM
      `ra-development`.`analytics`.`companies_dim` AS companies_dim
    INNER JOIN
      monthly_clients mc ON companies_dim.company_pk = mc.company_pk
    LEFT JOIN
      `ra-development`.`analytics`.`messages_fact` AS messages_fact ON companies_dim.company_pk = messages_fact.company_fk
    LEFT JOIN
      `ra-development`.`analytics`.`contacts_dim` AS message_contacts ON messages_fact.contact_fk = message_contacts.contact_pk
    WHERE
      messages_fact.message_ts >= TIMESTAMP(DATETIME(TIMESTAMP_TRUNC(mc.engagement_month, MONTH)))
      AND messages_fact.message_ts < TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(mc.engagement_month, MONTH)), INTERVAL 1 MONTH))
    GROUP BY
      1, 2
  ),
  confluence_pages as (
    SELECT
      d.company_name,
      mc.engagement_month,
      CONCAT(
            '[',
            STRING_AGG(
              TO_JSON_STRING(
                STRUCT(
      CONCAT('"',page_title,'" published on ', page_created_at_ts,' : ', page_summary) 
      )
              )
              ORDER BY
                page_created_at_ts 
            ),
            ']'
          ) AS all_docs_published
    FROM
      `ra-development`.`analytics`.`delivery_project_docs_dim` p
    LEFT JOIN
      `ra-development.analytics.companies_dim` d
    ON
      p.company_fk = d.company_pk
    INNER JOIN
      monthly_clients mc ON d.company_pk = mc.company_pk
    WHERE
      page_title is not null 
      and page_summary is not null
      and DATE_TRUNC(date(page_created_at_ts),MONTH) = mc.engagement_month
    GROUP BY 1,2
  ),
  meeting_summaries AS (
    SELECT
      mc.company_name,
      mc.engagement_month,
      FORMAT_TIMESTAMP('%F %T', customer_meetings.meeting_start_ts) AS meeting_start_time,
      customer_meetings.meeting_title AS meeting_title,
      customer_meetings.meeting_summary AS meeting_summary,
      AVG(customer_meetings.meeting_engagement_level) AS avg_meeting_engagement_level,
      AVG(
        (
          CASE
            WHEN (
              trim(customer_meetings.contribution_sentiment_category)
            ) IN ('UNENGAGED', 'CONCERNED') THEN -1
            WHEN (
              trim(customer_meetings.contribution_sentiment_category)
            ) = 'POSITIVE' THEN 1
            ELSE 0
          END
        )
      ) AS average_contribution_sentiment_score
    FROM
      `ra-development`.`analytics`.`companies_dim` AS companies_dim
    INNER JOIN
      monthly_clients mc ON companies_dim.company_pk = mc.company_pk
    LEFT JOIN
      (
        SELECT
          *
        FROM
          `ra-development`.`analytics`.`meeting_contact_contributions_fact`          
      ) customer_meetings ON companies_dim.company_pk = customer_meetings.company_fk
    WHERE
      customer_meetings.meeting_start_ts >= TIMESTAMP(DATETIME(TIMESTAMP_TRUNC(mc.engagement_month, MONTH)))
      AND customer_meetings.meeting_start_ts < TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(mc.engagement_month, MONTH)), INTERVAL 1 MONTH))
    GROUP BY
      1,
      2,
      3,
      4,
      5
  ),
  meeting_summaries_agg AS (
    SELECT
      company_name,
      engagement_month,
      CONCAT(
        '[',
        STRING_AGG(
          TO_JSON_STRING(
            STRUCT(
              meeting_start_time,
              meeting_title,
              meeting_summary,
              avg_meeting_engagement_level,
              average_contribution_sentiment_score
            )
          )
          ORDER BY
            meeting_start_time DESC
        ),
        ']'
      ) AS meeting_summaries
    FROM
      meeting_summaries
    GROUP BY
      1,2
  ),
  client_team_meeting_commments AS (
    WITH
      customer_meetings AS (
        SELECT
          *
        FROM
          `ra-development`.`analytics`.`meeting_contact_contributions_fact`  
      )
    SELECT
      mc.company_name,
      mc.engagement_month,
      CONCAT(
        '[',
        STRING_AGG(
          TO_JSON_STRING(
            STRUCT(
              companies_dim.company_name AS company_name,
              FORMAT_TIMESTAMP('%F %T', customer_meetings.meeting_start_ts) AS customer_meetings_meeting_start_time,
              customer_meetings.meeting_title AS meeting_title,
              customer_meetings.meeting_summary AS meeting_summary,
              customer_meetings.contact_name AS team_member_name,
              customer_meetings.meeting_contribution AS meeting_comments,
              trim(customer_meetings.contribution_sentiment_category) AS customer_meetings_contribution_sentiment_category
            )
          )
          ORDER BY
            customer_meetings.meeting_start_ts DESC
        ),
        ']'
      ) AS meeting_comments
    FROM
      `ra-development`.`analytics`.`companies_dim` AS companies_dim
    INNER JOIN
      monthly_clients mc ON companies_dim.company_pk = mc.company_pk
    LEFT JOIN
      customer_meetings ON companies_dim.company_pk = customer_meetings.company_fk
   WHERE
      customer_meetings.meeting_start_ts >= TIMESTAMP(DATETIME(TIMESTAMP_TRUNC(mc.engagement_month, MONTH)))
      AND customer_meetings.meeting_start_ts < TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(mc.engagement_month, MONTH)), INTERVAL 1 MONTH))
    GROUP BY
      1, 2
  ),
  tasks AS (
    SELECT
      *,
      MIN(task_start_ts) OVER (
        PARTITION BY
          delivery_project_fk,
          epic_name
      ) AS epic_start_ts,
      MAX(task_end_ts) OVER (
        PARTITION BY
          delivery_project_fk,
          epic_name
      ) AS epic_end_ts
    FROM
      `ra-development`.`analytics`.`delivery_tasks_fact`
  ),
  delivery_tasks AS (
    SELECT
      mc.company_name AS company_name,
       mc.engagement_month,
      CONCAT(
        '[',
        STRING_AGG(
          TO_JSON_STRING(
            STRUCT(
              DATE(tasks.task_start_ts) AS task_start_date,
              DATE(tasks.task_completed_ts) AS task_completed_date,
              tasks.task_name AS task_name,
              tasks.task_status AS task_status,
              tasks.task_story_points AS task_story_points,
              CASE
                WHEN task_status != 'Done' THEN timestamp_diff(current_timestamp, tasks.task_start_ts, DAY)
              END AS task_days_open,
              tasks.sprint_name AS sprint_name,
              tasks.epic_name AS epic_name
            )
          )
          ORDER BY
            tasks.task_start_ts DESC
        ),
        ']'
      ) AS all_tasks
    FROM
      `ra-development`.`analytics`.`companies_dim` AS companies_dim
    LEFT JOIN
      `ra-development`.`analytics`.`delivery_projects_dim` AS projects_managed ON companies_dim.company_pk = projects_managed.company_fk
    LEFT JOIN
      tasks ON projects_managed.delivery_project_pk = tasks.delivery_project_fk
     INNER JOIN
      monthly_clients mc ON companies_dim.company_pk = mc.company_pk
    WHERE
      (tasks.is_latest_task_version)
       AND tasks.task_start_ts <= TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(mc.engagement_month, MONTH)), INTERVAL 1 MONTH))
          AND  coalesce(tasks.task_completed_ts,timestamp(mc.engagement_month)) >= timestamp(mc.engagement_month)
     
      AND tasks.task_status != 'To Do'
    GROUP BY
      1,2
  )
  ,
    project_financials as (SELECT
        t8.`__f0` AS company_name,
        DATE(TIMESTAMP_TRUNC(TIMESTAMP(concat(t8.`__f4`,'-01')), MONTH)) as engagement_month,
        COALESCE(SUM(CASE WHEN CASE WHEN t8.groupingVal IN (0, 1, 2, 3) THEN 0 ELSE 32 END + CASE WHEN t8.groupingVal IN (0, 1, 2, 3) THEN 0 ELSE 16 END + CASE WHEN t8.groupingVal IN (0, 1, 2, 3) THEN 0 ELSE 8 END + CASE WHEN t8.groupingVal = 0 THEN 0 ELSE 4 END + CASE WHEN t8.groupingVal = 1 THEN 0 ELSE 2 END + CASE WHEN t8.groupingVal = 2 THEN 0 ELSE 1 END = 5 AND t8.`__f8` > 0 THEN t8.`__f7` ELSE NULL END), 0) AS project_sprints_hours_budget,
        COALESCE(SUM(CASE WHEN CASE WHEN t8.groupingVal IN (0, 1, 2, 3) THEN 0 ELSE 32 END + CASE WHEN t8.groupingVal IN (0, 1, 2, 3) THEN 0 ELSE 16 END + CASE WHEN t8.groupingVal IN (0, 1, 2, 3) THEN 0 ELSE 8 END + CASE WHEN t8.groupingVal = 0 THEN 0 ELSE 4 END + CASE WHEN t8.groupingVal = 1 THEN 0 ELSE 2 END + CASE WHEN t8.groupingVal = 2 THEN 0 ELSE 1 END = 5 AND t8.`__f8` > 0 THEN t8.`__f9` ELSE NULL END), 0) AS total_project_sprints_fee_amount,
        CAST(MIN(CASE WHEN CASE WHEN t8.groupingVal IN (0, 1, 2, 3) THEN 0 ELSE 32 END + CASE WHEN t8.groupingVal IN (0, 1, 2, 3) THEN 0 ELSE 16 END + CASE WHEN t8.groupingVal IN (0, 1, 2, 3) THEN 0 ELSE 8 END + CASE WHEN t8.groupingVal = 0 THEN 0 ELSE 4 END + CASE WHEN t8.groupingVal = 1 THEN 0 ELSE 2 END + CASE WHEN t8.groupingVal = 2 THEN 0 ELSE 1 END = 7 THEN t8.`__f10` ELSE NULL END) AS FLOAT64) AS total_billable_hours_billed,
        CAST(MIN(CASE WHEN CASE WHEN t8.groupingVal IN (0, 1, 2, 3) THEN 0 ELSE 32 END + CASE WHEN t8.groupingVal IN (0, 1, 2, 3) THEN 0 ELSE 16 END + CASE WHEN t8.groupingVal IN (0, 1, 2, 3) THEN 0 ELSE 8 END + CASE WHEN t8.groupingVal = 0 THEN 0 ELSE 4 END + CASE WHEN t8.groupingVal = 1 THEN 0 ELSE 2 END + CASE WHEN t8.groupingVal = 2 THEN 0 ELSE 1 END = 7 THEN t8.`__f11` ELSE NULL END) AS FLOAT64) AS total_nonbillable_hours_billed,
        CAST(MIN(CASE WHEN CASE WHEN t8.groupingVal IN (0, 1, 2, 3) THEN 0 ELSE 32 END + CASE WHEN t8.groupingVal IN (0, 1, 2, 3) THEN 0 ELSE 16 END + CASE WHEN t8.groupingVal IN (0, 1, 2, 3) THEN 0 ELSE 8 END + CASE WHEN t8.groupingVal = 0 THEN 0 ELSE 4 END + CASE WHEN t8.groupingVal = 1 THEN 0 ELSE 2 END + CASE WHEN t8.groupingVal = 2 THEN 0 ELSE 1 END = 7 THEN t8.`__f12` ELSE NULL END) AS FLOAT64) AS total_timesheet_cost_amount_gbp,
        COALESCE(SUM(CASE WHEN CASE WHEN t8.groupingVal IN (0, 1, 2, 3) THEN 0 ELSE 32 END + CASE WHEN t8.groupingVal IN (0, 1, 2, 3) THEN 0 ELSE 16 END + CASE WHEN t8.groupingVal IN (0, 1, 2, 3) THEN 0 ELSE 8 END + CASE WHEN t8.groupingVal = 0 THEN 0 ELSE 4 END + CASE WHEN t8.groupingVal = 1 THEN 0 ELSE 2 END + CASE WHEN t8.groupingVal = 2 THEN 0 ELSE 1 END = 6 AND t8.`__f14` > 0 THEN t8.`__f13` ELSE NULL END), 0) AS total_contractor_cost_amount_gbp,
        COUNT(CASE WHEN CASE WHEN t8.groupingVal IN (0, 1, 2, 3) THEN 0 ELSE 32 END + CASE WHEN t8.groupingVal IN (0, 1, 2, 3) THEN 0 ELSE 16 END + CASE WHEN t8.groupingVal IN (0, 1, 2, 3) THEN 0 ELSE 8 END + CASE WHEN t8.groupingVal = 0 THEN 0 ELSE 4 END + CASE WHEN t8.groupingVal = 1 THEN 0 ELSE 2 END + CASE WHEN t8.groupingVal = 2 THEN 0 ELSE 1 END = 3 THEN t8.`__f15` ELSE NULL END) AS count_of_project_sprints
    FROM
        (SELECT
                CASE WHEN t6.groupingVal IN (0, 1, 2, 3) THEN t1.companies_dim_company_name ELSE NULL END AS `__f0`,
                    CASE WHEN t6.groupingVal = 0 THEN t1.projects_delivered_project_code ELSE NULL END AS `__f3`,
                    CASE WHEN t6.groupingVal IN (0, 1, 2, 3) THEN t1.projects_delivered_project_delivery_end_ts_month ELSE NULL END AS `__f4`,
                    CASE WHEN t6.groupingVal IN (0, 1, 2, 3) THEN t1.projects_delivered_project_delivery_start_ts_month ELSE NULL END AS `__f5`,
                    CASE WHEN t6.groupingVal = 1 THEN t1.`__f20` ELSE NULL END AS `__f20`,
                    CASE WHEN t6.groupingVal = 2 THEN t1.`__f26` ELSE NULL END AS `__f26`,
                t6.groupingVal,
                MIN(CASE WHEN t1.`__f19` THEN t1.`__f18` ELSE NULL END) AS `__f7`,
                COUNT(CASE WHEN t1.`__f19` THEN 1 ELSE NULL END) AS `__f8`,
                MIN(CASE WHEN t1.`__f19` THEN t1.projects_delivered_project_fee_amount ELSE NULL END) AS `__f9`,
                COALESCE(SUM(t1.`__f21`), 0) AS `__f10`,
                COALESCE(SUM(t1.`__f22`), 0) AS `__f11`,
                COALESCE(SUM(t1.`__f23`), 0) AS `__f12`,
                MIN(CASE WHEN t1.`__f25` THEN t1.`__f24` ELSE NULL END) AS `__f13`,
                COUNT(CASE WHEN t1.`__f25` THEN 1 ELSE NULL END) AS `__f14`,
                MIN(t1.projects_delivered_project_code) AS `__f15`
            FROM
                (SELECT
                        companies_dim.company_name  AS companies_dim_company_name,
                        projects_delivered.project_code  AS projects_delivered_project_code,
                            (FORMAT_TIMESTAMP('%Y-%m', timestamp(projects_delivered.project_delivery_end_ts) )) AS projects_delivered_project_delivery_end_ts_month,
                            (FORMAT_TIMESTAMP('%Y-%m', timestamp(projects_delivered.project_delivery_start_ts) )) AS projects_delivered_project_delivery_start_ts_month,
                        projects_delivered.project_fee_amount  AS projects_delivered_project_fee_amount,
                        projects_delivered.project_budget_amount  AS `__f18`,
                            (projects_delivered.timesheet_project_pk ) IS NOT NULL AS `__f19`,
                        projects_delivered.timesheet_project_pk  AS `__f20`,
                            CASE WHEN project_invoice_timesheets.timesheet_is_billable  THEN coalesce(project_invoice_timesheets.timesheet_hours_billed,0)  ELSE NULL END AS `__f21`,
                            CASE WHEN NOT COALESCE( project_invoice_timesheets.timesheet_is_billable  , FALSE) THEN coalesce(project_invoice_timesheets.timesheet_hours_billed,0)  ELSE NULL END AS `__f22`,
                        coalesce(project_invoice_timesheets.timesheet_hours_billed * project_invoice_timesheets.timesheet_billable_hourly_cost_amount,0)  AS `__f23`,
                        coalesce(( timesheet_project_costs_fact.expense_amount_local / expenses_exchange_rates.CURRENCY_RATE ),0)  AS `__f24`,
                            (timesheet_project_costs_fact.expense_pk ) IS NOT NULL AS `__f25`,
                        timesheet_project_costs_fact.expense_pk  AS `__f26`
                    FROM `ra-development`.`analytics`.`companies_dim`  AS companies_dim
    LEFT JOIN `ra-development`.`analytics`.`timesheet_projects_dim`
        AS projects_delivered ON companies_dim.company_pk = projects_delivered.company_fk
    INNER JOIN
      monthly_clients mc ON companies_dim.company_pk = mc.company_pk
    LEFT JOIN `ra-development`.`analytics`.`timesheets_fact`
        AS project_invoice_timesheets ON projects_delivered.timesheet_project_pk = project_invoice_timesheets.timesheet_project_fk
    LEFT JOIN `ra-development`.`analytics`.`timesheet_project_costs_fact`
        AS timesheet_project_costs_fact ON projects_delivered.timesheet_project_pk = timesheet_project_costs_fact.timesheet_project_fk
    LEFT JOIN `ra-development.analytics_seed.exchange_rates`
        AS expenses_exchange_rates ON timesheet_project_costs_fact.expense_currency_code = expenses_exchange_rates.CURRENCY_CODE
                    WHERE
                        TIMESTAMP(projects_delivered.project_delivery_end_ts) >= TIMESTAMP(DATETIME(TIMESTAMP_TRUNC(mc.engagement_month, MONTH)))
                        AND TIMESTAMP(projects_delivered.project_delivery_end_ts) < TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(mc.engagement_month, MONTH)), INTERVAL 1 MONTH))
                    ) AS t1,
                    (SELECT
                            0 AS groupingVal
                        UNION ALL
                        SELECT
                            1 AS groupingVal
                        UNION ALL
                        SELECT
                            2 AS groupingVal
                        UNION ALL
                        SELECT
                            3 AS groupingVal) AS t6
            GROUP BY
                1,
                2,
                3,
                4,
                5,
                6,
                7) AS t8
    GROUP BY
        1,
        2),
    project_financials_running_sum AS (
      SELECT
        company_name, engagement_month,sum(project_sprints_hours_budget) over (order by engagement_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as project_sprints_hours_budget,
        sum(total_project_sprints_fee_amount) over (order by engagement_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as total_project_sprints_fee_amount,
        sum(total_billable_hours_billed) over (order by engagement_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as total_billable_hours_billed,
        sum(total_nonbillable_hours_billed) over (order by engagement_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as total_nonbillable_hours_billed,
        sum(total_timesheet_cost_amount_gbp) over (order by engagement_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)  as total_timesheet_cost_amount_gbp,
        sum(total_contractor_cost_amount_gbp) over (order by engagement_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as total_contractor_cost_amount_gbp,
        sum(count_of_project_sprints) over (order by engagement_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as count_of_project_sprints
      FROM
    project_financials
    ),
    project_financial_metrics AS (
    SELECT 
        company_name,
        engagement_month,
        CONCAT(
        '[',
        STRING_AGG(
          TO_JSON_STRING(
            STRUCT(
                CONCAT(ROUND(100*(SAFE_DIVIDE((total_billable_hours_billed+total_nonbillable_hours_billed),project_sprints_hours_budget))),'%')  AS project_sprints_hours_vs_budget_pct,
                CONCAT(ROUND(100*(SAFE_DIVIDE(total_nonbillable_hours_billed,(total_billable_hours_billed+total_nonbillable_hours_billed)))),'%') AS nonbillable_hours_pct,
                CONCAT(ROUND(100*(SAFE_DIVIDE(total_project_sprints_fee_amount-(total_timesheet_cost_amount_gbp+total_contractor_cost_amount_gbp),total_project_sprints_fee_amount))),'%') AS project_sprints_margin_pct,
                CONCAT('£',ROUND(SAFE_DIVIDE(total_project_sprints_fee_amount,(total_billable_hours_billed+total_nonbillable_hours_billed)))) AS project_sprints_hourly_rate,
                CONCAT('£',ROUND(total_project_sprints_fee_amount)) AS total_project_sprints_fee_amount,
                CONCAT('£',ROUND(total_project_sprints_fee_amount-(total_timesheet_cost_amount_gbp+total_contractor_cost_amount_gbp))) AS total_project_sprints_profit_amount,
                CONCAT('£',ROUND(total_contractor_cost_amount_gbp)) AS total_contractor_resource_cost_gbp
            )
          )          
        ),
        ']'
      ) AS all_project_financial_metrics        
    FROM 
        project_financials_running_sum
    GROUP BY
      1,2)  
  ,
  overall_rag_status AS (
    SELECT
      company_name AS client_name,
      deal_name AS engagement_name,
      deal_description AS engagement_description,
      project_name AS sprint_name,
      engagement_month AS reporting_month,
      JSON_VALUE(
        REPLACE(
          REPLACE(
            REPLACE(ml_generate_text_llm_result, '```json', ''),
            '```',
            ''
          ),
          '\n',
          ''
        ),
        '$.overall_status'
      ) overall_rag_status,
      JSON_VALUE(
        REPLACE(
          REPLACE(
            REPLACE(ml_generate_text_llm_result, '```json', ''),
            '```',
            ''
          ),
          '\n',
          ''
        ),
        '$.overall_rationale'
      ) AS overall_rag_status_reason,
      JSON_VALUE(
        REPLACE(
          REPLACE(
            REPLACE(ml_generate_text_llm_result, '```json', ''),
            '```',
            ''
          ),
          '\n',
          ''
        ),
        '$.financials_status'
      ) financials_status,
      JSON_VALUE(
        REPLACE(
          REPLACE(
            REPLACE(ml_generate_text_llm_result, '```json', ''),
            '```',
            ''
          ),
          '\n',
          ''
        ),
        '$.financials_rationale'
      ) AS financials_status_rationale,
      JSON_VALUE(
        REPLACE(
          REPLACE(
            REPLACE(ml_generate_text_llm_result, '```json', ''),
            '```',
            ''
          ),
          '\n',
          ''
        ),
        '$.schedule_status'
      ) schedule_status,
      JSON_VALUE(
        REPLACE(
          REPLACE(
            REPLACE(ml_generate_text_llm_result, '```json', ''),
            '```',
            ''
          ),
          '\n',
          ''
        ),
        '$.schedule_rationale'
      ) AS schedule_status_rationale,      
      JSON_VALUE(
        REPLACE(
          REPLACE(
            REPLACE(ml_generate_text_llm_result, '```json', ''),
            '```',
            ''
          ),
          '\n',
          ''
        ),
        '$.scope_status'
      ) scope_status,
      JSON_VALUE(
        REPLACE(
          REPLACE(
            REPLACE(ml_generate_text_llm_result, '```json', ''),
            '```',
            ''
          ),
          '\n',
          ''
        ),
        '$.scope_rationale'
      ) AS scope_status_rationale,
      JSON_VALUE(
        REPLACE(
          REPLACE(
            REPLACE(ml_generate_text_llm_result, '```json', ''),
            '```',
            ''
          ),
          '\n',
          ''
        ),
        '$.technology_status'
      ) technology_status,
      JSON_VALUE(
        REPLACE(
          REPLACE(
            REPLACE(ml_generate_text_llm_result, '```json', ''),
            '```',
            ''
          ),
          '\n',
          ''
        ),
        '$.technology_rationale'
      ) AS technology_rationale,
      JSON_VALUE(
        REPLACE(
          REPLACE(
            REPLACE(ml_generate_text_llm_result, '```json', ''),
            '```',
            ''
          ),
          '\n',
          ''
        ),
        '$.resourcing_status'
      ) resourcing_status,
      JSON_VALUE(
        REPLACE(
          REPLACE(
            REPLACE(ml_generate_text_llm_result, '```json', ''),
            '```',
            ''
          ),
          '\n',
          ''
        ),
        '$.resourcing_rationale'
      ) AS resourcing_rationale,
      JSON_VALUE(
        REPLACE(
          REPLACE(
            REPLACE(ml_generate_text_llm_result, '```json', ''),
            '```',
            ''
          ),
          '\n',
          ''
        ),
        '$.data_quality_qa_status'
      ) data_quality_qa_status,
      JSON_VALUE(
        REPLACE(
          REPLACE(
            REPLACE(ml_generate_text_llm_result, '```json', ''),
            '```',
            ''
          ),
          '\n',
          ''
        ),
        '$.data_quality_qa_rationale'
      ) AS data_quality_qa_rationale,
      ARRAY(
          SELECT STRUCT(
              JSON_VALUE(event, '$.event_date') AS event_date,
              JSON_VALUE(event, '$.event_description') AS event_description
          )
          FROM UNNEST(JSON_EXTRACT_ARRAY(JSON_QUERY(REPLACE(REPLACE(REPLACE(ml_generate_text_llm_result, '```json', ''), '```', ''), '\n', ''), '$.month_timeline'))) AS event
          ORDER BY JSON_VALUE(event, '$.event_date') ASC
      ) AS month_timeline_events      
    FROM
      ML.GENERATE_TEXT(
        MODEL `analytics_ai.gemini_1_5_flash`,
        (
          SELECT
            mc.company_name,
            mc.deal_name,
            mc.deal_description,
            mc.project_name,
            mc.engagement_month,            
            CONCAT(
              'Provide a RAG status report for a Rittman Analytics client project in the following JSON format, where each field is filled with a brief, clear, and specific explanation of the project status in the context of the engagement background and requirements, the set of business objectives and contractual deliverables, the planned overall solution and the project documents published, meeting transcripts, slack messages and other project output and evidence. The possible statuses are RED, AMBER, or GREEN and the rationales should succinctly explain why each status was assigned (citing evidence), taking care to consider whether the engagement and project are for Discovery, Planning or Design and where the calculation of RAG status should relate to Rittman Analytics scope, financials, resourcing, schedule, technology OR the engagement is a project implementation where we are then responsible for the clients current technology, data quality, project scope, resourcing and schedule and for the project financials). Expected financial performance is a project margin of 70%+, project sprint hours vs budget <=100% and an hourly rate <=£140, ideally £180+. In addition, provide a timeline of the significant (not all) activities (excluding daily standups and exchanges of pleasantries) and outputs of the engagement this month to accompany the RAG statuses. Use the format {\"overall_status\": \"<RED|AMBER|GREEN>\", \"overall_rationale\": \"<Brief explanation of the overall project status>\", \"scope_status\": \"<RED|AMBER|GREEN>\", \"scope_rationale\": \"<Brief explanation of the scope status>\", \"financials_status\": \"<RED|AMBER|GREEN>\", \"financials_rationale\": \"<Brief explanation of the financial status>\", \"schedule_status\": \"<RED|AMBER|GREEN>\", \"schedule_rationale\": \"<Brief explanation of the schedule status>\", \"data_quality_qa_status\": \"<RED|AMBER|GREEN>\", \"data_quality_qa_rationale\": \"<Brief explanation of the data quality & QA status>\", \"technology_status\": \"<RED|AMBER|GREEN>\", \"technology_rationale\": \"<Brief explanation of the technology status>\", \"resourcing_status\": \"<RED|AMBER|GREEN>\", \"resourcing_rationale\": \"<Brief explanation of the resourcing status>\", \"month_timeline\": \"<json_array_of_events_each_event_listing_event_date_and_event_description_sorted_in_date_order\"} Please use consistent, structured language for the rationales, ensuring they are concise (but specific, detailed) and actionable., for this company: ',
              mc.company_name,
              ' for an engagement called "',COALESCE(mc.deal_name, ''),
              '" with a description of "',COALESCE(mc.deal_description, ''),
              '" with background to the engagement of "',COALESCE(mc.background, ''),
              '" a set of client requirements given as "',COALESCE(mc.requirements, ''),
              '" and business objectives defined by the client as "',COALESCE(mc.objectives_json, ''),
              '", a solution proposed by Rittman Analytics to deliver "',COALESCE(mc.solution, ''),
              '" with contractual deliverables due by engagement completion agreed as "',COALESCE(mc.deliverables_json, ''),
              '" and a current sprint(s) called "',
              COALESCE(mc.project_name, ''),
              '" with these resulting project financial metrics : ',
              COALESCE(p.all_project_financial_metrics, ''),
              '" that worked on delivering these project tasks : ',
              COALESCE(j.all_tasks, ''),
              ' published and posted these Confluence pages: ',
              COALESCE(cp.all_docs_published, ''),
              ' generated these slack messaging posts: ',
              COALESCE(s.all_messages, ''),
              ', and led to these meeting summaries ',
              COALESCE(ms.meeting_summaries, ''),
              ' and these team-member feedback items from those meetings ',
              COALESCE(t.meeting_comments, ''),
              '. Remember to consider, if giving a RED or AMBER RAG status for a discovery engagement, whether the issues causing this red or amber status are affecting our ability to deliver the project or not.'
            ) AS prompt
          FROM
            monthly_clients mc
          LEFT JOIN
            meeting_summaries_agg ms ON mc.company_name = ms.company_name AND mc.engagement_month = ms.engagement_month
          LEFT JOIN
            slack_messages s ON mc.company_name = s.company_name AND mc.engagement_month = s.engagement_month
          LEFT JOIN 
            confluence_pages cp ON mc.company_name = cp.company_name AND mc.engagement_month = cp.engagement_month
          LEFT JOIN
            client_team_meeting_commments t ON mc.company_name = t.company_name AND mc.engagement_month = t.engagement_month
          LEFT JOIN
            delivery_tasks j ON mc.company_name = j.company_name AND mc.engagement_month = j.engagement_month
          LEFT JOIN  
            project_financial_metrics p ON mc.company_name = p.company_name and mc.engagement_month = p.engagement_month 
          WHERE 
            mc.background is not null

        ),
        STRUCT(
          0.2 AS temperature,
          4096 AS max_output_tokens,
          TRUE AS flatten_json_output
        )
      )
  )
SELECT
  *
FROM
  overall_rag_status
WHERE 
  overall_rag_status.overall_rag_status IS NOT NULL
order by 
    client_name,engagement_name,reporting_month


    );
  
