Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
    - Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
    - Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
    - Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
    - Find [dbt events](https://events.getdbt.com) near you
    - Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


Project Overview:
    This project is an end-to-end retail analytics data pipeline built using dbt and Databricks. It follows the Medallion Architecture (Bronze, Silver, Gold) to transform raw data into clean, reliable, and analytics-ready datasets.

    The pipeline processes sales, customers, products, stores, and returns data to generate business KPIs and reporting marts.



Architecture:
    - Bronze Layer:
    Raw ingested data from source systems.

    - Silver Layer:
    Cleaned, validated, and standardized data models.

    - Gold Layer:
    Aggregated business metrics and reporting marts (daily & monthly sales).

Key Features:
    - Data cleaning and validation (handling nulls, formatting, casting)
    - Derived metrics (discount %, high discount flags, promotion indicators)
    - Sales and returns analysis
    - Daily and monthly aggregation marts
    - KPI calculations:
    - Total revenue
    - Net revenue after returns
    - Average transaction value
    - Promotion attach rate
    - Return rate
    - Payment method analysis


Tech Stack:
    - dbt (data transformation)
    - Databricks (data platform)
    - SQL (analytics & modeling)
    - Git (version control)

Project Goal:
    To build a scalable and production-ready data pipeline that transforms raw retail data into actionable insights for analytics and decision-making.