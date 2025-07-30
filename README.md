# sql-DWH-project
This is a guided, hands-on project from DataWithBaraa designed to build a modern data warehouse using SSMS. The goal is to learn progressively throughout the process, covering key concepts like ETL, data modeling, and analytics step by step.

# 🏗️ Data Architecture
Medallion Architecture 
   - Bronze Layer - Raw data, full bulk load from the csv files into table, not transformation
   - Silver Layer - Full load from bronze into table. Data is tranformed here.
   - Gold Layer - No load. Using the tranformed data to create business-ready data views

# 🛠️ Specifications
- Resources and Tools Used:
   - Data Sources: CSV files
   - Environment: SQL Server Management Studio (SSMS)
   - Diagrams & Visuals: https://draw.io
   - Project Tracking & Notes: https://www.notion.so
   - Data Models: Star schema
   - Version Control: GitHub

# 📌 Key Learnings
  - I genuinely enjoyed working on this project — it helped me consolidate and apply the concepts I’ve been learning in a structured, hands-on way. One of the practical takeaways was the value of printing log messages during data loading, which made debugging and workflow tracking much clearer.

   This project also deepened my understanding of:
   - How to design and implement a Medallion Architecture (Bronze, Silver, Gold layers)
   - What to look for when performing data transformations and ensuring data quality
   - The importance of clear data modeling and schema design (like Star Schema) for analytics
   - Structuring a data pipeline from raw ingestion to business-ready reporting
   - Overall, this experience improved both my technical skills and the way I think about data architecture and workflow management.
     
# 👋 About Me
Hi, I'm Aleks — a Data Automation Specialist with a passion for all things data. I'm always looking for opportunities to broaden my knowledge and explore new tools, technologies, and concepts in the data space. This project is part of my ongoing journey to keep learning and growing in the field.
🔗 Feel free to connect with me on LinkedIn https://www.linkedin.com/in/aleksandra-petrova-a582351b2/
