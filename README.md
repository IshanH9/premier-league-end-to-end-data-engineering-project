# premier-league-end-to-end-data-engineering-project
In this project, we build an ETL (Extract, Transform, Load) pipeline using the Football-Data API on AWS. The pipeline retrieves Premier League match data, transforms it into a structured format, and loads it into AWS data stores for querying and analysis.

## Architecture

![ETL Architecture](https://raw.githubusercontent.com/IshanH9/premier-league-end-to-end-data-engineering-project/main/pl-ETL-project-ishan.png)

## 🛠️ Technology Used

- **Programming Language**: Python  
- **Cloud Platform**: AWS  
  - AWS Lambda  
  - Amazon S3  
  - Amazon CloudWatch  
  - AWS Glue (Crawler + Data Catalog)  
  - Amazon Athena  
- **Data Analytics**: SQL (via Athena)

---

## 📂 Dataset Used

We are using the **Premier League match data** from the [Football-Data.org API](https://www.football-data.org/). The dataset contains information such as:

- Match date, status, and season
- Home and away teams
- Full-time scores
- Matchday and competition details

### Sample Endpoint Used:
```
https://api.football-data.org/v4/competitions/PL/matches
```

To access the API, sign up at [football-data.org](https://www.football-data.org/) and use the provided token with the header:

```python
headers = { "X-Auth-Token": "YOUR_API_KEY" }
```

---

## 🧱 Data Tables Extracted

| Table       | Description                        |
|-------------|------------------------------------|
| Matches     | Match info (id, date, teams, etc.) |
| Teams       | Unique team info (home/away)       |
| Season      | Season start, end, id              |
| Score       | Full-time and half-time scores     |
| Referee     | Referee details per match          |

---

## 🔁 Automation

- **CloudWatch Event Rule** triggers data extraction Lambda function on a scheduled basis (daily).
- **S3 Object Put Trigger** automatically invokes transformation Lambda after raw data is saved.
- **Glue Crawler** runs on a schedule or on demand to update schema in the Data Catalog.

---

## 📊 Dashboarding (Optional)

You can integrate **Amazon QuickSight**, **Power BI**, or **Tableau** with Athena for visualizations like:
- Team standings over time
- Goals per matchday
- Home vs. away win analysis

---

