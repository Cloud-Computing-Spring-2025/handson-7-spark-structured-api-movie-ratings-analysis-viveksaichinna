# Movie Ratings Analysis Report

## **Project Overview**
This project analyzes **movie ratings and streaming trends** using Apache Spark Structured APIs. The analysis is divided into three tasks:
1. **Binge Watching Patterns** - Identifies binge-watchers across different age groups.
2. **Churn Risk Users** - Determines users at risk of churning based on low engagement.
3. **Movie Watching Trends** - Examines how movie consumption trends change over the years.

## **Project Structure**
```
MovieRatingsAnalysis/
├── input/
│   └── movie_ratings_data.csv
├── outputs/
│   ├── binge_watching_patterns.csv
│   ├── churn_risk_users.csv
│   └── movie_watching_trends.csv
├── src/
│   ├── task1_binge_watching_patterns.py
│   ├── task2_churn_risk_users.py
│   ├── task3_movie_watching_trends.py
│   └── generate_dataset.py
├── docker-compose.yml
└── README.md
```

## **Setup Instructions**
### 1️⃣ **Install Required Dependencies**
```bash
pip install pyspark
```

### 2️⃣ **Generate Dataset**
Before running the analysis, generate the dataset using:
```bash
python3 src/generate_dataset.py
```
Verify dataset creation:
```bash
ls -l input/
cat input/movie_ratings_data.csv | head -5
```

### 3️⃣ **Run Analysis Tasks**
#### **Task 1: Binge Watching Patterns**
```bash
spark-submit src/task1_binge_watching_patterns.py
```
Expected Output Location:
```bash
ls outputs/
cat outputs/binge_watching_patterns.csv
```

#### **Task 2: Churn Risk Users**
```bash
spark-submit src/task2_churn_risk_users.py
```
Expected Output Location:
```bash
ls outputs/
cat outputs/churn_risk_users.csv
```

#### **Task 3: Movie Watching Trends**
```bash
spark-submit src/task3_movie_watching_trends.py
```
Expected Output Location:
```bash
ls outputs/
cat outputs/movie_watching_trends.csv
```

## **Summary of Findings**
### 📌 **Binge Watching Patterns**
- Identified binge-watchers across different **age groups**.
- Calculated **percentage of binge-watchers** per age category.

### 📌 **Churn Risk Users**
- Filtered users with **canceled subscriptions** and **low watch time (<100 minutes)**.
- Computed total **churn-risk users**.

### 📌 **Movie Watching Trends**
- Analyzed movie consumption trends **year over year**.
- Found **peak years** for movie watching.

## **Troubleshooting**
- **Dataset is empty?** Run:
  ```bash
  python3 src/generate_dataset.py
  ```
- **Files not in `outputs/`?** Check:
  ```bash
  ls outputs/
  ```
- **Spark errors?** Try:
  ```bash
  spark-submit --version
  ```

## **Final Verification**
Ensure all expected files are created:
```bash
ls outputs/
```
Expected Output:
```
binge_watching_patterns.csv
churn_risk_users.csv
movie_watching_trends.csv
```