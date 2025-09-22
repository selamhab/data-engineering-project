# data-engineering-project
Data engineering projects including ETL pipelines, data processing, and database management using Python, SQL, and APIs

This project demonstrates an end-to-end ETL (Extract, Transform, Load) pipeline using Python to collect, process, and store financial data on the world’s largest banks. The pipeline extracts data from a Wikipedia page, transforms it with currency conversion based on exchange rates, and loads the results into both a CSV file and an SQLite database. This project showcases skills in data engineering, including web scraping, data cleaning, transformation, database management, and automation using Python libraries.

---

## Technologies & Tools Used 

- Python 3.x 
- Pandas (data manipulation) 
- Requests & BeautifulSoup (web scraping) 
- SQLite3 (database management) 
- CSV file handling 
- Git & GitHub for version control and repository management 

---

## Project Structure 

- banks_project.py — Main ETL script that runs the pipeline 
- exchange_rate.csv — CSV file containing currency exchange rates for transformations 
- Largest_banks_data.csv — Output CSV with transformed data 
- Banks.db — SQLite database storing the largest banks data 
- code_log.txt — Log file tracking the ETL process progress 

---

## How It Works

1. **Extract:** Scrapes the list of largest banks and their market caps from a Wikipedia page archive.
2. **Transform:** Cleans the data, removes unwanted characters, converts market cap values to multiple currencies using exchange rates from exchange_rate.csv. 
3. **Load:** Saves the transformed data to a CSV file and loads it into an SQLite database table. 
4. **Query:** Runs example SQL queries to display the stored data and aggregate statistics. 

---

## How to Run the Project 

1. Clone the repository:

    ```bash
    git clone https://github.com/selamhab/data-engineering-project.git
    cd data-engineering-project
    ```

2. Install the required Python libraries:

    ```bash
    pip install pandas requests beautifulsoup4
    ```

3. Ensure `exchange_rate.csv` is present in the same folder.

4. Run the ETL script:

    ```bash
    python banks_project.py
    ```

---

## Author

**Selam Habte**  
[GitHub Profile](https://github.com/selamhab)

---
