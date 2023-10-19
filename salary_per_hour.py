import pandas as pd
from sqlalchemy import create_engine,text
from datetime import datetime, timedelta
import psycopg2

def main():
    # Define database connection
    database_url = "postgresql://username:password@database.sever.name.com:5432/database"

    # Extract employee and timesheets data
    employees = pd.read_csv('employees.csv')
    timesheets = pd.read_csv('timesheets.csv')
    
    transform(employees,timesheets,database_url)

def transform(employees,timesheets,database_url):
    try:
        # Clean and preprocess data
        timesheets['checkin'] = timesheets['checkin'].str.replace('"', '')
        timesheets['checkout'] = timesheets['checkout'].str.replace('"', '')

        # Assume resign_date today for no resign_date data
        employees['resign_date'].fillna(datetime.now().date(), inplace=True)

        # Remove rows with no checkin or checkout times
        timesheets = timesheets.dropna(subset=['checkin', 'checkout'])

        # Convert date columns to datetime
        timesheets['date'] = pd.to_datetime(timesheets['date'])
        employees['join_date'] = pd.to_datetime(employees['join_date'])
        employees['resign_date'] = pd.to_datetime(employees['resign_date'])

        # Filter timesheets within the employment date range
        merged_data = timesheets.merge(employees, left_on='employee_id', right_on='employe_id')

        # Create a database connection
        engine = create_engine(database_url)
        connection = engine.connect()

        # Load the last processed timesheet_id for incremental mode
        query = text("SELECT last_timesheet_id FROM last_processed_timesheet")
        last_processed_timesheet_id = connection.execute(query).scalar()
        print("last processed timesheet_id: ",last_processed_timesheet_id)
        engine.dispose()

        # Filter new data based on timesheet_id
        new_data = merged_data[merged_data['timesheet_id'] > last_processed_timesheet_id]

        # If there is new data, proceed with processing
        if not new_data.empty:
            # Extract year and month from the date
            new_data['year'] = new_data['date'].dt.year
            new_data['month'] = new_data['date'].dt.month

            # Calculate total work hours
            new_data['checkout'] = pd.to_datetime(new_data['checkout'], format='%H:%M:%S')
            new_data['checkin'] = pd.to_datetime(new_data['checkin'], format='%H:%M:%S')
            new_data['total_work_hours'] = (new_data['checkout'] - new_data['checkin']).dt.total_seconds() / 3600

            # Group and aggregate data 
            grouped_data = new_data.groupby(['branch_id', 'year', 'month']).agg(
                total_salary=pd.NamedAgg(column='salary', aggfunc='sum'),
                total_work_hours=pd.NamedAgg(column='total_work_hours', aggfunc='sum')
            ).reset_index()

            # Calculate salary per hour
            grouped_data['salary_per_hour'] = grouped_data['total_salary'] / grouped_data['total_work_hours']

            load(database_url,timesheets,grouped_data)

        else:
            print("No new data to process.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def load(database_url,timesheets,grouped_data):
        # Create a database connection
        engine = create_engine(database_url)

        # Append data to the destination table
        destination_table='salary_per_hour_python' 
        grouped_data[['year', 'month', 'branch_id', 'salary_per_hour']].to_sql(destination_table, con=engine, if_exists='append', index=False)
        print(f"Success update {destination_table}")
        engine.dispose()
        try:
            conn = psycopg2.connect(
            host="host_name",
            database="database",
            user="username",
            password="password"
            )

            cursor = conn.cursor()
            # Store the new last processed timesheet_id for incremental mode
            new_last_processed_timesheet_id = timesheets['timesheet_id'].max()
            update_query = f"UPDATE last_processed_timesheet SET last_timesheet_id = '{new_last_processed_timesheet_id}'"
            cursor.execute(update_query)
            conn.commit()
            print("Update last processed timesheet_id success")

        except (Exception, psycopg2.Error) as error:
            print("Error updating:", error)

        finally:
            if conn:
                cursor.close()
                conn.close()

if __name__ == "__main__":
    main()
