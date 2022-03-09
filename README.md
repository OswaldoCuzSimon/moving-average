

## Getting Started
________

### Installing
Using virtualenv
~~~
virtualenv -p /usr/bin/python3.6 env
source env/bin/activate
pip install -r requirements.txt
~~~
Set environment variables
Rename [dotenv](dotenv) file to .env and replace the example values for real ones<br>
`cp dotenv .env`

### execution
Show instructions to run
~~~
python rolling_operations.py --help
~~~
Run the script for local file
~~~
python rolling_operations.py --input_file_location historical_stock_prices.csv.gz --output_file_location GOOGL --ticker GOOGL
~~~

Run the script for file in s3
~~~
python rolling_operations.py --input_file_location s3a://kueski-challenge-data-engineer/historical_stock_prices.csv.gz --output_file_location kueski-challenge-data-engineer/GOOGL --ticker GOOGL
~~~