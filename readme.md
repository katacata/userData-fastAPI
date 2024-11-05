Required
> Python 3.13.0
> 
> pip 24.2

Activate virtual environment
>cd app
> 
>python3 -m venv env
>
>source env/bin/activate
 
Install dependencies
>pip install -r requirements.txt

Start the API server
>pip install uvicorn
> 
>uvicorn main:app --reload

For mysql
>pip install mysqlclient

Just start
>make all