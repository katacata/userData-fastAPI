
Activate virtual environment
>cd app
> 
>python3 -m venv env
>
>source env/bin/activate
 
Install dependencies
>pip3 install pipreqs
>
>python3 -m  pipreqs.pipreqs .
> 
>pip3 install -r requirements.txt

Start the API server
>pip3 install uvicorn
> 
>uvicorn main:app --reload

For mysql
>pip install mysqlclient
