
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
>python3 main.py