python -m venv env

env\Scripts\activate

pip install -r requirements.txt

uvicorn main:app --reload


pip freeze > requirements.txt