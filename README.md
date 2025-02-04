## Instructions for Running the Project


Clone the repository, you can use this command in your terminal: 
```bash
git clone https://github.com/mnuel1/tech_exam.git
```


### 1. Running `background.py`
To run `background.py`, use the following command in your terminal:
```bash
python -u "{filepath}>\backround\background.py"
```
To stop the `background.py`, simply use `Ctrl-C`.

### 2. Running `api.py`
To run `api.py`, use the following command in your terminal:
```bash
uvicorn api.api:app --reload
```
To stop the `api.py`, just press `Ctrl-C`.

### 3. Installing Dependencies
To install the necessary dependencies, refer to the `requirements.txt` file or run the following command in your terminal:
```bash
pip install -r requirements.txt
```

### 4. Testing the API
To test the `background.py`, run the `background.py` and you can use Postman to send a POST request to the `/upload` endpoint. This will automatically detect any changes and insert the records into MongoDB.

### 5. Logging
The project includes a `file_watcher.log` that displays the following types of messages:
- **Error**
- **Warning**
- **Info** (e.g., success messages)


### ADDITIONAL
You can check the db configs in `config > db.py` for the:
- **DB_USERNAME**
- **DB_PASSWORD**
- **DB_NAME**
- **COLLECTION_NAME**
- uri

- PS. I should have considered using environment variables for the security of the database credentials, but for this exam, I purposely didn't include them so that the database can be tested without running into errors.