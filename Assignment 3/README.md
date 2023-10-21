## Project setup and configuration

### docker setup
```bash
docker pull mongo
docker run -d -p 27017:27017 --name storedist mongo
```

### python setup
```Bash
#install python 3.12.0 if not already installed
brew install pyenv
pyenv install 3.12.0
pyenv global 3.12.0
# Activate the virtual environment for python3.12
python3.12 -m venv myenv
source myenv/bin/activate  

python -m pip install pymongo
```

### setup the database

when docker is running
```Bash
python setup_db.py
```


## Running the application

```Bash
. myenv/bin/activate.fish
python queries.py
``` 