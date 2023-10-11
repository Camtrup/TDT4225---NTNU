## Project setup and configuration

Before we begin, we'll create a virtual Python environment to isolate the project from the rest of the globally-installed Python packages. We'll use the venv package, which comes with your Python installation. Execute the following command from the terminal:

```Bash
#install python 3.12.0 if not already installed
brew install pyenv
pyenv install 3.12.0
pyenv global 3.12.0
# Activate the virtual environment for python3.12
python3.12 -m venv myenv
source myenv/bin/activate  

python -m pip install 'pymongo[srv]' python-dotenv
```

Send a message to mattis for getting a mongodb user and password. 

.env file:
```.env
ATLAS_URI=mongodb+srv://<username>:<password>@cluster1.lqlql.mongodb.net/?retryWrites=true&w=majority
DB_NAME=stroedist
``````