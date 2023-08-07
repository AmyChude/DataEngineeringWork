# Setting up a python virtual environment 

## Concept 

Let’s assume you have two projects, with different python library version requirements. 

![images/venv1.png](images/venv1.png)

Which library version should we install on our computer? 

If we install Project 1’s requirements, then Project 2 will not work on our computer, and vice-versa. 

To solve this, we create standalone python environments for each project, with the specific library versions installed. 

![images/venv2.png](images/venv2.png)

## Setting it up 

To set up a python virtual environment using conda, you will first need to have conda installed. If you don't have conda installed, please [anaconda installation](https://docs.anaconda.com/anaconda/install/).

Steps: 

1. Open terminal or command prompt. 

2. Notice the `(base)` conda environment has already been activated. 

3. Create a new conda environment 

```
conda create -n <your_conda_env_name> python=3.9
```

- `-n` flag: is used to specify the name of your conda environment. 
- `<your_conda_env_name>`: is to be replaced with the name of your conda environment e.g. `py-etl`.
- `python=3.9`: is used to specify the version of python to be installed for this conda environment. 

4. Activate the new conda environment 

```
conda activate <your_conda_env_name>
```

Notice the conda environment has switched to your environment e.g. `(py-etl)`. 

5. Install required libraries for this week's lesson in your conda environment. 

In `2-evr-setting-up-virtual-env/solved`, you will find a `requirements.txt` file with a list of libraries that we are going to need. To install these libraries, run: 

```
cd ~/2-evr-setting-up-virtual-env/solved # change directory to where the requirements.txt is stored 
pip install -r requirements.txt # install the libraries in the file 
```

6. Check that the libraries have been installed. 

```
pip list # prints out all the libraries installed in your activated environment. 
```


# Working with the repository 

Here are some helpful commands when working with the repository. 

```bash 
# checkout to a new branch 
git checkout -b <new_branch_name>
# save your changes in the new branch 
git add . 
git commit -m "changes"
# checkout back to the main branch 
git checkout main 
# pull new code from the main branch 
git pull 
```
