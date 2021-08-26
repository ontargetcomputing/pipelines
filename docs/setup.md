# Setup

The instructions below will guide you through setting up a local development environment on a **MAC**.  

These instructions do not include installation of a IDE or editor.  I use [VS Code](https://code.visualstudio.com/), however, feel free to use whatever you are comfortable with and makes you the most efficient.

## Instructions

1. **Install GIT**
	
	You most likely already have GIT install on your machine but if you do not, follow the instructions [here](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git).
	 

1. **Install Python 3.8**
	
	Your machine probably does not have 3.8 installed as the version of python used by the system.  Even if it does, it is a good idea to work within a virtual environment.
	
	1. **Install [pyenv](https://github.com/pyenv/pyenv)**
	
		`$ brew install pyenv`
	
	2. **Update PATH in .bashrc or .zshrc**

		`$ echo 'PATH=$(pyenv root)/shims:$PATH' >> ~/.zshrc`

		or
		
		`$ echo 'PATH=$(pyenv root)/shims:$PATH' >> ~/.bashrc`
		
		
	3. **Refresh your shell**
		
		`$ source ~/.zshrc`
		
		or
		
		`$ source ~/.bashrc`
		
		
	4. **Install Python 3.8.0**

		`$ pyenv install 3.8.0`


	5. **Use Python 3.8.0**

		`$ pyenv global 3.8.0`


	5. **Confirm your python path**		
		
		```	
		$ which python
		/Users/my_username/.pyenv/shims/python
		```

At this point your machine should be setup for local development.  Please refer back to the main [README](../README.md) for details on doing development locally.
	
