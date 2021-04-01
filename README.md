# PIPELINES

The [Pipelines](https://github.com/LambWestonIT/pipelines) project contains a set of reusable components used for developing [AWS Step Functions](https://aws.amazon.com/step-functions) to ingest and prepare data for load into Snowflake.

## Getting Started

### Prerequisites
* [GIT](https://git-scm.com/) - Source Control
* [Python 3.8](https://www.python.org/) - The orchestration services are executed in a Python runtime environment.

**Mac Users**  For Step By Step instructions on preparing your machine to do local development, please see the instructions [here](./docs/setup.md).

**Windows Users** Sorry, I have not setup a development environment here.  I'll get to this one day, although, **volunteers** to do setup and document are welcome.

## Local Development

### Setup

Please see the prerequisites in above if you have not already done so.

1. **Check out this repository**

	`$ git clone https://github.com/LambWestonIT/pipelines.git`
	
1. **Change to directoy where you clone the repository**

	`$ cd /path/to/repository`
	
1. **Activate the virtual environment for python**

	`$ pipenv shell`
	
	**NOTE**:  You must do this **each** time you start a new terminal window.
	
1. **Load required modules**
	
	`$ pipenv sync`
	
1. **Load required dev modules**
	
	`$ pipenv sync --dev`	
	
### Unit Testing

We use the following for unit testing

* [pytest](https://docs.pytest.org/en/stable/) - Unit Testing framework for Python
* [moto](https://github.com/spulec/moto) - A library to use in conjunction with **pytest**.  **Moto** allows you to easily mock out AWS Services for testing.

#### Running all Unit Tests

`$ pytest --html=.reports/report.html --self-contained-html`
	
#### Unit Test Markers
We also use [custom markers](https://docs.pytest.org/en/stable/example/markers.html).  

`pytest -v -m marker_name`

You can see a list of the available custom markers [here](./pytest.ini).

	
### Linting

We are using [flake8](https://flake8.pycqa.org/en/latest/).

Please see [.flake8](./.flake8) for our configuration.  We currently only ignore the long line errors (**E501**)

`flake8 .`
	
### Deployment

We are not publishing this python package to PyPI, instead we are including it in dependant projects directly from Github.  
When ready to deploy follow the steps below:

1.  Ensure any new dependencies are added to [setup.py](./setup.py)

	`$ pipenv-setup sync`
	
1.  Merge your changes into **master**
2.  Update **version** in [setup.py](./setup.py)

	**NOTE**:  We used [semantic versioning](https://semver.org/), please version appropriately.
	
2.  Commit the change to [setup.py](./setup.py).

2.  Tag the commit with the same version you put in [setup.py](./setup.py).

2.  Push the commit to master
3.  Push the tag.

FORCE CHANGE