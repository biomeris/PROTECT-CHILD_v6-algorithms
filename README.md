<p align="center"><img src="https://protect-child.eu/wp-content/uploads/2025/01/Logo_Header.png" /></p>

# PROTECT-CHILD - Vantage6 algorithms

This (temporary) repository contains all algorithms developed for federated analysis with Vantage6 for the [PROTECT-CHILD](https://protect-child.eu/) project.

Below are the operational instructions for developing a new algorithm.

## Useful links

- [Vantage6 Documentation](https://docs.vantage6.ai/en/main/index.html)
- [Start a local vantage6 network](https://docs.vantage6.ai/en/main/introduction/quickstart.html#start-a-local-vantage6-network)

## Python environment setup

### Python 
Python 3.13 is required to use v5 of Vantage6.

### uv
If `uv` is not installed, follow the installation instructions available at the following link: [Installing uv](https://docs.astral.sh/uv/getting-started/installation/)

### Install Vantage6 using `uv`
Create a virtual Python environment:
```bash
uv venv --python 3.13
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

To install Vantage6 v5 run the following command:
```bash
uv pip install vantage6
```

## Develop a new algorithm
The following instructions refer to using Visual Studio Code as the editor. Alternatively, another editor or IDE can be used (e.g., PyCharm).

> [!TIP]
> Develop the algorithm in a Jupyter notebook (e.g., `test-algorithm.ipynb`), including only the computation part and omitting Vantage6-specific functions for now. The Vantage6 integration should be added only after confirming that the algorithm produces the expected results using test datasets.

### Steps in VS Code

1. Open the working folder that will contain the algorithm  
2. Press `⌘⇧P` (cmd+shift+P) → select `Python: Select Interpreter` → choose the environment created earlier (e.g., `demo-env`)

Once the algorithm has been developed and tested, integrate it with the Vantage6 components.

### Command-line instructions

```bash
# Create a new Vantage6 algorithm template
v6 algorithm create
```

Running this command will prompt you to answering some questions, which will result in a personalized starting point or ‘boilerplate’ for your algorithm. After doing so, you will have a new folder with the name of your algorithm, boilerplate code and a checklist in the `README.md` file that you can follow to complete your algorithm.

In VS Code, open the newly created folder and integrate the previously developed code, adapting it to Vantage6 requirements.

If you need to update some aspects of the algorithm by changing the answers from the creation step:

```bash
# Update answers
v6 algorithm update --change-answers
```

[Algorithm development step-by-step guide](https://docs.vantage6.ai/en/version-5.0.0b1/algorithms/develop.html)

## Build your algorithm into a docker image

To be able to run your algorithm in the vantage6 infrastructure, you need to make your algorithm available online. The easiest way to do this is to use Dockerhub.

The algorithm boilerplate contains a `Dockerfile` in the root folder. Enter the following commands to build your algorithm into a docker image.

```bash
cd /directory/with/dockerfile
# replace $myusername with your Dockerhub username
docker login -u $myusername
docker build -t $myusername/algorithm-name .
docker push $myusername/algorithm-name
```

## Publish the algorithm in the algorithm store

> [!IMPORTANT]
> This is required if you want to run your algorithm in the user interface: the user interface gathers information about how to run the algorithm from the algorithm store.

The boilerplate created contains an `algorithm_store.json` that contains a JSON description of the algorithm.

You can put the algorithm in the store by selecting the local algorithm store in the UI, for example.
You can do this by selecting that store in the UI, and then by clicking on the “Add algorithm” button on the page with approved algorithms. You can upload the `algorithm_store.json` file in the top. After uploading it, you can change the details of the algorithm before submitting it.
