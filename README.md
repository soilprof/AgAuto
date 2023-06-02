# Automation Scripts for AgWeather

Contains scripts written in Python to automate repetitive and error-prone tasks within AgWeather.


## Installation Instructions

 * On Windows 10
  * Download or clone the repository:
    * Use the 'Download ZIP' button on the right OR
    * ```git clone https://github.com/charleamaoAGR/AgWeather``` or ```git clone git@https://github.com/charleamaoAGR/AgWeather```
    
  * Install latest Anaconda for Python.
  * Create a virtual environment for Python 2.7 using Anaconda Prompt.
    ```
    conda create -n py27 python=2.7 anaconda
    ```
  * Activate py27
    ```
    conda activate py27
    ```
  * Navigate using cd commands until you're inside the AgAuto directory. Install the necessary packages listed in REQUIREMENTS.txt.
    ```
    pip install -r REQUIREMENTS.txt
    ```
  * You can use your favorite Python editor to run the code or you can use the terminal:
    ```
    python AgAuto.py
    ```
  * Later you can deactivate the virtualenv
    ```
    conda deactivate
    ```
  * The necessary packages are installed in the virtual environment, so the py27 virtual environment must be active so that the code works.

## Changes

More changes are coming soon, including more comprehensive documentation and support for Python 3.

**Coming soon:**
 * Python 3 Support
