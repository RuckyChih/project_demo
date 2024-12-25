# Evaluation Tool

- [Evaluation Tool](#evaluation-tool)
  - [Installation (第一次執行)](#installation-第一次執行)
  - [Run](#run)


## Installation (第一次執行)

1. Install Python3.11 [官方下載 Link](https://www.python.org/downloads/)

    如果你用 conda 升級可以執行
    ```
    conda install python=3.11
    ```

2. Install Virtual Environment

    ```
    pip3 install virtualenv
    virtualenv eval_tools_env --python=3.11
    source eval_tools_env/bin/activate
    pip install -r requirements.txt
    ```

    執行完會看到生成了一個 eval_tools_env 虛擬環境的資料夾

## Run

1. Activate Virtual Environment

    啟動虛擬環境(執行前記得把路徑換到 evaluation_tool 下)
    ```
    source eval_tools_env/bin/activate
    ```

2. Run Streamlit

    ```
    streamlit run main.py
    ```

3. Deactivate Virtual Environment
   
    如要跳出虛擬環境
    ```
    deactivate
    ```
