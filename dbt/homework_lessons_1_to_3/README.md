**dbt week 1 homework**
====================

This repository emulates an “open-source” project, though exclusively shared within the dataexpert community. Members can access the repository for independent use or contribute enhancements to the project's design and functionality. This serves as an opportunity to practice contributing to publicly shared open-source repositories.

# 📑 Table of Contents

1. [🚀 Getting Started](#-getting-started)  
   - [Prerequisites](#prerequisites)  
   - [Local Development](#local-development)  

2. [⚙️ dbt Project Setup](#dbt-project-setup)  
   - [Step 1: Create a Branch and Homework Folder](#step-1-create-a-new-branch-and-your-homework-folder)  
   - [Step 2: Create a Virtual Environment](#step-2-create-a-virtual-environment)  
   - [Step 3: Activate the Virtual Environment](#step-3-activate-the-virtual-environment)  
   - [Step 4: Install Required Packages](#step-4-install-the-required-packages)  
   - [Step 5: Set Environment Variables](#step-5-set-environment-variables)  
   - [Step 6: Test Your Connection](#step-6-test-your-connection)  
   - [Step 7 (Optional): dbt Power User Extension](#step-7-optional-set-up-dbt-power-user-extension-in-vscode)  

3. [🏃 Running dbt](#running-dbt)  
   - [Install dbt Packages](#-install-dbt-packages)  
   - [Build Your Models](#-build-your-models)  
   - [Run a Specific Model](#-run-a-specific-model)  
   - [Generate and View Docs](#-generate-and-view-docs)  
   - [Inspect dbt Output](#-check-what-dbt-is-running)  
   - [Common dbt Commands](#-pro-tips-common-dbt-commands)  

4. [📊 dbt Assignment](#-dbt-assignment)  
   - [Step 1: Add New Sources](#1---add-new-sources)  
   - [Step 2: Create Base Models](#2---create-base-models)  
   - [Step 3: Seed Valid Email Domains](#3---create-a-seed-for-valid-email-domains)  
   - [Step 4: Fact and Dimension Tables](#4---create-the-fact-and-dimension-tables)  
   - [Step 5 (Optional): Add Custom Tests](#optional-5---add-custom-tests)  

5. [📄 Required Files List](#-list-of-files-required)  
6. [📤 Submission Instructions](#submission)  
7. [📚 Other Helpful Resources](#-other-helpful-resources-for-learning)  
8. [📂 Navigating the Repository](#-navigating-the-repository)


# **🚀 Getting Started**

## **Prerequisites**

1. **Python >= 3.9**

## **Local Development**

1. **Clone the Repository**: Open a terminal, navigate to your desired directory, and clone the repository using:
    ```bash
    git clone git@github.com:DataExpert-io/analytics-engineering-bootcamp-homework.git # clone the repo
    cd analytics-engineering-bootcamp-homework # navigate into the new folder
    ```

    1. If you don’t have SSH configured with the GitHub CLI, please follow the instructions for [generating a new SSH key](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent) and [adding a new SSH key to your GitHub account](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account?tool=cli) in the GitHub docs.


## dbt Project Setup

> ⚠️ **Important:
Please make sure you are in a personal branch, and not main.**

### Step 1: Create a new branch and your homework folder
-  **Create a Branch:**
    - Navigate to the **`analytics-engineering-bootcamp-homework/dbt_basics/homework`** folder on your local machine.
    - Use the **`git checkout -b`** command to create a new branch where you can commit and push your changes. Prefix your branch name with your Git username to avoid conflicts.
          For example:

        ```bash
        git checkout -b homework/my-git-username
        ```
    - Create a copy of the **`template/`** folder and rename it to **`<your-git-username>`**, for example **`dbt_basics/homework/bruno`**.

- Go to the project's directory, assuming you are already in the **`homework`** folder:
  ```bash
  cd <your-git-username>
  ```

### Step 2: Create a Virtual Environment
```bash
python3 -m venv venv # MacOS/Linux
# or
python -m venv venv # Windows/PC
```

### Step 3: Activate the Virtual Environment
```bash
source venv/bin/activate # MacOS/Linux
# or for Windows:
# CMD:
venv\Scripts\activate.bat
# PowerShell:
venv\Scripts\Activate.ps1
```

### Step 4: Install the Required Packages
```bash
pip3 install -r dbt-requirements.txt # MacOS/Linux
# or
pip install -r dbt-requirements.txt # Windows/PC
```

### Step 5: Set Environment Variables

We will configure four environment variables needed by dbt:

| Variable | Purpose |
|:---|:---|
| `STUDENT_SCHEMA` | Tells dbt which database schema to use for your personal work. Each student should have a different schema to avoid conflicts. |
| `SNOWFLAKE_USER` | Your snowflake user. |
| `SNOWFLAKE_PASSWORD` | Your snowflake password. |
| `DBT_PROFILES_DIR` | Tells dbt where to find your `profiles.yml` file (set to the current folder `.`). |
| `DBT_PROJECT_DIR` | Tells dbt where to find your `dbt_project.yml` file (set to the current folder `.`). |
| `DBT_PARTIAL_PARSE` | Disables partial parsing to avoid known bugs with snapshots and sources. Setting this to `'False'` forces dbt to do a full parse every time, which is safer for our setup. |

> ⚠️ **Important:**
> Please make sure you also update the `.env` file in the **root directory** of the project with your correct `STUDENT_SCHEMA`.
> This file will later be read automatically by Astronomer Airflow (Docker) to set environment variables when simulating a production environment.

> ⚠️ **Note on Partial Parsing:**
> There's a known issue in `dbt-core` when using snapshot definitions (in the new YAML format) that snap a source. If you modify the source, partial parsing may cause errors—especially in environments like dbt Cloud IDE, which uses partial parsing automatically.
> To avoid this, we explicitly disable partial parsing by setting `DBT_PARTIAL_PARSE='False'`. This ensures that dbt performs a **full parse** on every run, which avoids errors.
> Since our project is small, this will not cause any noticeable performance issues.

> ⚠️ **Warning:**
> Never push your personal changes (such as your `.env` updates) to the main or production branch.
> This can cause conflicts with other students' work and break shared environments. Always keep your local changes private or work on a separate branch if needed.

---

#### MacOS/Linux

- **Temporary (for current terminal session only)**:
  ```bash
  export STUDENT_SCHEMA='your_schema' # e.g., export STUDENT_SCHEMA='john'
  export SNOWFLAKE_USER='your snowflake user'
  export SNOWFLAKE_PASSWORD='your snowflake password'
  export DBT_PROFILES_DIR='.'
  export DBT_PROJECT_DIR='.'
  export DBT_PARTIAL_PARSE='False'
  ```

- **Permanent (applies to all terminal sessions)**:
  - Add the same lines to your shell configuration file (like `~/.bashrc`, `~/.zshrc`, or `~/.profile`):
    ```bash
    export STUDENT_SCHEMA='your_schema'
    export SNOWFLAKE_USER='your snowflake user'
    export SNOWFLAKE_PASSWORD='your snowflake password'
    export DBT_PROFILES_DIR='.'
    export DBT_PROJECT_DIR='.'
    export DBT_PARTIAL_PARSE='False'
    ```
  - Then reload your shell configuration:
    ```bash
    source ~/.bashrc  # or ~/.zshrc, depending on your system
    ```

---

#### Windows/PC

- **Temporary (for current terminal session only)**:
  - **CMD**:
    ```cmd
    set STUDENT_SCHEMA=your_schema
    set SNOWFLAKE_USER=your snowflake user
    set SNOWFLAKE_PASSWORD=your snowflake password
    set DBT_PROFILES_DIR=.
    set DBT_PROJECT_DIR=.
    set DBT_PARTIAL_PARSE=False
    ```
  - **PowerShell**:
    ```powershell
    $env:STUDENT_SCHEMA = "your_schema"
    $env:SNOWFLAKE_USER = "your snowflake user"
    $env:SNOWFLAKE_PASSWORD = "your snowflake password"
    $env:DBT_PROFILES_DIR = "."
    $env:DBT_PROJECT_DIR = "."
    $env:DBT_PARTIAL_PARSE = "False"
    ```

- **Permanent**:
  - Open **Environment Variables** settings.
  - Under **User variables**, click "**New**" and create each one:
    - `STUDENT_SCHEMA` → your schema (e.g., `john`)
    - `SNOWFLAKE_USER` → `your snowflake user`
    - `SNOWFLAKE_PASSWORD` → `your snowflake password`
    - `DBT_PROFILES_DIR` → `.`
    - `DBT_PROJECT_DIR` → `.`
    - `DBT_PARTIAL_PARSE` → `False`

> ⚠️ Note: Variables set with `set` or `$env:` are temporary for that terminal session only unless you add them permanently in system settings.


---

### Step 6: Test Your Connection

Run:

```bash
dbt debug
```

If everything is configured correctly, you should see output like:

 ```
    13:43:43  Running with dbt=1.9.0-b3
    13:43:43  dbt version: 1.9.0-b3
    13:43:43  python version: 3.9.6
    13:43:43  python path: .../dbt-basics/venv/bin/python3
    13:43:43  os info: macOS-15.1-arm64-arm-64bit
    13:43:44  Using profiles dir at .
    13:43:44  Using profiles.yml file at ./profiles.yml
    13:43:44  Using dbt_project.yml file at ./dbt_project.yml
    13:43:44  adapter type: snowflake
    13:43:44  adapter version: 1.8.4
    13:43:44  Configuration:
    13:43:44    profiles.yml file [OK found and valid]
    13:43:44    dbt_project.yml file [OK found and valid]
    13:43:44  Required dependencies:
    13:43:44   - git [OK found]

    13:43:44  Connection:
    13:43:44    account: aab46027.us-west-2
    13:43:44    user: dataexpert_student
    13:43:44    database: DATAEXPERT_STUDENT
    13:43:44    warehouse: COMPUTE_WH
    13:43:44    role: ALL_USERS_ROLE
    13:43:44    schema: john
    13:43:44    authenticator: None
    13:43:44    oauth_client_id: None
    13:43:44    query_tag: john
    13:43:44    client_session_keep_alive: False
    13:43:44    host: None
    13:43:44    port: None
    13:43:44    proxy_host: None
    13:43:44    proxy_port: None
    13:43:44    protocol: None
    13:43:44    connect_retries: 0
    13:43:44    connect_timeout: 10
    13:43:44    retry_on_database_errors: False
    13:43:44    retry_all: False
    13:43:44    insecure_mode: False
    13:43:44    reuse_connections: True
    13:43:44  Registered adapter: snowflake=1.8.4
    13:43:50    Connection test: [OK connection ok]

    13:43:50  All checks passed!
 ```

---

### ✅ You are now ready to start working with dbt!

---

### Quick Notes:

- `STUDENT_SCHEMA` → your personal schema (different for each user)
- `DBT_PROFILES_DIR` → points dbt to your `profiles.yml`
- `DBT_PROJECT_DIR` → points dbt to your `dbt_project.yml`

---

### Step 7 (Optional): Set up dbt Power User Extension in VSCode

If you use **VSCode**, you can install the [dbt Power User extension](https://marketplace.visualstudio.com/items?itemName=innoverio.vscode-dbt-power-user) to enhance your development experience with features like model navigation, documentation previews, and dbt command integration.

#### Installation:
1. Open VSCode.
2. Go to the Extensions panel (`Ctrl+Shift+X`).
3. Search for **"dbt Power User"** and click **Install**.

#### Configuration:
Follow the extension setup instructions.
If you use this extension, you must also create a `.env` file inside the `dbt_project/` folder with the following line:

```env
STUDENT_SCHEMA=<your_schema>  # e.g., STUDENT_SCHEMA=john
```

> This allows the extension to parse your `dbt_project.yml` and macros correctly using your schema.

To use this extension properly, you need to open VSCode inside the dbt_project/ folder.

Great additions! Here's how you can include that in your README under a new section. I’ve slightly refined the phrasing for clarity and added a clean structure:

---

# Running dbt

Once your environment is ready, here are some essential commands to start working with dbt:

## ✅ Install dbt Packages

Before running any models, install the packages defined in `packages.yml`:

```bash
dbt deps
```

> If you skip this step, dbt will throw errors when trying to run or compile your project.

---

## 🏗️ Build Your Models

To create all tables and views in your Snowflake schema:

```bash
dbt build
```

This runs models, tests, seeds, and snapshots (if defined). After running this, you can verify the created objects in your schema on Snowflake.

---

## 🧪 Run a Specific Model

To build only a single model:

```bash
dbt build -s your_model_name
```

You can also use other selectors like:

- `+your_model_name` → builds the model and its **parents (upstream models)**
- `your_model_name+` → builds the model and all **childs (downstream models)**
- `+your_model_name+` → builds **everything related** (parents and children)

More about selection syntax: https://docs.getdbt.com/reference/node-selection/syntax

---

## 📊 Generate and View Docs

To see a visual representation and documentation of your project:

```bash
dbt docs generate
dbt docs serve
```

This will open a web page with your dbt models, dependencies, and documentation.

---

## 🔍 Check What dbt is Running

You can inspect the compiled SQL and files generated by dbt:

- Compiled SQL: `target/compiled/`
- Executed SQL: `target/run/`

These folders show exactly what dbt sends to Snowflake, which is helpful for debugging and learning.

---

## 🧠 Pro Tips: Common dbt Commands

| Command | Purpose |
|--------|---------|
| `dbt run` | Runs only models (not tests or seeds) |
| `dbt seed` | Loads seed CSV files into your database |
| `dbt test` | Runs tests defined in `.yml` files |
| `dbt build` | Runs models + tests + seeds + snapshots |
| `dbt clean` | Removes `dbt_modules` and `target/` |
| `dbt list` | Lists models, seeds, snapshots, etc. |
| `dbt run-operation` | Executes a macro manually |
| `dbt compile` | Compiles your models without running them |
| `dbt ls -s tag:your_tag` | Selects models by tag |

> 🧩 You can combine selectors and flags for powerful workflows. For example:
> ```bash
> dbt build -s staging+ --exclude tag:skip_ci
> ```

---

# 📊 dbt Assignment

**🎃 Welcome to the Haunted House Extravaganza! 👻🎟️**
Step into the data-filled world of thrills and chills as you venture into a theme park with 10 spine-tingling haunted houses!

🏚️ Your mission this week? Manage all the creepy customer tickets, terrifying feedback, and haunted house details lurking in the shadows. 💀

--

Remember that, if you are running your project for the first time, dbt will ask you to run

```bash
$ dbt deps
```

to install the packages defined in packages.yml

## Steps

### 1 - Add new sources

Inside the `models/staging` folder, create a source YAML file called `_sources.yml` and add the sources:
- `bootcamp.raw_customers`
- `bootcamp.raw_customer_feedbacks`
- `bootcamp.raw_haunted_houses`
- `bootcamp.raw_haunted_house_tickets`
to the `_sources.yml` file in the `models/staging` folder. (more info about sources here https://docs.getdbt.com/docs/build/sources)

### 2 - Create base models

Inside the `models/staging` folder, create a base model for each source (In dbt we call them staging models).
- `stg_customers.sql`
- `stg_customer_feedbacks.sql`
- `stg_haunted_houses.sql`
- `stg_haunted_house_tickets.sql`

These models should pull data from the sources. (use the `{{ source() }}` function we saw in the lab and lecture), and it should select all columns.

> :bulb: Note:
>
> You run your models by running in the terminal:
> - `dbt run` (To run all the models in the project)
>or
>- `dbt run -s stg_customers` (To run the specific model)
>

Inside the `models/staging` folder, create a YAML file for each model to include tests.
- `stg_customers.yml`
- `stg_customer_feedbacks.yml`
- `stg_haunted_houses.yml`
- `stg_haunted_house_tickets.yml`


Add at least `unique`and `not_null` tests to the primary keys. Feel free to add more tests to other columns. [More info about tests here](https://docs.getdbt.com/docs/build/data-tests#generic-data-tests).

> :bulb: Note:
>
> You test your models by running in the terminal:
> - `dbt test` (To test all the models in the project)
>or
>- `dbt test -s stg_customers` (To test the specific model)
>

> :bulb: Note:
>
> You run AND test your models by running in the terminal:
> - `dbt build` (To run AND test all the models in the project)
>or
>- `dbt build -s stg_customers` (To run AND test the specific model)
>


_(Optional) You can add documentation to your models. [Here's how to do it](https://docs.getdbt.com/docs/build/documentation#adding-descriptions-to-your-project)._

### 3 - Create a seed for valid email domains

As part of the logic of a _Dimension Table_ we will create in the next step, we want to validate the accepeted email domains for customers.

To be able to do it, we need to create a list of valid email domains. This is a good use-case for a seed. [More info about seeds here](https://docs.getdbt.com/docs/build/seeds).

Inside the **`seeds/`** folder, create a CSV file named **`**valid_domains.csv`**.

This CSV should have one single column name **`valid_domain`**, and two rows:
- `@example.com`
- `@example.io`

> :bulb: Note:
>
> You can run your seed by running in the terminal:
> - `dbt seed` (To run all the seeds in the project)
>or
>- `dbt seed -s valid_domains` (To run the specific seed)
>

### 4 - Create the Fact and Dimension tables

#### 4.1 - fact_visits
We will create a fact table to display information about a customer's visit to a haunted_house.

Inside the **`models/marts`** folder, create the fact_visits SQL file **`fact_visits.sql`**

Use your SQL skills to select the data from the models:
- `stg_haunted_house_tickets`
- `stg_customer_feedbacks`

When selecting from another model, use the `{{ ref('model_name') }}` function.

And join them to display all this information:
- `ticket_id`
- `customer_id`
- `haunted_house_id`
- `purchase_date`
- `visit_date`
- `ticket_type`
- `ticket_price`
- `rating`
- `comments`

--

Inside the **`models/marts`** folder, create the fact_visits YAML file **`fact_visits.yml`**.

Add at least `unique`and `not_null` tests to the primary key.

#### 4.2 - dim_haunted_houses

Inside the **`models/marts`** folder, create the fact_visits SQL file **`dim_haunted_houses.sql`**

Use your SQL skills to select the data from the model:
- `stg_haunted_houses`

When selecting from another model, use the `{{ ref('model_name') }}` function.

Display all this information:
- `haunted_house_id`
- `house_name`
- `park_area`
- `theme`
- `fear_level`
- `house_size_in_ft2` (The original `house_size` columns)
- `house_size_in_m2` (read below)

_(Optional) The column `house_size_in_m2` can be calculated using a [macro](https://docs.getdbt.com/docs/build/jinja-macros). For that, you can follow the [dbt guide](https://docs.getdbt.com/docs/build/jinja-macros#macros) to declare your macro and call it in your model._

--

Inside the **`models/marts`** folder, create the fact_visits YAML file **`dim_haunted_houses.yml`**.

Add at least `unique`and `not_null` tests to the primary key.

#### 4.3 - dim_customers

Inside the **`models/marts`** folder, create the fact_visits SQL file **`dim_customers.sql`**

Use your SQL skills to select the data from the model and seed:
- `stg_customers`
- `valid_domains`

When selecting from another model/seed, use the `{{ ref('model_name') }}` function.

Display all this information:
- `customer_id`
- `age`
- `gender`
- `email`
- `is_valid_email_address` (read below)

You will need to create a logic that uses the `valid_domains` seed to check if the customer has a valid email address.

A valid email address is an email:
- Containing a `@` separating the username from the domain.
- Containing a `.` in the domain.
- Having a domain listed in `valid_domains`.
[You can use this doc as referece to this validation, it is literally the same thing](https://docs.getdbt.com/docs/build/unit-tests#unit-testing-a-model).

--

Inside the **`models/marts`** folder, create the fact_visits YAML file **`dim_customers.yml`**.

Add at least `unique`and `not_null` tests to the primary key.

### (Optional) 5 - Add custom tests

#### (Optional) 5.1 Custom generic test
Inside the **`tests/generic`** folder, create file named **`is_positive.sql`**.

Inside this file, create a [generic test](https://docs.getdbt.com/best-practices/writing-custom-generic-tests) called `is_positive`, that asserts if the column has only positive values.

Add this test to the **`ticket_price`** column in **`models/staging/stg_haunted_house_tickets`**.



#### (Optional) 5.2 Unit test
Inside the **`models/marts/dim_customers.yml`** file, create a _unit test_ that checks if your logic to validate emails are right.

[You can use this doc as referece, it is literally the same thing](https://docs.getdbt.com/docs/build/unit-tests#unit-testing-a-model).

> :bulb: Just remember of including the example.io case too.
>



### 📄 List of files required:
Use this list to check if you have all the file for the homework.

#### sources
- `models/staging/_sources.yml`
#### staging
- `models/staging/stg_customer_feedbacks.sql`
- `models/staging/stg_customer_feedbacks.yml`
- `models/staging/stg_customers.sql`
- `models/staging/stg_customers.yml`
- `models/staging/stg_haunted_house_tickets.sql`
- `models/staging/stg_haunted_house_tickets.yml`
- `models/staging/stg_haunted_houses.sql`
- `models/staging/stg_haunted_houses.yml`
### seeds
- `seeds/valid_domains.csv`
#### marts
- `models/marts/fact_visits.sql`
- `models/marts/fact_visits.yml`
- `models/marts/dim_haunted_houses.sql`
- `models/marts/dim_haunted_houses.yml`
- `models/marts/dim_customers.sql`
- `models/marts/dim_customers.yml`
#### (Optional) tests
- `tests/generic/is_positive.sql`
- `models/marts/dim_customers.yml` (the unit test goes here)
#### (Optional) macros
- `macros/<the_name_you_want>.sql`

### Submission
To submit your work, compress the whole project (everything inside dbt_basics/homework/<your_name>/ folder) into a ZIP file. Upload the ZIP file in the assignments page.

⚠️⚠️⚠️
**Before ziping the project, please delete the folders:**
- dbt_packages/
- logs/
- target/
- venv/

They can become quite heavy and are not required.
⚠️⚠️⚠️

# 📚 Other helpful resources for learning!

### dbt docs
- [dbt best practices for enterprises](https://www.phdata.io/blog/accelerating-and-scaling-dbt-for-the-enterprise/)
- [dbt cheat sheet](https://github.com/bruno-szdl/cheatsheets/blob/main/dbt_cheat_sheet.pdf)
- [models](https://docs.getdbt.com/docs/build/sql-models)
- [tests](https://docs.getdbt.com/docs/build/data-tests)
- [sources](https://docs.getdbt.com/docs/build/sources)
- [seeds](https://docs.getdbt.com/docs/build/seeds)
- [snapshots](https://docs.getdbt.com/docs/build/snapshots)
- [dbt_project.yml](https://docs.getdbt.com/reference/dbt_project.yml)
- [profiles,yml](https://docs.getdbt.com/docs/core/connect-data-platform/profiles.yml)
- [Commands](https://docs.getdbt.com/reference/commands/build)
- [Node selection](https://docs.getdbt.com/reference/node-selection/syntax)

### 📂 Navigating the Repository

Each dbt project contains various directories and files. Learn more about the structure of the project below:

- **`dbt_packages/`**: This folder is where dbt install packages (outside projects).
- **`logs/`**: This folder is where dbt store logs.
- **`macros/`**: This folder is where dbt searches for custom macros.
- **`models/`**: This folder is where dbt searches for models. You can create subfolders in the way you want, no problem.
- **`seeds/`**: This folder is where dbt searches for seeds.
- **`snapshots/`**: This folder is where dbt searches for snapshots.
- **`target/`**: This folder is where dbt stores [artifacts](https://docs.getdbt.com/reference/artifacts/dbt-artifacts) and the compiled SQL code (the code dbt sends to the data warehouse to run).
- **`tests/`**: This folder is where dbt searches for custom tests (generic or singular).
- **`dbt_project.yml`**: Every dbt project needs a dbt_project.yml file — this is how dbt knows a directory is a dbt project. It also contains important information that tells dbt how to operate your project. [More info here](https://docs.getdbt.com/reference/dbt_project.yml).
- **`packages.yml`**: This folder is where you define the packages you want dbt to install.

### Sources

* fetched from [analytics-engineering-bootcamp-homework](https://github.com/DataExpert-io/analytics-engineering-bootcamp-homework/tree/main/dbt_basics/homework) repo