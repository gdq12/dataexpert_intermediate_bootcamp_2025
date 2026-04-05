## Getting started with the lab 

1. spin up a docker container based on image set up in [setup_dbt](../../setup_dbt)

    ```{bash}
    cd dataexpert_course_material/setup_dbt

    source .env
    code .
    ```
 2. test out everything all good with dbt setup via `dbt debug`

 ## Lecture notes 

 ### Incremental loading 

 * it has 3 requirements 

    - materialization strategy must be configed to `incremental`

    - model must exists in the DB already 

    - there is no `--full-refresh` flag in the `dbt build` command

* if any of the above criterias are false then dbt defaults to the `create or replace` command

* in snowflake the default incremental_strategy is `merge`

* for taking a look at what code was sent to snowflake:

    - `target/compiled/jaffle_shop/models/stageing/stg_order.sql` is the script executed to fetch the new records --> this is materialized as a temp tbl in snowflake 

    - `target/run/jaffle_shop/models/staging/stg_order.sql` is the script used to "add" the new records to the target model --> can be a merge, delete+ insert or append statment 

* can also see all this in the command line via the command: `dbt run -s ModelName --debug`

### Macros 

* they are defined as sql files and stored in [macros](../../setup_dbt/macros/) directory 

* the minmum syntax they should have is: 

    ```
    {% macro macro_name(param1, param2) %}

        max( {{ param1 }}/{{ param2 }} )

    {% endmacro %}
    ```

    - the macro name (when its call upon) is defined by the `{% macro %}` syntax

* calling upon the macro ina given model: 

    ```
    select 
        id,
        amount,
        {{ macro_name('amount', 'splits') }} as amount_normalized
    from {{ ref('ModelName') }}
    ```

* can also call upon macros in the command line

    - command: `dbt run-operation macroName --args '{"argName": "argValue"}'`

    - when macros are called upon by the CLI, its actually not being excecuted, **but** it will run expressions 

        + to run expression, need to use the equivalent of "print", `log()`

        + need to have the following syntax to render expressions:

            ```
            {% macro macro_name(param1, param2) %}

                {% set result = max( {{ param1 }}/{{ param2 }} ) %}

                {{ log(result, info=True) }}

            {% endmacro %}
            ```

* other uses for log() 

    - can also use it to combine multiple string values together to create print statments: `{{ log('the value is: ' ~ arg, info=True) }}`

* creating fail-safes in macros 

    - to catch edge case logics that should be alerted on 

    - syntax embeded in ifelse statments

    - can either use the raise error (`exceptions.raise_compiler_error()`) or warning (`exceptions.warn()`)

    - example:

        ```
        {% if arg1 > agr2 %}
            {{ exceptions.raise_compiler_error("Invalid inputs: " ~ arg1 ~ "and" ~ arg2) }}
        {% elif arg1 <> arg2 %}
            {{ exceptions.warn("input arguments aren't the same") }}
        {% else %}
            {{ log("macro run successfully", info=True) }}
        {% endif %}
        ```
* `adapter.dispatch('macroName')()`

    - this is a sort of extension typically used to build packages 

    - so when wanting to implement a macro within a package that is compatible with several DBs (each using different SQL syntax for the same action) --> can nest all the different syntax within a single macro

    - an example of this can be found in the macro [cents_to_dollars](../../setup_dbt/macros/cents_to_dollars.sql) 

* dbt built in variable `graph`

    - its a variable that holds all the dbt run centric info 

    - its native format is json

    - can use the different objects like var1.var2.var3 etc to extrapolate components of the project to then take on further actions 

    - for instance, to cleanup your current DB env --> can compare assets in DB to enabled models in the project to determine what needs to be materialized vs what needs to be dropped --> clean up DB env more 

### setting vars 

* they can be set at command line: `dbt run -s ModelName --var "{'my_var': 'foo'}"

* they can also be set within the model script: `{% set my_var = 'foo' %}`

    - should be set before its called upon, just like in python

    - to then call upon it with in the model for compilation, dont need to encapsulate it with var(): `{{ my_var }}`

    - can also set the var as a list and can then use it in some sort of for-loop compilation to enable more DRY method of coding 