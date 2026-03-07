### Get started with the lab

1. follow instructions under [#2 of Unit Testing PySpark Course Getting Started](../../setup_spark/README.md)

**all jobs are coded in [src/jobs](../../setup_spark/src/jobs/)**

**all test are coded in [src/tests](../../setup_spark/src/tests/)**

### Building out spark scripts for testing 

* seperate out the logic of the code into multiple parts/functions

    - makes it easier to implement test.

    - a good example is in [src/jobs/players_scd_job.py](../../setup_spark/src/jobs/players_scd_job.py), where the actual transformation logic is handled by its own function and called upon via `main()`

* for pytest to run a function as a test, the function named must start with `test_*`

* when buidling test, need to make sure that comparing vars of same data type. For instance in [test_team_vertex_job.py](../../setup_spark/src/tests/test_team_vertex_job.py), need to make sure that using `map` within the spark sql query to build the select table **AND** the expected DF has the same data type. 

    - FYI when using struct vs map: in map all vars need to be the same data type 

    - also need to make sure the var name between the 2 are the same as well 

    - when want to compare 2 DFs without considering nullables, then add `assert_df_equality(df1, df2, ignore_nullable = True)`

### Explanation of some of the test 

* testing spark connection 

    ```{python}
    @pytest.fixture(scope='session')
    def spark():
        return SparkSession.builder \
        .master("local") \
        .appName("chispa") \
        .getOrCreate()
    ```

    * test out spark anywhere it is referenced 

    * the spark session is defined in [conftest.py](../../setup_spark/src/tests/conftest.py), it provides the spark session connection for `tests_*` in the same directory 

* [test_player_scd.py](../../setup_spark/src/tests/test_player_scd.py):

    1. [players_scd_job.py](../../setup_spark/src/jobs/players_scd_job.py) takes transaction/fact entries and calculates slowly changing dimension records

    2. the function `test_scd_generation` in [test_player_scd.py](../../setup_spark/src/tests/test_player_scd.py) compares the scd entries created by `do_player_scd_transformation` to that of expected values (those hard coded to be the correct true records)

    3. if the comparison comes out to be false by the assert, then the test will fail 

* [test_team_vertex_job.py](../../setup_spark/src/tests/test_team_vertex_job.py)

    - this is testing is evaluating the folllwing:
        
        + the validity of the transformation in function `do_team_vertex_transformation` in [team_vertex_job.py](../../setup_spark/src/jobs/team_vertex_job.py)

        + if the query is indeed deduping the records 

    - `Team` and `TeamVertex` are the predefined schemas created for the testing 

    - `test_vertex_generation` function is verifying whether the query can correctly take source transactional/fact records and transform them into anticipated vertex records 

    - the goal of entry `Team(1, "GSW", "Bad Warriors", "San Francisco", "Chase Center", 1900)` in `input_data` is to verify that `row_number()` is correctly keeping only 1 record per ID