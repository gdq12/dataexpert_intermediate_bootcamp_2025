### Get started with the lab

1. Spin up the containers from set up [README](../../setup_postgres/README.md)

    ```bash
    ~/git_repos/dataexpert_intermediate_bootcamp_2025/1_dimensional_data_modeling/1_setup_postgres

    make up
    ```

2. Go to PGadmin UI: [http://localhost:5050](http://localhost:5050) 

3. Go to server from the setup session. If starting from scratch follow setup instructions in [README](../1_setup_postgres/README.md)


### Initial issues with `game_details` table 

1. its a lof tbl that inserted an event more than 1x

    ```sql 
    select 
        game_id, 
        team_id, 
        player_id
    from game_details
    group by 1, 2, 3
    having count(1) > 1
    ;
    ```

2. its quite a denormalized tbl. For exmaple, there is both player_id and player_name in the table. A fact table would consist of player_id only in this instance 

    - adding dim cols into a fact table unecessarily increase the tbl memory size, umless the cols have low cardinality, they should reside in the dim tbl 

3. there is no when dimension (when the game occured), have to get that from the `game` tbl instead 

    - this is necessary to bring into the fact table because it is unique to each game_id

### Recommendations on transforming dimensions

1. the comment column indicates reason as to why a given player in a game had no start position. This column is really more like a raw data column that should attempt to parse. do this by creating 3 new cols and true false as to whether that record pertains to a particular category in comment 

2. dimension played mintes is provided in an workable format, as string and minute:seconds. best to convert this to a fraction --> doing it this way makes it easier for analyst to work with this col 

### Recommendations on the DDL structure 

1. columns that are more dimension like should be listed first in the tbl, the fact/numerical cols then should follow

2. col name convention:

    - cols that should be filtered on or listed in a group by should have the prefix `dim_`

    - cols that are metrics and should have calculations performed on them should have a prefix of `m_`

    - should change col names when building a fact tbl cause the raw col names are usually not very good and its better to make them more intuitive so end users know how to use the cols when querying the tbl 

3. primary key: use `team_id` to help create indexes

**goal of DE is to make data fun and easy to query**

### Creating the fact tbl 

1. create tbl DDL

    ```sql
    create table fct_game_details (
        dim_game_date date,
        dim_season integer,
        dim_team_id integer, 
        dim_player_id integer, 
        dim_player_name text,
        dim_start_position text,
        dim_is_playing_at_home boolean,
        dim_did_not_play boolean,
        dim_did_not_dress boolean,
        dim_not_with_team boolean,
        m_minutes real, 
        m_fgm integer, 
        m_fga integer,
        m_fg3m integer, 
        m_fg3a integer,
        m_ftm integer, 
        m_fta integer,
        m_oreb integer,
        m_dreb integer,
        m_reb integer,
        m_ast integer,
        m_stl integer,
        m_blk integer,
        m_turnovers integer,
        m_pf integer,
        m_pts integer,
        m_plus_minus integer,	
        primary key (dim_game_date, dim_team_id, dim_player_id)
    )
    ;
    ```

2. insert statment to populate tbl 

    ```sql 
    insert into fct_game_details
    with dedup as 
    (select 
        gd.*,
        g.game_date_est,
        g.season,
        g.home_team_id,
        row_number() over (partition by gd.game_id, gd.player_id, gd.team_id 
                            order by g.game_date_est) row_num
    from game_details gd
    join games g on gd.game_id = g.game_id
    -- where g.game_date_est = '2016-10-04' -- this is just to make the query run faster during testing/dev
    )
    select 
        game_date_est dim_game_date,
        season dim_season,
        team_id dim_team_id, 
        player_id dim_player_id, 
        player_name dim_player_name,
        start_position dim_start_position,
        team_id = home_team_id dim_is_playing_at_home,
        coalesce(position('DNP' in comment), 0) > 0 dim_did_not_play,
        coalesce(position('DND' in comment), 0) > 0 dim_did_not_dress,
        coalesce(position('NWT' in comment), 0) > 0 dim_not_with_team,
        split_part(min, ':', 1)::real + (split_part(min, ':', 2)::real/60) m_minutes, 
        fgm m_fgm, 
        fga m_fga,
        fg3m m_fg3m, 
        fg3a m_fg3a,
        ftm m_ftm, 
        fta m_fta,
        oreb m_oreb,
        dreb m_dreb,
        reb m_reb,
        ast m_ast,
        stl m_stl,
        blk m_blk,
        "TO" m_turnovers,
        pf m_pf,
        pts m_pts,
        plus_minus m_plus_minus
    from dedup 
    where row_num = 1
    ;
    ```

### Example analysis 

1. For a given players career to date, what percentage of their team games did they sit out on?

    ```sql
    select 
        dim_player_name,
        count(1) num_games,
        count(case when dim_not_with_team then 1 end) bailed_games,
        count(case when dim_not_with_team then 1 end)::real/count(1) bailed_pct
    from fct_game_details
    group by dim_player_name
    order by 4 desc
    ;
    ```