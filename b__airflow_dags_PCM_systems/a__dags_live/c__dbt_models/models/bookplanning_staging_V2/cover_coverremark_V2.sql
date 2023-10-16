        

        ------------------------------------------------------------------------------
        -- Jira         :   DEPA - 1351
        -- Source       :   dbt
        -- Project      :   snproject52c6e272
        -- Dataset      :   bookplanning_staging
        -- Path         :   -
        -- Name         :   cover_coverremark.sql
        -- Author       :   Jörg Sachsenweger
        -- Date         :   2023-10-10
        -- Update       :   -                   
        --
        -- Requester    :   joerg.zanner@springer.com
        -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
        --
        --------------------------------------------------------------------------    

        Select  main.* 
                ,cover_coverremark_cover.cover_coverid as fk_cover_cover_coverid
                ,cover_coverremark_user.system_userid as fk_system_user_userid        

        from {{ source ('bookplaning_cleansing','cover_coverremark') }}  as main        
        
                left join {{ source ('bookplaning_cleansing','cover_coverremark_cover') }} as cover_coverremark_cover
                on main.id = cover_coverremark_cover.cover_coverremarkid
                
                left join {{ source ('bookplaning_cleansing','cover_coverremark_user') }} as cover_coverremark_user
                on main.id = cover_coverremark_user.cover_coverremarkid
                