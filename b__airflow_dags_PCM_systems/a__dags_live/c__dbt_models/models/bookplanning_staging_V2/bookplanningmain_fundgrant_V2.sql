        

        ------------------------------------------------------------------------------
        -- Jira         :   DEPA - 1351
        -- Source       :   dbt
        -- Project      :   snproject52c6e272
        -- Dataset      :   bookplanning_staging
        -- Path         :   -
        -- Name         :   bookplanningmain_fundgrant.sql
        -- Author       :   Jörg Sachsenweger
        -- Date         :   2023-10-10
        -- Update       :   -                   
        --
        -- Requester    :   joerg.zanner@springer.com
        -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
        --
        --------------------------------------------------------------------------    

        Select  main.* 
                ,bookplanningmain_fundgrant_funding.bookplanningmain_fundingid as fk_bookplanningmain_funding_fundingid        

        from {{ source ('bookplaning_cleansing','bookplanningmain_fundgrant') }}  as main        
        
                left join {{ source ('bookplaning_cleansing','bookplanningmain_fundgrant_funding') }} as bookplanningmain_fundgrant_funding
                on main.id = bookplanningmain_fundgrant_funding.bookplanningmain_fundgrantid
                