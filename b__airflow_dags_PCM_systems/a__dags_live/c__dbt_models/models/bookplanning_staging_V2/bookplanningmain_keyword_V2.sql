        

        ------------------------------------------------------------------------------
        -- Jira         :   DEPA - 1351
        -- Source       :   dbt
        -- Project      :   snproject52c6e272
        -- Dataset      :   bookplanning_staging
        -- Path         :   -
        -- Name         :   bookplanningmain_keyword.sql
        -- Author       :   Jörg Sachsenweger
        -- Date         :   2023-10-10
        -- Update       :   -                   
        --
        -- Requester    :   joerg.zanner@springer.com
        -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
        --
        --------------------------------------------------------------------------    

        Select  main.* 
                ,bookplanningmain_keyword_edition.bookplanningmain_editionid as fk_bookplanningmain_edition_editionid        

        from {{ source ('bookplaning_cleansing','bookplanningmain_keyword') }}  as main        
        
                left join {{ source ('bookplaning_cleansing','bookplanningmain_keyword_edition') }} as bookplanningmain_keyword_edition
                on main.id = bookplanningmain_keyword_edition.bookplanningmain_keywordid
                