        

        ------------------------------------------------------------------------------
        -- Jira         :   DEPA - 1351
        -- Source       :   dbt
        -- Project      :   snproject52c6e272
        -- Dataset      :   bookplanning_staging
        -- Path         :   -
        -- Name         :   bookplanningmain_funding.sql
        -- Author       :   Jörg Sachsenweger
        -- Date         :   2023-10-10
        -- Update       :   -                   
        --
        -- Requester    :   joerg.zanner@springer.com
        -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
        --
        --------------------------------------------------------------------------    

        Select  main.* 
                ,bookplanningmain_funding_edition.bookplanningmain_editionid as fk_bookplanningmain_edition_editionid        

        from {{ source ('bookplaning_cleansing','bookplanningmain_funding') }}  as main        
        
                left join {{ source ('bookplaning_cleansing','bookplanningmain_funding_edition') }} as bookplanningmain_funding_edition
                on main.id = bookplanningmain_funding_edition.bookplanningmain_fundingid
                