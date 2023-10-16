        

        ------------------------------------------------------------------------------
        -- Jira         :   DEPA - 1351
        -- Source       :   dbt
        -- Project      :   snproject52c6e272
        -- Dataset      :   bookplanning_staging
        -- Path         :   -
        -- Name         :   bookplanningmain_sntdiscipline.sql
        -- Author       :   Jörg Sachsenweger
        -- Date         :   2023-10-10
        -- Update       :   -                   
        --
        -- Requester    :   joerg.zanner@springer.com
        -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
        --
        --------------------------------------------------------------------------    

        Select  main.* 
                ,bookplanningmain_sntdiscipline_edition.bookplanningmain_editionid as fk_bookplanningmain_edition_editionid        

        from {{ source ('bookplaning_cleansing','bookplanningmain_sntdiscipline') }}  as main        
        
                left join {{ source ('bookplaning_cleansing','bookplanningmain_sntdiscipline_edition') }} as bookplanningmain_sntdiscipline_edition
                on main.id = bookplanningmain_sntdiscipline_edition.bookplanningmain_sntdisciplineid
                