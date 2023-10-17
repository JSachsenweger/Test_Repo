        

    ------------------------------------------------------------------------------
    -- Jira         :   DEPA - 1351
    -- Source       :   dbt
    -- Project      :   snproject52c6e272
    -- Dataset      :   bookplanning_staging
    -- Path         :   -
    -- Name         :   bookplanningutils_historicsubjectcollection.sql
    -- Author       :   Jörg Sachsenweger
    -- Date         :   2023-10-10
    -- Update       :   -                   
    --
    -- Requester    :   joerg.zanner@springer.com
    -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
    --
    --------------------------------------------------------------------------    
    
    Select  main.* 
            ,bookplanningutils_historicsubjectcollection_edition.bookplanningmain_editionid as fk_bookplanningmain_edition_editionid        
    
    from {{ source ('bookplaning_cleansing','bookplanningutils_historicsubjectcollection') }}  as main        
    
            left join {{ source ('bookplaning_cleansing','bookplanningutils_historicsubjectcollection_edition') }} as bookplanningutils_historicsubjectcollection_edition
            on main.id = bookplanningutils_historicsubjectcollection_edition.bookplanningutils_historicsubjectcollectionid
            