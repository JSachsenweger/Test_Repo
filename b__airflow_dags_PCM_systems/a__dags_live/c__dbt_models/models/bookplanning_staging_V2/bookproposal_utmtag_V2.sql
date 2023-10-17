        

    ------------------------------------------------------------------------------
    -- Jira         :   DEPA - 1351
    -- Source       :   dbt
    -- Project      :   snproject52c6e272
    -- Dataset      :   bookplanning_staging
    -- Path         :   -
    -- Name         :   bookproposal_utmtag.sql
    -- Author       :   Jörg Sachsenweger
    -- Date         :   2023-10-10
    -- Update       :   -                   
    --
    -- Requester    :   joerg.zanner@springer.com
    -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
    --
    --------------------------------------------------------------------------    
    
    Select  main.* 
            ,bookproposal_utmtag_edition.bookplanningmain_editionid as fk_bookplanningmain_edition_editionid        
    
    from {{ source ('bookplaning_cleansing','bookproposal_utmtag') }}  as main        
    
            left join {{ source ('bookplaning_cleansing','bookproposal_utmtag_edition') }} as bookproposal_utmtag_edition
            on main.id = bookproposal_utmtag_edition.bookproposal_utmtagid
            