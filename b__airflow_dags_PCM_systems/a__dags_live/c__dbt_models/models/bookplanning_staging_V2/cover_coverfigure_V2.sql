        

    ------------------------------------------------------------------------------
    -- Jira         :   DEPA - 1351
    -- Source       :   dbt
    -- Project      :   snproject52c6e272
    -- Dataset      :   bookplanning_staging
    -- Path         :   -
    -- Name         :   cover_coverfigure.sql
    -- Author       :   Jörg Sachsenweger
    -- Date         :   2023-10-10
    -- Update       :   -                   
    --
    -- Requester    :   joerg.zanner@springer.com
    -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
    --
    --------------------------------------------------------------------------    
    
    Select  main.* 
            ,cover_coverfigure_cover.cover_coverid as fk_cover_cover_coverid        
    
    from {{ source ('bookplaning_cleansing','cover_coverfigure') }}  as main        
    
            left join {{ source ('bookplaning_cleansing','cover_coverfigure_cover') }} as cover_coverfigure_cover
            on main.id = cover_coverfigure_cover.cover_coverfigureid
            