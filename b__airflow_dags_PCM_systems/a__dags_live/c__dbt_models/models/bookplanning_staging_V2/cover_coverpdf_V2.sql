        

    ------------------------------------------------------------------------------
    -- Jira         :   DEPA - 1351
    -- Source       :   dbt
    -- Project      :   snproject52c6e272
    -- Dataset      :   bookplanning_staging
    -- Path         :   -
    -- Name         :   cover_coverpdf.sql
    -- Author       :   Jörg Sachsenweger
    -- Date         :   2023-10-10
    -- Update       :   -                   
    --
    -- Requester    :   joerg.zanner@springer.com
    -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
    --
    --------------------------------------------------------------------------    
    
    Select  main.* 
            ,cover_coverpdf_cover.cover_coverid as fk_cover_cover_coverid
            ,cover_coverpdf_coveruploadcontext.cover_coveruploadcontextid as fk_cover_coveruploadcontext_coveruploadcontextid        
    
    from {{ source ('bookplaning_cleansing','cover_coverpdf') }}  as main        
    
            left join {{ source ('bookplaning_cleansing','cover_coverpdf_cover') }} as cover_coverpdf_cover
            on main.id = cover_coverpdf_cover.cover_coverpdfid
            
            left join {{ source ('bookplaning_cleansing','cover_coverpdf_coveruploadcontext') }} as cover_coverpdf_coveruploadcontext
            on main.id = cover_coverpdf_coveruploadcontext.cover_coverpdfid
            