        

    ------------------------------------------------------------------------------
    -- Jira         :   DEPA - 1351
    -- Source       :   dbt
    -- Project      :   snproject52c6e272
    -- Dataset      :   bookplanning_staging
    -- Path         :   -
    -- Name         :   cover_coveruploadcontext.sql
    -- Author       :   Jörg Sachsenweger
    -- Date         :   2023-10-10
    -- Update       :   -                   
    --
    -- Requester    :   joerg.zanner@springer.com
    -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
    --
    --------------------------------------------------------------------------    
    
    Select  main.* 
            ,cover_coveruploadcontext_backcovertifffile.cover_covertiffid as fk_cover_covertiff_backcovertifffileid
            ,cover_coveruploadcontext_cover.cover_coverid as fk_cover_cover_coverid
            ,cover_coveruploadcontext_dustjacketfile.cover_coverpdfid as fk_cover_coverpdf_dustjacketfileid
            ,cover_coveruploadcontext_jpegfile.cover_coverfigureid as fk_cover_coverfigure_jpegfileid
            ,cover_coveruploadcontext_pitstopreportfile.cover_workitemid as fk_cover_workitem_pitstopreportfileid
            ,cover_coveruploadcontext_session.system_sessionid as fk_system_session_sessionid
            ,cover_coveruploadcontext_tifffile.cover_covertiffid as fk_cover_covertiff_tifffileid        
    
    from {{ source ('bookplaning_cleansing','cover_coveruploadcontext') }}  as main        
    
            left join {{ source ('bookplaning_cleansing','cover_coveruploadcontext_backcovertifffile') }} as cover_coveruploadcontext_backcovertifffile
            on main.id = cover_coveruploadcontext_backcovertifffile.cover_coveruploadcontextid
            
            left join {{ source ('bookplaning_cleansing','cover_coveruploadcontext_cover') }} as cover_coveruploadcontext_cover
            on main.id = cover_coveruploadcontext_cover.cover_coveruploadcontextid
            
            left join {{ source ('bookplaning_cleansing','cover_coveruploadcontext_dustjacketfile') }} as cover_coveruploadcontext_dustjacketfile
            on main.id = cover_coveruploadcontext_dustjacketfile.cover_coveruploadcontextid
            
            left join {{ source ('bookplaning_cleansing','cover_coveruploadcontext_jpegfile') }} as cover_coveruploadcontext_jpegfile
            on main.id = cover_coveruploadcontext_jpegfile.cover_coveruploadcontextid
            
            left join {{ source ('bookplaning_cleansing','cover_coveruploadcontext_pitstopreportfile') }} as cover_coveruploadcontext_pitstopreportfile
            on main.id = cover_coveruploadcontext_pitstopreportfile.cover_coveruploadcontextid
            
            left join {{ source ('bookplaning_cleansing','cover_coveruploadcontext_session') }} as cover_coveruploadcontext_session
            on main.id = cover_coveruploadcontext_session.cover_coveruploadcontextid
            
            left join {{ source ('bookplaning_cleansing','cover_coveruploadcontext_tifffile') }} as cover_coveruploadcontext_tifffile
            on main.id = cover_coveruploadcontext_tifffile.cover_coveruploadcontextid
            