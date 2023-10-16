        

        ------------------------------------------------------------------------------
        -- Jira         :   DEPA - 1351
        -- Source       :   dbt
        -- Project      :   snproject52c6e272
        -- Dataset      :   bookplanning_staging
        -- Path         :   -
        -- Name         :   cover_coverzip.sql
        -- Author       :   Jörg Sachsenweger
        -- Date         :   2023-10-10
        -- Update       :   -                   
        --
        -- Requester    :   joerg.zanner@springer.com
        -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
        --
        --------------------------------------------------------------------------    

        Select  main.* 
                ,cover_coverzip_cover.cover_coverid as fk_cover_cover_coverid
                ,cover_coverzip_coveruploadcontext.cover_coveruploadcontextid as fk_cover_coveruploadcontext_coveruploadcontextid        

        from {{ source ('bookplaning_cleansing','cover_coverzip') }}  as main        
        
                left join {{ source ('bookplaning_cleansing','cover_coverzip_cover') }} as cover_coverzip_cover
                on main.id = cover_coverzip_cover.cover_coverzipid
                
                left join {{ source ('bookplaning_cleansing','cover_coverzip_coveruploadcontext') }} as cover_coverzip_coveruploadcontext
                on main.id = cover_coverzip_coveruploadcontext.cover_coverzipid
                