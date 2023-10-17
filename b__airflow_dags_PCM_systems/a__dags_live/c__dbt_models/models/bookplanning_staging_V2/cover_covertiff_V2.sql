        

    ------------------------------------------------------------------------------
    -- Jira         :   DEPA - 1351
    -- Source       :   dbt
    -- Project      :   snproject52c6e272
    -- Dataset      :   bookplanning_staging
    -- Path         :   -
    -- Name         :   cover_covertiff.sql
    -- Author       :   Jörg Sachsenweger
    -- Date         :   2023-10-10
    -- Update       :   -                   
    --
    -- Requester    :   joerg.zanner@springer.com
    -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
    --
    --------------------------------------------------------------------------    
    
    Select  main.* 
            ,cover_covertiff_cover.cover_coverid as fk_cover_cover_coverid
            ,sub__cover_covertiff_va_cover_tiff_type._va_cover_tiff_type        
    
    from {{ source ('bookplaning_cleansing','cover_covertiff') }}  as main        
    
            left join {{ source ('bookplaning_cleansing','cover_covertiff_cover') }} as cover_covertiff_cover
            on main.id = cover_covertiff_cover.cover_covertiffid
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _va_cover_tiff_type                              
                
                from {{ source ('bookplaning_cleansing','cover_covertiff') }}     as main
                
                left join {{ source ('bookplaning_cleansing','cover_covertiff_va_cover_tiff_type') }}     as cover_covertiff_va_cover_tiff_type 
                on main.id = cover_covertiff_va_cover_tiff_type.cover_covertiffid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  cover_covertiff_va_cover_tiff_type.avatarclient_va_cover_tiff_typeid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__cover_covertiff_va_cover_tiff_type 

            on main.id = sub__cover_covertiff_va_cover_tiff_type.id                      
            