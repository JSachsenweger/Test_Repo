        

    ------------------------------------------------------------------------------
    -- Jira         :   DEPA - 1351
    -- Source       :   dbt
    -- Project      :   snproject52c6e272
    -- Dataset      :   bookplanning_staging
    -- Path         :   -
    -- Name         :   bookseries_link.sql
    -- Author       :   Jörg Sachsenweger
    -- Date         :   2023-10-10
    -- Update       :   -                   
    --
    -- Requester    :   joerg.zanner@springer.com
    -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
    --
    --------------------------------------------------------------------------    
    
    Select  main.* 
            ,bookseries_link_bibliographicseries.bookseries_bibliographicseriesid as fk_bookseries_bibliographicseries_bibliographicseriesid
            ,sub__bookseries_link_va_link_type._va_link_type        
    
    from {{ source ('bookplaning_cleansing','bookseries_link') }}  as main        
    
            left join {{ source ('bookplaning_cleansing','bookseries_link_bibliographicseries') }} as bookseries_link_bibliographicseries
            on main.id = bookseries_link_bibliographicseries.bookseries_linkid
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _va_link_type                              
                
                from {{ source ('bookplaning_cleansing','bookseries_link') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookseries_link_va_link_type') }}     as bookseries_link_va_link_type 
                on main.id = bookseries_link_va_link_type.bookseries_linkid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookseries_link_va_link_type.avatarclient_va_link_typeid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookseries_link_va_link_type 

            on main.id = sub__bookseries_link_va_link_type.id                      
            