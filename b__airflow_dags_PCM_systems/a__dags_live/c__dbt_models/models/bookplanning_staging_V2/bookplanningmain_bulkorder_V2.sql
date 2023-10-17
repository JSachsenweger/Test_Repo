        

    ------------------------------------------------------------------------------
    -- Jira         :   DEPA - 1351
    -- Source       :   dbt
    -- Project      :   snproject52c6e272
    -- Dataset      :   bookplanning_staging
    -- Path         :   -
    -- Name         :   bookplanningmain_bulkorder.sql
    -- Author       :   Jörg Sachsenweger
    -- Date         :   2023-10-10
    -- Update       :   -                   
    --
    -- Requester    :   joerg.zanner@springer.com
    -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
    --
    --------------------------------------------------------------------------    
    
    Select  main.* 
            ,bookplanningmain_bulkorder_rendition.bookplanningmain_renditionid as fk_bookplanningmain_rendition_renditionid
            ,sub__bookplanningmain_bulkorder_country._vacountry
            ,sub__bookplanningmain_bulkorder_state._vastate        
    
    from {{ source ('bookplaning_cleansing','bookplanningmain_bulkorder') }}  as main        
    
            left join {{ source ('bookplaning_cleansing','bookplanningmain_bulkorder_rendition') }} as bookplanningmain_bulkorder_rendition
            on main.id = bookplanningmain_bulkorder_rendition.bookplanningmain_bulkorderid
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vacountry                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_bulkorder') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_bulkorder_country') }}     as bookplanningmain_bulkorder_country 
                on main.id = bookplanningmain_bulkorder_country.bookplanningmain_bulkorderid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_bulkorder_country.avatarclient_va_countryid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_bulkorder_country 

            on main.id = sub__bookplanningmain_bulkorder_country.id                      
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vastate                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_bulkorder') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_bulkorder_state') }}     as bookplanningmain_bulkorder_state 
                on main.id = bookplanningmain_bulkorder_state.bookplanningmain_bulkorderid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_bulkorder_state.avatarclient_va_stateid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_bulkorder_state 

            on main.id = sub__bookplanningmain_bulkorder_state.id                      
            