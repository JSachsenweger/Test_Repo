        

    ------------------------------------------------------------------------------
    -- Jira         :   DEPA - 1351
    -- Source       :   dbt
    -- Project      :   snproject52c6e272
    -- Dataset      :   bookplanning_staging
    -- Path         :   -
    -- Name         :   bookplanningmain_share.sql
    -- Author       :   Jörg Sachsenweger
    -- Date         :   2023-10-10
    -- Update       :   -                   
    --
    -- Requester    :   joerg.zanner@springer.com
    -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
    --
    --------------------------------------------------------------------------    
    
    Select  main.* 
            ,bookplanningmain_share_printingdata.bookplanningmain_printingdataid as fk_bookplanningmain_printingdata_printingdataid
            ,sub__bookplanningmain_share_sharestatus._vasharestatus
            ,sub__bookplanningmain_share_sharetype._vasharetype        
    
    from {{ source ('bookplaning_cleansing','bookplanningmain_share') }}  as main        
    
            left join {{ source ('bookplaning_cleansing','bookplanningmain_share_printingdata') }} as bookplanningmain_share_printingdata
            on main.id = bookplanningmain_share_printingdata.bookplanningmain_shareid
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vasharestatus                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_share') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_share_sharestatus') }}     as bookplanningmain_share_sharestatus 
                on main.id = bookplanningmain_share_sharestatus.bookplanningmain_shareid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_share_sharestatus.avatarclient_va_share_statusid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_share_sharestatus 

            on main.id = sub__bookplanningmain_share_sharestatus.id                      
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vasharetype                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_share') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_share_sharetype') }}     as bookplanningmain_share_sharetype 
                on main.id = bookplanningmain_share_sharetype.bookplanningmain_shareid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_share_sharetype.avatarclient_va_share_typeid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_share_sharetype 

            on main.id = sub__bookplanningmain_share_sharetype.id                      
            