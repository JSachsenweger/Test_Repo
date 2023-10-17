        

    ------------------------------------------------------------------------------
    -- Jira         :   DEPA - 1351
    -- Source       :   dbt
    -- Project      :   snproject52c6e272
    -- Dataset      :   bookplanning_staging
    -- Path         :   -
    -- Name         :   bookplanningmain_affiliation.sql
    -- Author       :   Jörg Sachsenweger
    -- Date         :   2023-10-10
    -- Update       :   -                   
    --
    -- Requester    :   joerg.zanner@springer.com
    -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
    --
    --------------------------------------------------------------------------    
    
    Select  main.* 
            ,bookplanningmain_affiliation_address.addresses_addressid as fk_addresses_address_addressid
            ,bookplanningmain_affiliation_originator.bookplanningmain_originatorid as fk_bookplanningmain_originator_originatorid
            ,sub__bookplanningmain_affiliation_country._vacountry
            ,sub__bookplanningmain_affiliation_state._vastate        
    
    from {{ source ('bookplaning_cleansing','bookplanningmain_affiliation') }}  as main        
    
            left join {{ source ('bookplaning_cleansing','bookplanningmain_affiliation_address') }} as bookplanningmain_affiliation_address
            on main.id = bookplanningmain_affiliation_address.bookplanningmain_affiliationid
            
            left join {{ source ('bookplaning_cleansing','bookplanningmain_affiliation_originator') }} as bookplanningmain_affiliation_originator
            on main.id = bookplanningmain_affiliation_originator.bookplanningmain_affiliationid
            
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
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_affiliation') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_affiliation_country') }}     as bookplanningmain_affiliation_country 
                on main.id = bookplanningmain_affiliation_country.bookplanningmain_affiliationid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_affiliation_country.avatarclient_va_countryid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_affiliation_country 

            on main.id = sub__bookplanningmain_affiliation_country.id                      
            
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
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_affiliation') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_affiliation_state') }}     as bookplanningmain_affiliation_state 
                on main.id = bookplanningmain_affiliation_state.bookplanningmain_affiliationid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_affiliation_state.avatarclient_va_stateid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_affiliation_state 

            on main.id = sub__bookplanningmain_affiliation_state.id                      
            