        

    ------------------------------------------------------------------------------
    -- Jira         :   DEPA - 1351
    -- Source       :   dbt
    -- Project      :   snproject52c6e272
    -- Dataset      :   bookplanning_staging
    -- Path         :   -
    -- Name         :   bookplanningmain_originator.sql
    -- Author       :   Jörg Sachsenweger
    -- Date         :   2023-10-10
    -- Update       :   -                   
    --
    -- Requester    :   joerg.zanner@springer.com
    -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
    --
    --------------------------------------------------------------------------    
    
    Select  main.* 
            ,bookplanningmain_originator_address.addresses_addressid as fk_addresses_address_addressid
            ,bookplanningmain_originator_edition.bookplanningmain_editionid as fk_bookplanningmain_edition_editionid
            ,sub__bookplanningmain_originator_governmentemployeetype._vagovernmentemployeetype
            ,sub__bookplanningmain_originator_originatortype._vaoriginatortype        
    
    from {{ source ('bookplaning_cleansing','bookplanningmain_originator') }}  as main        
    
            left join {{ source ('bookplaning_cleansing','bookplanningmain_originator_address') }} as bookplanningmain_originator_address
            on main.id = bookplanningmain_originator_address.bookplanningmain_originatorid
            
            left join {{ source ('bookplaning_cleansing','bookplanningmain_originator_edition') }} as bookplanningmain_originator_edition
            on main.id = bookplanningmain_originator_edition.bookplanningmain_originatorid
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vagovernmentemployeetype                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_originator') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_originator_governmentemployeetype') }}     as bookplanningmain_originator_governmentemployeetype 
                on main.id = bookplanningmain_originator_governmentemployeetype.bookplanningmain_originatorid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_originator_governmentemployeetype.avatarclient_va_ctr_gov_emp_typeid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_originator_governmentemployeetype 

            on main.id = sub__bookplanningmain_originator_governmentemployeetype.id                      
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vaoriginatortype                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_originator') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_originator_originatortype') }}     as bookplanningmain_originator_originatortype 
                on main.id = bookplanningmain_originator_originatortype.bookplanningmain_originatorid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_originator_originatortype.avatarclient_va_col_originatorid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_originator_originatortype 

            on main.id = sub__bookplanningmain_originator_originatortype.id                      
            