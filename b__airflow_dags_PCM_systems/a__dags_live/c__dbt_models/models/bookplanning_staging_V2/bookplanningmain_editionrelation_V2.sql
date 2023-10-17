        

    ------------------------------------------------------------------------------
    -- Jira         :   DEPA - 1351
    -- Source       :   dbt
    -- Project      :   snproject52c6e272
    -- Dataset      :   bookplanning_staging
    -- Path         :   -
    -- Name         :   bookplanningmain_editionrelation.sql
    -- Author       :   Jörg Sachsenweger
    -- Date         :   2023-10-10
    -- Update       :   -                   
    --
    -- Requester    :   joerg.zanner@springer.com
    -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
    --
    --------------------------------------------------------------------------    
    
    Select  main.* 
            ,bookplanningmain_editionrelation_childedition.bookplanningmain_editionid as fk_bookplanningmain_edition_childeditionid
            ,bookplanningmain_editionrelation_parentedition.bookplanningmain_editionid as fk_bookplanningmain_edition_parenteditionid
            ,sub__bookplanningmain_editionrelation_editionrelationtype._vaeditionrelationtype        
    
    from {{ source ('bookplaning_cleansing','bookplanningmain_editionrelation') }}  as main        
    
            left join {{ source ('bookplaning_cleansing','bookplanningmain_editionrelation_childedition') }} as bookplanningmain_editionrelation_childedition
            on main.id = bookplanningmain_editionrelation_childedition.bookplanningmain_editionrelationid
            
            left join {{ source ('bookplaning_cleansing','bookplanningmain_editionrelation_parentedition') }} as bookplanningmain_editionrelation_parentedition
            on main.id = bookplanningmain_editionrelation_parentedition.bookplanningmain_editionrelationid
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vaeditionrelationtype                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_editionrelation') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_editionrelation_editionrelationtype') }}     as bookplanningmain_editionrelation_editionrelationtype 
                on main.id = bookplanningmain_editionrelation_editionrelationtype.bookplanningmain_editionrelationid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_editionrelation_editionrelationtype.avatarclient_va_edition_relation_typeid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_editionrelation_editionrelationtype 

            on main.id = sub__bookplanningmain_editionrelation_editionrelationtype.id                      
            