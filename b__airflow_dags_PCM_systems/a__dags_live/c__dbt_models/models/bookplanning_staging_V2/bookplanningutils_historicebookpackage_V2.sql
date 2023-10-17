        

    ------------------------------------------------------------------------------
    -- Jira         :   DEPA - 1351
    -- Source       :   dbt
    -- Project      :   snproject52c6e272
    -- Dataset      :   bookplanning_staging
    -- Path         :   -
    -- Name         :   bookplanningutils_historicebookpackage.sql
    -- Author       :   Jörg Sachsenweger
    -- Date         :   2023-10-10
    -- Update       :   -                   
    --
    -- Requester    :   joerg.zanner@springer.com
    -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
    --
    --------------------------------------------------------------------------    
    
    Select  main.* 
            ,bookplanningutils_historicebookpackage_edition.bookplanningmain_editionid as fk_bookplanningmain_edition_editionid
            ,sub__bookplanningutils_historicebookpackage_va_ebook_pack._va_ebook_pack        
    
    from {{ source ('bookplaning_cleansing','bookplanningutils_historicebookpackage') }}  as main        
    
            left join {{ source ('bookplaning_cleansing','bookplanningutils_historicebookpackage_edition') }} as bookplanningutils_historicebookpackage_edition
            on main.id = bookplanningutils_historicebookpackage_edition.bookplanningutils_historicebookpackageid
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _va_ebook_pack                              
                
                from {{ source ('bookplaning_cleansing','bookplanningutils_historicebookpackage') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningutils_historicebookpackage_va_ebook_pack') }}     as bookplanningutils_historicebookpackage_va_ebook_pack 
                on main.id = bookplanningutils_historicebookpackage_va_ebook_pack.bookplanningutils_historicebookpackageid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningutils_historicebookpackage_va_ebook_pack.avatarclient_va_ebook_packid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningutils_historicebookpackage_va_ebook_pack 

            on main.id = sub__bookplanningutils_historicebookpackage_va_ebook_pack.id                      
            