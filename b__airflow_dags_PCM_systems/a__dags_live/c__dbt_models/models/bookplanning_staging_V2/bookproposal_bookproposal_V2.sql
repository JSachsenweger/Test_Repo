        

    ------------------------------------------------------------------------------
    -- Jira         :   DEPA - 1351
    -- Source       :   dbt
    -- Project      :   snproject52c6e272
    -- Dataset      :   bookplanning_staging
    -- Path         :   -
    -- Name         :   bookproposal_bookproposal.sql
    -- Author       :   Jörg Sachsenweger
    -- Date         :   2023-10-10
    -- Update       :   -                   
    --
    -- Requester    :   joerg.zanner@springer.com
    -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
    --
    --------------------------------------------------------------------------    
    
    Select  main.* 
            ,bookproposal_bookproposal_edition.bookplanningmain_editionid as fk_bookplanningmain_edition_editionid
            ,sub__bookproposal_bookproposal_va_project_proposal_status._va_project_proposal_status
            ,sub__bookproposal_bookproposal_va_project_type._va_project_type
            ,sub__bookproposal_bookproposal_va_source._va_source        
    
    from {{ source ('bookplaning_cleansing','bookproposal_bookproposal') }}  as main        
    
            left join {{ source ('bookplaning_cleansing','bookproposal_bookproposal_edition') }} as bookproposal_bookproposal_edition
            on main.id = bookproposal_bookproposal_edition.bookproposal_bookproposalid
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _va_project_proposal_status                              
                
                from {{ source ('bookplaning_cleansing','bookproposal_bookproposal') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookproposal_bookproposal_va_project_proposal_status') }}     as bookproposal_bookproposal_va_project_proposal_status 
                on main.id = bookproposal_bookproposal_va_project_proposal_status.bookproposal_bookproposalid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookproposal_bookproposal_va_project_proposal_status.avatarclient_va_project_proposal_statusid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookproposal_bookproposal_va_project_proposal_status 

            on main.id = sub__bookproposal_bookproposal_va_project_proposal_status.id                      
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _va_project_type                              
                
                from {{ source ('bookplaning_cleansing','bookproposal_bookproposal') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookproposal_bookproposal_va_project_type') }}     as bookproposal_bookproposal_va_project_type 
                on main.id = bookproposal_bookproposal_va_project_type.bookproposal_bookproposalid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookproposal_bookproposal_va_project_type.avatarclient_va_project_typeid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookproposal_bookproposal_va_project_type 

            on main.id = sub__bookproposal_bookproposal_va_project_type.id                      
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _va_source                              
                
                from {{ source ('bookplaning_cleansing','bookproposal_bookproposal') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookproposal_bookproposal_va_source') }}     as bookproposal_bookproposal_va_source 
                on main.id = bookproposal_bookproposal_va_source.bookproposal_bookproposalid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookproposal_bookproposal_va_source.avatarclient_va_sourceid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookproposal_bookproposal_va_source 

            on main.id = sub__bookproposal_bookproposal_va_source.id                      
            