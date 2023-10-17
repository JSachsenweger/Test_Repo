        

    ------------------------------------------------------------------------------
    -- Jira         :   DEPA - 1351
    -- Source       :   dbt
    -- Project      :   snproject52c6e272
    -- Dataset      :   bookplanning_staging
    -- Path         :   -
    -- Name         :   bookplanningmain_attachment.sql
    -- Author       :   Jörg Sachsenweger
    -- Date         :   2023-10-10
    -- Update       :   -                   
    --
    -- Requester    :   joerg.zanner@springer.com
    -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
    --
    --------------------------------------------------------------------------    
    
    Select  main.* 
            ,bookplanningmain_attachment_cover.cover_coverid as fk_cover_cover_coverid
            ,bookplanningmain_attachment_edition.bookplanningmain_editionid as fk_bookplanningmain_edition_editionid
            ,bookplanningmain_attachment_manuscriptsubmission.bookplanningmain_manuscriptsubmissionid as fk_bookplanningmain_manuscriptsubmission_manuscriptsubmissionid
            ,sub__bookplanningmain_attachment_filetype._vafiletype        
    
    from {{ source ('bookplaning_cleansing','bookplanningmain_attachment') }}  as main        
    
            left join {{ source ('bookplaning_cleansing','bookplanningmain_attachment_cover') }} as bookplanningmain_attachment_cover
            on main.id = bookplanningmain_attachment_cover.bookplanningmain_attachmentid
            
            left join {{ source ('bookplaning_cleansing','bookplanningmain_attachment_edition') }} as bookplanningmain_attachment_edition
            on main.id = bookplanningmain_attachment_edition.bookplanningmain_attachmentid
            
            left join {{ source ('bookplaning_cleansing','bookplanningmain_attachment_manuscriptsubmission') }} as bookplanningmain_attachment_manuscriptsubmission
            on main.id = bookplanningmain_attachment_manuscriptsubmission.bookplanningmain_attachmentid
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vafiletype                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_attachment') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_attachment_filetype') }}     as bookplanningmain_attachment_filetype 
                on main.id = bookplanningmain_attachment_filetype.bookplanningmain_attachmentid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_attachment_filetype.avatarclient_va_file_typeid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_attachment_filetype 

            on main.id = sub__bookplanningmain_attachment_filetype.id                      
            