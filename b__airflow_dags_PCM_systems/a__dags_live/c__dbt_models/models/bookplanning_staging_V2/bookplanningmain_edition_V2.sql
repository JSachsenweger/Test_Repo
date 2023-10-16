        

        ------------------------------------------------------------------------------
        -- Jira         :   DEPA - 1351
        -- Source       :   dbt
        -- Project      :   snproject52c6e272
        -- Dataset      :   bookplanning_staging
        -- Path         :   -
        -- Name         :   bookplanningmain_edition.sql
        -- Author       :   Jörg Sachsenweger
        -- Date         :   2023-10-10
        -- Update       :   -                   
        --
        -- Requester    :   joerg.zanner@springer.com
        -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
        --
        --------------------------------------------------------------------------    

        Select  main.* 
                ,bookplanningmain_edition_cover.cover_coverid as fk_cover_cover_coverid
                ,bookplanningmain_edition_marketingtexts.bookplanningmain_marketingtextsid as fk_bookplanningmain_marketingtexts_marketingtextsid
                ,bookplanningmain_edition_publishingsegmentlocal.rgstructure_publishingsegmentlocalid as fk_rgstructure_publishingsegmentlocal_publishingsegmentlocalid
                ,sub__bookplanningmain_edition_abstractindocumentlanguage._vaabstractindocumentlanguage
                ,sub__bookplanningmain_edition_abstractingindexingservice._vaabstractingindexingservice
                ,sub__bookplanningmain_edition_adstatus._vaadstatus
                ,sub__bookplanningmain_edition_bibliographyposition._vabibliographyposition
                ,sub__bookplanningmain_edition_bibliographystyle._vabibliographystyle
                ,sub__bookplanningmain_edition_bodymarkup._vabodymarkup
                ,sub__bookplanningmain_edition_citationstyle._vacitationstyle
                ,sub__bookplanningmain_edition_contentlevel._vacontentlevel
                ,sub__bookplanningmain_edition_copublisher._vacopublisher
                ,sub__bookplanningmain_edition_dynamic._vadynamic
                ,sub__bookplanningmain_edition_ebookformat._vaebookformat
                ,sub__bookplanningmain_edition_ebookpackage._vaebookpackage
                ,sub__bookplanningmain_edition_entrypoint._vaentrypoint
                ,sub__bookplanningmain_edition_fundingdetails._vafundingdetails
                ,sub__bookplanningmain_edition_headinglevels._vaheadinglevels
                ,sub__bookplanningmain_edition_headingnumbering._vaheadingnumbering
                ,sub__bookplanningmain_edition_igocopyrightholder._vaigocopyrightholder
                ,sub__bookplanningmain_edition_imprint._vaimprint
                ,sub__bookplanningmain_edition_keywordsindocumentlanguage._vakeywordsindocumentlanguage
                ,sub__bookplanningmain_edition_language._valanguage
                ,sub__bookplanningmain_edition_machinegeneratedcontent._vamachinegeneratedcontent
                ,sub__bookplanningmain_edition_manuscriptstate._vamanuscriptstate
                ,sub__bookplanningmain_edition_openaccesslicensesubtype._vaopenaccesslicensesubtype
                ,sub__bookplanningmain_edition_openaccesslicenseversion._vaopenaccesslicenseversion
                ,sub__bookplanningmain_edition_originality._vaoriginality
                ,sub__bookplanningmain_edition_originallanguage._vaoriginallanguage
                ,sub__bookplanningmain_edition_productcategory._vaproductcategory
                ,sub__bookplanningmain_edition_productioncategory._vaproductioncategory
                ,sub__bookplanningmain_edition_productionservice._vaproductionservice
                ,sub__bookplanningmain_edition_publicationclass._vapublicationclass
                ,sub__bookplanningmain_edition_publisher._vapublisher
                ,sub__bookplanningmain_edition_saleshighlight._vasaleshighlight
                ,sub__bookplanningmain_edition_secondarylanguage._vasecondarylanguage
                ,sub__bookplanningmain_edition_subjectcollection._vasubjectcollection
                ,sub__bookplanningmain_edition_toclevels._vatoclevels
                ,sub__bookplanningmain_edition_translationmethod._vatranslationmethod
                ,sub__bookplanningmain_edition_typesettinglayout._vatypesettinglayout
                ,sub__bookplanningmain_edition_typesettingprofile._vatypesettingprofile        

        from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}  as main        
        
                left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_cover') }} as bookplanningmain_edition_cover
                on main.id = bookplanningmain_edition_cover.bookplanningmain_editionid
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_marketingtexts') }} as bookplanningmain_edition_marketingtexts
                on main.id = bookplanningmain_edition_marketingtexts.bookplanningmain_editionid
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_publishingsegmentlocal') }} as bookplanningmain_edition_publishingsegmentlocal
                on main.id = bookplanningmain_edition_publishingsegmentlocal.bookplanningmain_editionid
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vaabstractindocumentlanguage                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_abstractindocumentlanguage') }}     as bookplanningmain_edition_abstractindocumentlanguage 
                    on main.id = bookplanningmain_edition_abstractindocumentlanguage.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_abstractindocumentlanguage.avatarclient_va_abstract_in_doc_langid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_abstractindocumentlanguage 

                on main.id = sub__bookplanningmain_edition_abstractindocumentlanguage.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vaabstractingindexingservice                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_abstractingindexingservice') }}     as bookplanningmain_edition_abstractingindexingservice 
                    on main.id = bookplanningmain_edition_abstractingindexingservice.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_abstractingindexingservice.avatarclient_va_abstract_index_srvcid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_abstractingindexingservice 

                on main.id = sub__bookplanningmain_edition_abstractingindexingservice.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vaadstatus                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_adstatus') }}     as bookplanningmain_edition_adstatus 
                    on main.id = bookplanningmain_edition_adstatus.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_adstatus.avatarclient_va_ad_statusid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_adstatus 

                on main.id = sub__bookplanningmain_edition_adstatus.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vabibliographyposition                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_bibliographyposition') }}     as bookplanningmain_edition_bibliographyposition 
                    on main.id = bookplanningmain_edition_bibliographyposition.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_bibliographyposition.avatarclient_va_bibliography_positionid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_bibliographyposition 

                on main.id = sub__bookplanningmain_edition_bibliographyposition.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vabibliographystyle                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_bibliographystyle') }}     as bookplanningmain_edition_bibliographystyle 
                    on main.id = bookplanningmain_edition_bibliographystyle.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_bibliographystyle.avatarclient_va_bibliography_styleid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_bibliographystyle 

                on main.id = sub__bookplanningmain_edition_bibliographystyle.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vabodymarkup                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_bodymarkup') }}     as bookplanningmain_edition_bodymarkup 
                    on main.id = bookplanningmain_edition_bodymarkup.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_bodymarkup.avatarclient_va_body_markupid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_bodymarkup 

                on main.id = sub__bookplanningmain_edition_bodymarkup.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vacitationstyle                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_citationstyle') }}     as bookplanningmain_edition_citationstyle 
                    on main.id = bookplanningmain_edition_citationstyle.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_citationstyle.avatarclient_va_citation_styleid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_citationstyle 

                on main.id = sub__bookplanningmain_edition_citationstyle.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vacontentlevel                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_contentlevel') }}     as bookplanningmain_edition_contentlevel 
                    on main.id = bookplanningmain_edition_contentlevel.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_contentlevel.avatarclient_va_content_levelid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_contentlevel 

                on main.id = sub__bookplanningmain_edition_contentlevel.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vacopublisher                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_copublisher') }}     as bookplanningmain_edition_copublisher 
                    on main.id = bookplanningmain_edition_copublisher.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_copublisher.avatarclient_va_co_publ_nameid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_copublisher 

                on main.id = sub__bookplanningmain_edition_copublisher.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vadynamic                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_dynamic') }}     as bookplanningmain_edition_dynamic 
                    on main.id = bookplanningmain_edition_dynamic.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_dynamic.avatarclient_va_dynamicid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_dynamic 

                on main.id = sub__bookplanningmain_edition_dynamic.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vaebookformat                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_ebookformat') }}     as bookplanningmain_edition_ebookformat 
                    on main.id = bookplanningmain_edition_ebookformat.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_ebookformat.avatarclient_va_ebook_formatid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_ebookformat 

                on main.id = sub__bookplanningmain_edition_ebookformat.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vaebookpackage                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_ebookpackage') }}     as bookplanningmain_edition_ebookpackage 
                    on main.id = bookplanningmain_edition_ebookpackage.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_ebookpackage.avatarclient_va_ebook_packid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_ebookpackage 

                on main.id = sub__bookplanningmain_edition_ebookpackage.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vaentrypoint                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_entrypoint') }}     as bookplanningmain_edition_entrypoint 
                    on main.id = bookplanningmain_edition_entrypoint.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_entrypoint.avatarclient_va_entry_pointid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_entrypoint 

                on main.id = sub__bookplanningmain_edition_entrypoint.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vafundingdetails                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_fundingdetails') }}     as bookplanningmain_edition_fundingdetails 
                    on main.id = bookplanningmain_edition_fundingdetails.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_fundingdetails.avatarclient_va_funding_detailsid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_fundingdetails 

                on main.id = sub__bookplanningmain_edition_fundingdetails.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vaheadinglevels                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_headinglevels') }}     as bookplanningmain_edition_headinglevels 
                    on main.id = bookplanningmain_edition_headinglevels.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_headinglevels.avatarclient_va_heading_levelsid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_headinglevels 

                on main.id = sub__bookplanningmain_edition_headinglevels.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vaheadingnumbering                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_headingnumbering') }}     as bookplanningmain_edition_headingnumbering 
                    on main.id = bookplanningmain_edition_headingnumbering.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_headingnumbering.avatarclient_va_heading_numberingid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_headingnumbering 

                on main.id = sub__bookplanningmain_edition_headingnumbering.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vaigocopyrightholder                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_igocopyrightholder') }}     as bookplanningmain_edition_igocopyrightholder 
                    on main.id = bookplanningmain_edition_igocopyrightholder.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_igocopyrightholder.avatarclient_va_igo_copyright_holderid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_igocopyrightholder 

                on main.id = sub__bookplanningmain_edition_igocopyrightholder.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vaimprint                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_imprint') }}     as bookplanningmain_edition_imprint 
                    on main.id = bookplanningmain_edition_imprint.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_imprint.avatarclient_va_imprintid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_imprint 

                on main.id = sub__bookplanningmain_edition_imprint.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vakeywordsindocumentlanguage                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_keywordsindocumentlanguage') }}     as bookplanningmain_edition_keywordsindocumentlanguage 
                    on main.id = bookplanningmain_edition_keywordsindocumentlanguage.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_keywordsindocumentlanguage.avatarclient_va_keywords_in_doc_langid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_keywordsindocumentlanguage 

                on main.id = sub__bookplanningmain_edition_keywordsindocumentlanguage.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _valanguage                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_language') }}     as bookplanningmain_edition_language 
                    on main.id = bookplanningmain_edition_language.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_language.avatarclient_va_languageid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_language 

                on main.id = sub__bookplanningmain_edition_language.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vamachinegeneratedcontent                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_machinegeneratedcontent') }}     as bookplanningmain_edition_machinegeneratedcontent 
                    on main.id = bookplanningmain_edition_machinegeneratedcontent.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_machinegeneratedcontent.avatarclient_va_machine_gen_contentid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_machinegeneratedcontent 

                on main.id = sub__bookplanningmain_edition_machinegeneratedcontent.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vamanuscriptstate                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_manuscriptstate') }}     as bookplanningmain_edition_manuscriptstate 
                    on main.id = bookplanningmain_edition_manuscriptstate.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_manuscriptstate.avatarclient_va_manuscript_stateid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_manuscriptstate 

                on main.id = sub__bookplanningmain_edition_manuscriptstate.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vaopenaccesslicensesubtype                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_openaccesslicensesubtype') }}     as bookplanningmain_edition_openaccesslicensesubtype 
                    on main.id = bookplanningmain_edition_openaccesslicensesubtype.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_openaccesslicensesubtype.avatarclient_va_oa_license_subtypeid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_openaccesslicensesubtype 

                on main.id = sub__bookplanningmain_edition_openaccesslicensesubtype.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vaopenaccesslicenseversion                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_openaccesslicenseversion') }}     as bookplanningmain_edition_openaccesslicenseversion 
                    on main.id = bookplanningmain_edition_openaccesslicenseversion.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_openaccesslicenseversion.avatarclient_va_oa_license_versionid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_openaccesslicenseversion 

                on main.id = sub__bookplanningmain_edition_openaccesslicenseversion.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vaoriginality                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_originality') }}     as bookplanningmain_edition_originality 
                    on main.id = bookplanningmain_edition_originality.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_originality.avatarclient_va_originalityid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_originality 

                on main.id = sub__bookplanningmain_edition_originality.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vaoriginallanguage                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_originallanguage') }}     as bookplanningmain_edition_originallanguage 
                    on main.id = bookplanningmain_edition_originallanguage.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_originallanguage.avatarclient_va_languageid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_originallanguage 

                on main.id = sub__bookplanningmain_edition_originallanguage.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vaproductcategory                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_productcategory') }}     as bookplanningmain_edition_productcategory 
                    on main.id = bookplanningmain_edition_productcategory.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_productcategory.avatarclient_va_product_categoryid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_productcategory 

                on main.id = sub__bookplanningmain_edition_productcategory.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vaproductioncategory                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_productioncategory') }}     as bookplanningmain_edition_productioncategory 
                    on main.id = bookplanningmain_edition_productioncategory.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_productioncategory.avatarclient_va_production_categoryid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_productioncategory 

                on main.id = sub__bookplanningmain_edition_productioncategory.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vaproductionservice                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_productionservice') }}     as bookplanningmain_edition_productionservice 
                    on main.id = bookplanningmain_edition_productionservice.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_productionservice.avatarclient_va_production_serviceid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_productionservice 

                on main.id = sub__bookplanningmain_edition_productionservice.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vapublicationclass                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_publicationclass') }}     as bookplanningmain_edition_publicationclass 
                    on main.id = bookplanningmain_edition_publicationclass.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_publicationclass.avatarclient_va_publication_classid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_publicationclass 

                on main.id = sub__bookplanningmain_edition_publicationclass.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vapublisher                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_publisher') }}     as bookplanningmain_edition_publisher 
                    on main.id = bookplanningmain_edition_publisher.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_publisher.avatarclient_va_publisherid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_publisher 

                on main.id = sub__bookplanningmain_edition_publisher.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vasaleshighlight                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_saleshighlight') }}     as bookplanningmain_edition_saleshighlight 
                    on main.id = bookplanningmain_edition_saleshighlight.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_saleshighlight.avatarclient_va_sales_highlightid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_saleshighlight 

                on main.id = sub__bookplanningmain_edition_saleshighlight.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vasecondarylanguage                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_secondarylanguage') }}     as bookplanningmain_edition_secondarylanguage 
                    on main.id = bookplanningmain_edition_secondarylanguage.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_secondarylanguage.avatarclient_va_languageid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_secondarylanguage 

                on main.id = sub__bookplanningmain_edition_secondarylanguage.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vasubjectcollection                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_subjectcollection') }}     as bookplanningmain_edition_subjectcollection 
                    on main.id = bookplanningmain_edition_subjectcollection.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_subjectcollection.avatarclient_va_subject_collectionid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_subjectcollection 

                on main.id = sub__bookplanningmain_edition_subjectcollection.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vatoclevels                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_toclevels') }}     as bookplanningmain_edition_toclevels 
                    on main.id = bookplanningmain_edition_toclevels.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_toclevels.avatarclient_va_toc_levelsid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_toclevels 

                on main.id = sub__bookplanningmain_edition_toclevels.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vatranslationmethod                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_translationmethod') }}     as bookplanningmain_edition_translationmethod 
                    on main.id = bookplanningmain_edition_translationmethod.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_translationmethod.avatarclient_va_translation_methodid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_translationmethod 

                on main.id = sub__bookplanningmain_edition_translationmethod.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vatypesettinglayout                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_typesettinglayout') }}     as bookplanningmain_edition_typesettinglayout 
                    on main.id = bookplanningmain_edition_typesettinglayout.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_typesettinglayout.avatarclient_va_typesetting_layoutid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_typesettinglayout 

                on main.id = sub__bookplanningmain_edition_typesettinglayout.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vatypesettingprofile                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_edition') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_edition_typesettingprofile') }}     as bookplanningmain_edition_typesettingprofile 
                    on main.id = bookplanningmain_edition_typesettingprofile.bookplanningmain_editionid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_edition_typesettingprofile.avatarclient_va_typesetting_profileid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_edition_typesettingprofile 

                on main.id = sub__bookplanningmain_edition_typesettingprofile.id                      
                