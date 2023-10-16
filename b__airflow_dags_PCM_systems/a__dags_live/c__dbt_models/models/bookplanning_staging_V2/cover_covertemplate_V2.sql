        

        ------------------------------------------------------------------------------
        -- Jira         :   DEPA - 1351
        -- Source       :   dbt
        -- Project      :   snproject52c6e272
        -- Dataset      :   bookplanning_staging
        -- Path         :   -
        -- Name         :   cover_covertemplate.sql
        -- Author       :   Jörg Sachsenweger
        -- Date         :   2023-10-10
        -- Update       :   -                   
        --
        -- Requester    :   joerg.zanner@springer.com
        -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
        --
        --------------------------------------------------------------------------    

        Select  main.* 
                ,cover_covertemplate_bibliographicseriesmain.bookseries_bibliographicseriesid as fk_bookseries_bibliographicseries_bibliographicseriesmainid
                ,cover_covertemplate_covertemplatejpg.cover_covertemplatejpgid as fk_cover_covertemplatejpg_covertemplatejpgid
                ,cover_covertemplate_covertemplatezip.cover_covertemplatezipid as fk_cover_covertemplatezip_covertemplatezipid
                ,sub__cover_covertemplate_actualdesigner._vaactualdesigner
                ,sub__cover_covertemplate_booktype._vabooktype
                ,sub__cover_covertemplate_coverrequesttype._vacoverrequesttype
                ,sub__cover_covertemplate_coversubjectarea._vacoversubjectarea
                ,sub__cover_covertemplate_covertype._vacovertype
                ,sub__cover_covertemplate_designstyle._vadesignstyle
                ,sub__cover_covertemplate_formattrimsize._vaformattrimsize
                ,sub__cover_covertemplate_graphicexception._vagraphicexception
                ,sub__cover_covertemplate_templatetype._vatemplatetype        

        from {{ source ('bookplaning_cleansing','cover_covertemplate') }}  as main        
        
                left join {{ source ('bookplaning_cleansing','cover_covertemplate_bibliographicseriesmain') }} as cover_covertemplate_bibliographicseriesmain
                on main.id = cover_covertemplate_bibliographicseriesmain.cover_covertemplateid
                
                left join {{ source ('bookplaning_cleansing','cover_covertemplate_covertemplatejpg') }} as cover_covertemplate_covertemplatejpg
                on main.id = cover_covertemplate_covertemplatejpg.cover_covertemplateid
                
                left join {{ source ('bookplaning_cleansing','cover_covertemplate_covertemplatezip') }} as cover_covertemplate_covertemplatezip
                on main.id = cover_covertemplate_covertemplatezip.cover_covertemplateid
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vaactualdesigner                              

                    from {{ source ('bookplaning_cleansing','cover_covertemplate') }}     as main

                    left join {{ source ('bookplaning_cleansing','cover_covertemplate_actualdesigner') }}     as cover_covertemplate_actualdesigner 
                    on main.id = cover_covertemplate_actualdesigner.cover_covertemplateid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  cover_covertemplate_actualdesigner.avatarclient_va_actual_designerid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__cover_covertemplate_actualdesigner 

                on main.id = sub__cover_covertemplate_actualdesigner.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vabooktype                              

                    from {{ source ('bookplaning_cleansing','cover_covertemplate') }}     as main

                    left join {{ source ('bookplaning_cleansing','cover_covertemplate_booktype') }}     as cover_covertemplate_booktype 
                    on main.id = cover_covertemplate_booktype.cover_covertemplateid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  cover_covertemplate_booktype.avatarclient_va_book_typeid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__cover_covertemplate_booktype 

                on main.id = sub__cover_covertemplate_booktype.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vacoverrequesttype                              

                    from {{ source ('bookplaning_cleansing','cover_covertemplate') }}     as main

                    left join {{ source ('bookplaning_cleansing','cover_covertemplate_coverrequesttype') }}     as cover_covertemplate_coverrequesttype 
                    on main.id = cover_covertemplate_coverrequesttype.cover_covertemplateid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  cover_covertemplate_coverrequesttype.avatarclient_va_cover_request_typeid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__cover_covertemplate_coverrequesttype 

                on main.id = sub__cover_covertemplate_coverrequesttype.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vacoversubjectarea                              

                    from {{ source ('bookplaning_cleansing','cover_covertemplate') }}     as main

                    left join {{ source ('bookplaning_cleansing','cover_covertemplate_coversubjectarea') }}     as cover_covertemplate_coversubjectarea 
                    on main.id = cover_covertemplate_coversubjectarea.cover_covertemplateid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  cover_covertemplate_coversubjectarea.avatarclient_va_cover_subject_areaid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__cover_covertemplate_coversubjectarea 

                on main.id = sub__cover_covertemplate_coversubjectarea.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vacovertype                              

                    from {{ source ('bookplaning_cleansing','cover_covertemplate') }}     as main

                    left join {{ source ('bookplaning_cleansing','cover_covertemplate_covertype') }}     as cover_covertemplate_covertype 
                    on main.id = cover_covertemplate_covertype.cover_covertemplateid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  cover_covertemplate_covertype.avatarclient_va_cover_typeid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__cover_covertemplate_covertype 

                on main.id = sub__cover_covertemplate_covertype.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vadesignstyle                              

                    from {{ source ('bookplaning_cleansing','cover_covertemplate') }}     as main

                    left join {{ source ('bookplaning_cleansing','cover_covertemplate_designstyle') }}     as cover_covertemplate_designstyle 
                    on main.id = cover_covertemplate_designstyle.cover_covertemplateid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  cover_covertemplate_designstyle.avatarclient_va_design_styleid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__cover_covertemplate_designstyle 

                on main.id = sub__cover_covertemplate_designstyle.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vaformattrimsize                              

                    from {{ source ('bookplaning_cleansing','cover_covertemplate') }}     as main

                    left join {{ source ('bookplaning_cleansing','cover_covertemplate_formattrimsize') }}     as cover_covertemplate_formattrimsize 
                    on main.id = cover_covertemplate_formattrimsize.cover_covertemplateid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  cover_covertemplate_formattrimsize.avatarclient_va_format_trimsizeid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__cover_covertemplate_formattrimsize 

                on main.id = sub__cover_covertemplate_formattrimsize.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vagraphicexception                              

                    from {{ source ('bookplaning_cleansing','cover_covertemplate') }}     as main

                    left join {{ source ('bookplaning_cleansing','cover_covertemplate_graphicexception') }}     as cover_covertemplate_graphicexception 
                    on main.id = cover_covertemplate_graphicexception.cover_covertemplateid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  cover_covertemplate_graphicexception.avatarclient_va_graphic_exceptionid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__cover_covertemplate_graphicexception 

                on main.id = sub__cover_covertemplate_graphicexception.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vatemplatetype                              

                    from {{ source ('bookplaning_cleansing','cover_covertemplate') }}     as main

                    left join {{ source ('bookplaning_cleansing','cover_covertemplate_templatetype') }}     as cover_covertemplate_templatetype 
                    on main.id = cover_covertemplate_templatetype.cover_covertemplateid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  cover_covertemplate_templatetype.avatarclient_va_template_typeid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__cover_covertemplate_templatetype 

                on main.id = sub__cover_covertemplate_templatetype.id                      
                