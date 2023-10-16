        

        ------------------------------------------------------------------------------
        -- Jira         :   DEPA - 1351
        -- Source       :   dbt
        -- Project      :   snproject52c6e272
        -- Dataset      :   bookplanning_staging
        -- Path         :   -
        -- Name         :   plagiarismcheck_screeningresponse.sql
        -- Author       :   Jörg Sachsenweger
        -- Date         :   2023-10-10
        -- Update       :   -                   
        --
        -- Requester    :   joerg.zanner@springer.com
        -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
        --
        --------------------------------------------------------------------------    

        Select  main.* 
                ,sub__plagiarismcheck_screeningresponse_feedbackdecision._vafeedbackdecision        

        from {{ source ('bookplaning_cleansing','plagiarismcheck_screeningresponse') }}  as main        
        
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vafeedbackdecision                              

                    from {{ source ('bookplaning_cleansing','plagiarismcheck_screeningresponse') }}     as main

                    left join {{ source ('bookplaning_cleansing','plagiarismcheck_screeningresponse_feedbackdecision') }}     as plagiarismcheck_screeningresponse_feedbackdecision 
                    on main.id = plagiarismcheck_screeningresponse_feedbackdecision.plagiarismcheck_screeningresponseid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  plagiarismcheck_screeningresponse_feedbackdecision.avatarclient_va_final_decisionid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__plagiarismcheck_screeningresponse_feedbackdecision 

                on main.id = sub__plagiarismcheck_screeningresponse_feedbackdecision.id                      
                