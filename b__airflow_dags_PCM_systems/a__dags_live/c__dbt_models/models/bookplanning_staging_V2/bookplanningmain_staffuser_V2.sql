        

        ------------------------------------------------------------------------------
        -- Jira         :   DEPA - 1351
        -- Source       :   dbt
        -- Project      :   snproject52c6e272
        -- Dataset      :   bookplanning_staging
        -- Path         :   -
        -- Name         :   bookplanningmain_staffuser.sql
        -- Author       :   Jörg Sachsenweger
        -- Date         :   2023-10-10
        -- Update       :   -                   
        --
        -- Requester    :   joerg.zanner@springer.com
        -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
        --
        --------------------------------------------------------------------------    

        Select  main.* 
                ,bookplanningmain_staffuser_aduser.snsaml_aduserid as fk_snsaml_aduser_aduserid
                ,bookplanningmain_staffuser_edition.bookplanningmain_editionid as fk_bookplanningmain_edition_editionid
                ,sub__bookplanningmain_staffuser_stafftype._vastafftype        

        from {{ source ('bookplaning_cleansing','bookplanningmain_staffuser') }}  as main        
        
                left join {{ source ('bookplaning_cleansing','bookplanningmain_staffuser_aduser') }} as bookplanningmain_staffuser_aduser
                on main.id = bookplanningmain_staffuser_aduser.bookplanningmain_staffuserid
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_staffuser_edition') }} as bookplanningmain_staffuser_edition
                on main.id = bookplanningmain_staffuser_edition.bookplanningmain_staffuserid
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vastafftype                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_staffuser') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_staffuser_stafftype') }}     as bookplanningmain_staffuser_stafftype 
                    on main.id = bookplanningmain_staffuser_stafftype.bookplanningmain_staffuserid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_staffuser_stafftype.avatarclient_va_col_inhouse_staffid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_staffuser_stafftype 

                on main.id = sub__bookplanningmain_staffuser_stafftype.id                      
                