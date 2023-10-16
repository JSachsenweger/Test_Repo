        

        ------------------------------------------------------------------------------
        -- Jira         :   DEPA - 1351
        -- Source       :   dbt
        -- Project      :   snproject52c6e272
        -- Dataset      :   bookplanning_staging
        -- Path         :   -
        -- Name         :   bookplanningmain_isbnrange.sql
        -- Author       :   Jörg Sachsenweger
        -- Date         :   2023-10-10
        -- Update       :   -                   
        --
        -- Requester    :   joerg.zanner@springer.com
        -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
        --
        --------------------------------------------------------------------------    

        Select  main.* 
                ,sub__bookplanningmain_isbnrange_isbnrangestatus._vaisbnrangestatus
                ,sub__bookplanningmain_isbnrange_publisher._vapublisher        

        from {{ source ('bookplaning_cleansing','bookplanningmain_isbnrange') }}  as main        
        
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vaisbnrangestatus                              

                    from {{ source ('bookplaning_cleansing','bookplanningmain_isbnrange') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_isbnrange_isbnrangestatus') }}     as bookplanningmain_isbnrange_isbnrangestatus 
                    on main.id = bookplanningmain_isbnrange_isbnrangestatus.bookplanningmain_isbnrangerelationid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_isbnrange_isbnrangestatus.avatarclient_va_isbn_range_statusid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_isbnrange_isbnrangestatus 

                on main.id = sub__bookplanningmain_isbnrange_isbnrangestatus.id                      
                
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

                    from {{ source ('bookplaning_cleansing','bookplanningmain_isbnrange') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookplanningmain_isbnrange_publisher') }}     as bookplanningmain_isbnrange_publisher 
                    on main.id = bookplanningmain_isbnrange_publisher.bookplanningmain_isbnrangerelationid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookplanningmain_isbnrange_publisher.avatarclient_va_publisherid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookplanningmain_isbnrange_publisher 

                on main.id = sub__bookplanningmain_isbnrange_publisher.id                      
                