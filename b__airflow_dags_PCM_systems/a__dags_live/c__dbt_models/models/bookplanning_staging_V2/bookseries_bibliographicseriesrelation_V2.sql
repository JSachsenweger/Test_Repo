        

        ------------------------------------------------------------------------------
        -- Jira         :   DEPA - 1351
        -- Source       :   dbt
        -- Project      :   snproject52c6e272
        -- Dataset      :   bookplanning_staging
        -- Path         :   -
        -- Name         :   bookseries_bibliographicseriesrelation.sql
        -- Author       :   Jörg Sachsenweger
        -- Date         :   2023-10-10
        -- Update       :   -                   
        --
        -- Requester    :   joerg.zanner@springer.com
        -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
        --
        --------------------------------------------------------------------------    

        Select  main.* 
                ,bookseries_bibliographicseriesrelation_bibliographicseries.bookseries_bibliographicseriesid as fk_bookseries_bibliographicseries_bibliographicseriesid
                ,bookseries_bibliographicseriesrelation_rendition.bookplanningmain_renditionid as fk_bookplanningmain_rendition_renditionid
                ,sub__bookseries_bibliographicseriesrelation_va_volume_name._va_volume_name        

        from {{ source ('bookplaning_cleansing','bookseries_bibliographicseriesrelation') }}  as main        
        
                left join {{ source ('bookplaning_cleansing','bookseries_bibliographicseriesrelation_bibliographicseries') }} as bookseries_bibliographicseriesrelation_bibliographicseries
                on main.id = bookseries_bibliographicseriesrelation_bibliographicseries.bookseries_bibliographicseriesrelationid
                
                left join {{ source ('bookplaning_cleansing','bookseries_bibliographicseriesrelation_rendition') }} as bookseries_bibliographicseriesrelation_rendition
                on main.id = bookseries_bibliographicseriesrelation_rendition.bookseries_bibliographicseriesrelationid
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _va_volume_name                              

                    from {{ source ('bookplaning_cleansing','bookseries_bibliographicseriesrelation') }}     as main

                    left join {{ source ('bookplaning_cleansing','bookseries_bibliographicseriesrelation_va_volume_name') }}     as bookseries_bibliographicseriesrelation_va_volume_name 
                    on main.id = bookseries_bibliographicseriesrelation_va_volume_name.bookseries_bibliographicseriesrelationid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  bookseries_bibliographicseriesrelation_va_volume_name.avatarclient_va_volume_nameid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__bookseries_bibliographicseriesrelation_va_volume_name 

                on main.id = sub__bookseries_bibliographicseriesrelation_va_volume_name.id                      
                