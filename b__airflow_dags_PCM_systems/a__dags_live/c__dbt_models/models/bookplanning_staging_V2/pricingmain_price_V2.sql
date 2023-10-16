        

        ------------------------------------------------------------------------------
        -- Jira         :   DEPA - 1351
        -- Source       :   dbt
        -- Project      :   snproject52c6e272
        -- Dataset      :   bookplanning_staging
        -- Path         :   -
        -- Name         :   pricingmain_price.sql
        -- Author       :   Jörg Sachsenweger
        -- Date         :   2023-10-10
        -- Update       :   -                   
        --
        -- Requester    :   joerg.zanner@springer.com
        -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
        --
        --------------------------------------------------------------------------    

        Select  main.* 
                ,pricingmain_price_calcprice.pricingmain_calcpriceid as fk_pricingmain_calcprice_calcpriceid
                ,pricingmain_price_niceprice.pricingmain_nicepriceid as fk_pricingmain_niceprice_nicepriceid
                ,pricingmain_price_product.pricingmain_productid as fk_pricingmain_product_productid
                ,pricingmain_price_rendition.bookplanningmain_renditionid as fk_bookplanningmain_rendition_renditionid
                ,sub__pricingmain_price_vabasecountry._vabasecountry
                ,sub__pricingmain_price_vabasecurrency._vabasecurrency
                ,sub__pricingmain_price_vacountry._vacountry
                ,sub__pricingmain_price_vacurrency._vacurrency
                ,sub__pricingmain_price_vapricetype._vapricetype        

        from {{ source ('bookplaning_cleansing','pricingmain_price') }}  as main        
        
                left join {{ source ('bookplaning_cleansing','pricingmain_price_calcprice') }} as pricingmain_price_calcprice
                on main.id = pricingmain_price_calcprice.pricingmain_priceid
                
                left join {{ source ('bookplaning_cleansing','pricingmain_price_niceprice') }} as pricingmain_price_niceprice
                on main.id = pricingmain_price_niceprice.pricingmain_priceid
                
                left join {{ source ('bookplaning_cleansing','pricingmain_price_product') }} as pricingmain_price_product
                on main.id = pricingmain_price_product.pricingmain_priceid
                
                left join {{ source ('bookplaning_cleansing','pricingmain_price_rendition') }} as pricingmain_price_rendition
                on main.id = pricingmain_price_rendition.pricingmain_priceid
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(va_va.code IGNORE NULLS) as code,
                                        array_agg(va_va.term IGNORE NULLS) as term,
                                        string_agg(va_va.code) as code_str,
                                        string_agg(va_va.term) as term_str
                                    ) as _vabasecountry                              

                    from {{ source ('bookplaning_cleansing','pricingmain_price') }}     as main

                    left join {{ source ('bookplaning_cleansing','pricingmain_price_vabasecountry') }}     as pricingmain_price_vabasecountry 
                    on main.id = pricingmain_price_vabasecountry.pricingmain_priceid
                    
                    left join {{ source ('bookplaning_cleansing','va_va') }}    as va_va
                    on  pricingmain_price_vabasecountry.va_vacountryid = va_va.id

                    group by 
                    main.id

                )   sub__pricingmain_price_vabasecountry 

                on main.id = sub__pricingmain_price_vabasecountry.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(va_va.code IGNORE NULLS) as code,
                                        array_agg(va_va.term IGNORE NULLS) as term,
                                        string_agg(va_va.code) as code_str,
                                        string_agg(va_va.term) as term_str
                                    ) as _vabasecurrency                              

                    from {{ source ('bookplaning_cleansing','pricingmain_price') }}     as main

                    left join {{ source ('bookplaning_cleansing','pricingmain_price_vabasecurrency') }}     as pricingmain_price_vabasecurrency 
                    on main.id = pricingmain_price_vabasecurrency.pricingmain_priceid
                    
                    left join {{ source ('bookplaning_cleansing','va_va') }}    as va_va
                    on  pricingmain_price_vabasecurrency.va_vacurrencyid = va_va.id

                    group by 
                    main.id

                )   sub__pricingmain_price_vabasecurrency 

                on main.id = sub__pricingmain_price_vabasecurrency.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(va_va.code IGNORE NULLS) as code,
                                        array_agg(va_va.term IGNORE NULLS) as term,
                                        string_agg(va_va.code) as code_str,
                                        string_agg(va_va.term) as term_str
                                    ) as _vacountry                              

                    from {{ source ('bookplaning_cleansing','pricingmain_price') }}     as main

                    left join {{ source ('bookplaning_cleansing','pricingmain_price_vacountry') }}     as pricingmain_price_vacountry 
                    on main.id = pricingmain_price_vacountry.pricingmain_priceid
                    
                    left join {{ source ('bookplaning_cleansing','va_va') }}    as va_va
                    on  pricingmain_price_vacountry.va_vacountryid = va_va.id

                    group by 
                    main.id

                )   sub__pricingmain_price_vacountry 

                on main.id = sub__pricingmain_price_vacountry.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(va_va.code IGNORE NULLS) as code,
                                        array_agg(va_va.term IGNORE NULLS) as term,
                                        string_agg(va_va.code) as code_str,
                                        string_agg(va_va.term) as term_str
                                    ) as _vacurrency                              

                    from {{ source ('bookplaning_cleansing','pricingmain_price') }}     as main

                    left join {{ source ('bookplaning_cleansing','pricingmain_price_vacurrency') }}     as pricingmain_price_vacurrency 
                    on main.id = pricingmain_price_vacurrency.pricingmain_priceid
                    
                    left join {{ source ('bookplaning_cleansing','va_va') }}    as va_va
                    on  pricingmain_price_vacurrency.va_vacurrencyid = va_va.id

                    group by 
                    main.id

                )   sub__pricingmain_price_vacurrency 

                on main.id = sub__pricingmain_price_vacurrency.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(va_va.code IGNORE NULLS) as code,
                                        array_agg(va_va.term IGNORE NULLS) as term,
                                        string_agg(va_va.code) as code_str,
                                        string_agg(va_va.term) as term_str
                                    ) as _vapricetype                              

                    from {{ source ('bookplaning_cleansing','pricingmain_price') }}     as main

                    left join {{ source ('bookplaning_cleansing','pricingmain_price_vapricetype') }}     as pricingmain_price_vapricetype 
                    on main.id = pricingmain_price_vapricetype.pricingmain_priceid
                    
                    left join {{ source ('bookplaning_cleansing','va_va') }}    as va_va
                    on  pricingmain_price_vapricetype.va_vapricetypeid = va_va.id

                    group by 
                    main.id

                )   sub__pricingmain_price_vapricetype 

                on main.id = sub__pricingmain_price_vapricetype.id                      
                