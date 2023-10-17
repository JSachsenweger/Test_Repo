        

    ------------------------------------------------------------------------------
    -- Jira         :   DEPA - 1351
    -- Source       :   dbt
    -- Project      :   snproject52c6e272
    -- Dataset      :   bookplanning_staging
    -- Path         :   -
    -- Name         :   pricingmain_calcprice.sql
    -- Author       :   Jörg Sachsenweger
    -- Date         :   2023-10-10
    -- Update       :   -                   
    --
    -- Requester    :   joerg.zanner@springer.com
    -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
    --
    --------------------------------------------------------------------------    
    
    Select  main.* 
            ,pricingmain_calcprice_bookproject.pricingmain_bookprojectid as fk_pricingmain_bookproject_bookprojectid
            ,pricingmain_calcprice_product.pricingmain_productid as fk_pricingmain_product_productid
            ,sub__pricingmain_calcprice_vabasecountry._vabasecountry
            ,sub__pricingmain_calcprice_vabasecurrency._vabasecurrency
            ,sub__pricingmain_calcprice_vamedium._vamedium
            ,sub__pricingmain_calcprice_vapricetype._vapricetype        
    
    from {{ source ('bookplaning_cleansing','pricingmain_calcprice') }}  as main        
    
            left join {{ source ('bookplaning_cleansing','pricingmain_calcprice_bookproject') }} as pricingmain_calcprice_bookproject
            on main.id = pricingmain_calcprice_bookproject.pricingmain_calcpriceid
            
            left join {{ source ('bookplaning_cleansing','pricingmain_calcprice_product') }} as pricingmain_calcprice_product
            on main.id = pricingmain_calcprice_product.pricingmain_calcpriceid
            
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
                
                from {{ source ('bookplaning_cleansing','pricingmain_calcprice') }}     as main
                
                left join {{ source ('bookplaning_cleansing','pricingmain_calcprice_vabasecountry') }}     as pricingmain_calcprice_vabasecountry 
                on main.id = pricingmain_calcprice_vabasecountry.pricingmain_calcpriceid
                
                left join {{ source ('bookplaning_cleansing','va_va') }}    as va_va
                on  pricingmain_calcprice_vabasecountry.va_vacountryid = va_va.id

                group by 
                main.id

            )   sub__pricingmain_calcprice_vabasecountry 

            on main.id = sub__pricingmain_calcprice_vabasecountry.id                      
            
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
                
                from {{ source ('bookplaning_cleansing','pricingmain_calcprice') }}     as main
                
                left join {{ source ('bookplaning_cleansing','pricingmain_calcprice_vabasecurrency') }}     as pricingmain_calcprice_vabasecurrency 
                on main.id = pricingmain_calcprice_vabasecurrency.pricingmain_calcpriceid
                
                left join {{ source ('bookplaning_cleansing','va_va') }}    as va_va
                on  pricingmain_calcprice_vabasecurrency.va_vacurrencyid = va_va.id

                group by 
                main.id

            )   sub__pricingmain_calcprice_vabasecurrency 

            on main.id = sub__pricingmain_calcprice_vabasecurrency.id                      
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(va_va.code IGNORE NULLS) as code,
                                    array_agg(va_va.term IGNORE NULLS) as term,
                                    string_agg(va_va.code) as code_str,
                                    string_agg(va_va.term) as term_str
                                ) as _vamedium                              
                
                from {{ source ('bookplaning_cleansing','pricingmain_calcprice') }}     as main
                
                left join {{ source ('bookplaning_cleansing','pricingmain_calcprice_vamedium') }}     as pricingmain_calcprice_vamedium 
                on main.id = pricingmain_calcprice_vamedium.pricingmain_calcpriceid
                
                left join {{ source ('bookplaning_cleansing','va_va') }}    as va_va
                on  pricingmain_calcprice_vamedium.va_vamediumid = va_va.id

                group by 
                main.id

            )   sub__pricingmain_calcprice_vamedium 

            on main.id = sub__pricingmain_calcprice_vamedium.id                      
            
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
                
                from {{ source ('bookplaning_cleansing','pricingmain_calcprice') }}     as main
                
                left join {{ source ('bookplaning_cleansing','pricingmain_calcprice_vapricetype') }}     as pricingmain_calcprice_vapricetype 
                on main.id = pricingmain_calcprice_vapricetype.pricingmain_calcpriceid
                
                left join {{ source ('bookplaning_cleansing','va_va') }}    as va_va
                on  pricingmain_calcprice_vapricetype.va_vapricetypeid = va_va.id

                group by 
                main.id

            )   sub__pricingmain_calcprice_vapricetype 

            on main.id = sub__pricingmain_calcprice_vapricetype.id                      
            