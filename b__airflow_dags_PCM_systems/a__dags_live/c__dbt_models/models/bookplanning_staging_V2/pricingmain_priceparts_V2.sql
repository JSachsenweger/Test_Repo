        

        ------------------------------------------------------------------------------
        -- Jira         :   DEPA - 1351
        -- Source       :   dbt
        -- Project      :   snproject52c6e272
        -- Dataset      :   bookplanning_staging
        -- Path         :   -
        -- Name         :   pricingmain_priceparts.sql
        -- Author       :   Jörg Sachsenweger
        -- Date         :   2023-10-10
        -- Update       :   -                   
        --
        -- Requester    :   joerg.zanner@springer.com
        -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
        --
        --------------------------------------------------------------------------    

        Select  main.* 
                ,pricingmain_priceparts_price.pricingmain_priceid as fk_pricingmain_price_priceid        

        from {{ source ('bookplaning_cleansing','pricingmain_priceparts') }}  as main        
        
                left join {{ source ('bookplaning_cleansing','pricingmain_priceparts_price') }} as pricingmain_priceparts_price
                on main.id = pricingmain_priceparts_price.pricingmain_pricepartsid
                