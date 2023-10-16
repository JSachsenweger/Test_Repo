        

        ------------------------------------------------------------------------------
        -- Jira         :   DEPA - 1351
        -- Source       :   dbt
        -- Project      :   snproject52c6e272
        -- Dataset      :   bookplanning_staging
        -- Path         :   -
        -- Name         :   bookplanningmain_orcid.sql
        -- Author       :   Jörg Sachsenweger
        -- Date         :   2023-10-10
        -- Update       :   -                   
        --
        -- Requester    :   joerg.zanner@springer.com
        -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
        --
        --------------------------------------------------------------------------    

        Select  main.* 
                ,bookplanningmain_orcid_address.addresses_addressid as fk_addresses_address_addressid        

        from {{ source ('bookplaning_cleansing','bookplanningmain_orcid') }}  as main        
        
                left join {{ source ('bookplaning_cleansing','bookplanningmain_orcid_address') }} as bookplanningmain_orcid_address
                on main.id = bookplanningmain_orcid_address.bookplanningmain_orcidid
                