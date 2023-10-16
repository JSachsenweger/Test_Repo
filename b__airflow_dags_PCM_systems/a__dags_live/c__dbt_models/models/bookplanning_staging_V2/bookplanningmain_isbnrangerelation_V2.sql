        

        ------------------------------------------------------------------------------
        -- Jira         :   DEPA - 1351
        -- Source       :   dbt
        -- Project      :   snproject52c6e272
        -- Dataset      :   bookplanning_staging
        -- Path         :   -
        -- Name         :   bookplanningmain_isbnrangerelation.sql
        -- Author       :   Jörg Sachsenweger
        -- Date         :   2023-10-10
        -- Update       :   -                   
        --
        -- Requester    :   joerg.zanner@springer.com
        -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
        --
        --------------------------------------------------------------------------    

        Select  main.* 
                ,bookplanningmain_isbnrangerelation_isbnrange.bookplanningmain_isbnrangeid as fk_bookplanningmain_isbnrange_isbnrangeid        

        from {{ source ('bookplaning_cleansing','bookplanningmain_isbnrangerelation') }}  as main        
        
                left join {{ source ('bookplaning_cleansing','bookplanningmain_isbnrangerelation_isbnrange') }} as bookplanningmain_isbnrangerelation_isbnrange
                on main.id = bookplanningmain_isbnrangerelation_isbnrange.bookplanningmain_isbnrangerelationid
                