        

        ------------------------------------------------------------------------------
        -- Jira         :   DEPA - 1351
        -- Source       :   dbt
        -- Project      :   snproject52c6e272
        -- Dataset      :   bookplanning_staging
        -- Path         :   -
        -- Name         :   bookplanningmain_internalcomment.sql
        -- Author       :   Jörg Sachsenweger
        -- Date         :   2023-10-10
        -- Update       :   -                   
        --
        -- Requester    :   joerg.zanner@springer.com
        -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
        --
        --------------------------------------------------------------------------    

        Select  main.* 
                ,bookplanningmain_internalcomment_edition.bookplanningmain_editionid as fk_bookplanningmain_edition_editionid
                ,bookplanningmain_internalcomment_user.system_userid as fk_system_user_userid        

        from {{ source ('bookplaning_cleansing','bookplanningmain_internalcomment') }}  as main        
        
                left join {{ source ('bookplaning_cleansing','bookplanningmain_internalcomment_edition') }} as bookplanningmain_internalcomment_edition
                on main.id = bookplanningmain_internalcomment_edition.bookplanningmain_internalcommentid
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_internalcomment_user') }} as bookplanningmain_internalcomment_user
                on main.id = bookplanningmain_internalcomment_user.bookplanningmain_internalcommentid
                