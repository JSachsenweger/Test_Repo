        

    ------------------------------------------------------------------------------
    -- Jira         :   DEPA - 1351
    -- Source       :   dbt
    -- Project      :   snproject52c6e272
    -- Dataset      :   bookplanning_staging
    -- Path         :   -
    -- Name         :   bookplanningmain_event.sql
    -- Author       :   Jörg Sachsenweger
    -- Date         :   2023-10-10
    -- Update       :   -                   
    --
    -- Requester    :   joerg.zanner@springer.com
    -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
    --
    --------------------------------------------------------------------------    
    
    Select  main.* 
            ,bookplanningmain_event_edition.bookplanningmain_editionid as fk_bookplanningmain_edition_editionid
            ,bookplanningmain_event_event.bookplanningmain_eventid2 as fk_bookplanningmain_eventi_eventid
            ,sub__bookplanningmain_event_eventstatus._vaeventstatus
            ,sub__bookplanningmain_event_eventtype._vaeventtype        
    
    from {{ source ('bookplaning_cleansing','bookplanningmain_event') }}  as main        
    
            left join {{ source ('bookplaning_cleansing','bookplanningmain_event_edition') }} as bookplanningmain_event_edition
            on main.id = bookplanningmain_event_edition.bookplanningmain_eventid
            
            left join {{ source ('bookplaning_cleansing','bookplanningmain_event_event') }} as bookplanningmain_event_event
            on main.id = bookplanningmain_event_event.bookplanningmain_eventid1
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vaeventstatus                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_event') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_event_eventstatus') }}     as bookplanningmain_event_eventstatus 
                on main.id = bookplanningmain_event_eventstatus.bookplanningmain_eventid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_event_eventstatus.avatarclient_va_event_statusid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_event_eventstatus 

            on main.id = sub__bookplanningmain_event_eventstatus.id                      
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vaeventtype                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_event') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_event_eventtype') }}     as bookplanningmain_event_eventtype 
                on main.id = bookplanningmain_event_eventtype.bookplanningmain_eventid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_event_eventtype.avatarclient_va_event_typeid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_event_eventtype 

            on main.id = sub__bookplanningmain_event_eventtype.id                      
            