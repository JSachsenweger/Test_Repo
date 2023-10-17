        

    ------------------------------------------------------------------------------
    -- Jira         :   DEPA - 1351
    -- Source       :   dbt
    -- Project      :   snproject52c6e272
    -- Dataset      :   bookplanning_staging
    -- Path         :   -
    -- Name         :   bookplanningmain_rendition.sql
    -- Author       :   Jörg Sachsenweger
    -- Date         :   2023-10-10
    -- Update       :   -                   
    --
    -- Requester    :   joerg.zanner@springer.com
    -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
    --
    --------------------------------------------------------------------------    
    
    Select  main.* 
            ,bookplanningmain_rendition_edition.bookplanningmain_editionid as fk_bookplanningmain_edition_editionid
            ,bookplanningmain_rendition_offsetprintingdata.bookplanningmain_offsetprintingdataid as fk_bookplanningmain_offsetprintingdata_offsetprintingdataid
            ,bookplanningmain_rendition_printingdata.bookplanningmain_printingdataid as fk_bookplanningmain_printingdata_printingdataid
            ,sub__bookplanningmain_rendition_announcementtomarketdelayreason._vaannouncementtomarketdelayreason
            ,sub__bookplanningmain_rendition_covertype._vacovertype
            ,sub__bookplanningmain_rendition_deliverystatus._vadeliverystatus
            ,sub__bookplanningmain_rendition_deliverystatusadditionalinformation._vadeliverystatusadditionalinformation
            ,sub__bookplanningmain_rendition_deliverystatussecondarylocation._vadeliverystatussecondarylocation
            ,sub__bookplanningmain_rendition_digitalprintexclusion._vadigitalprintexclusion
            ,sub__bookplanningmain_rendition_discountgroup._vadiscountgroup
            ,sub__bookplanningmain_rendition_discountgroupsecondarylocation._vadiscountgroupsecondarylocation
            ,sub__bookplanningmain_rendition_formattrimsize._vaformattrimsize
            ,sub__bookplanningmain_rendition_marketingclassificationdach._vamarketingclassificationdach
            ,sub__bookplanningmain_rendition_marketingclassificationrow._vamarketingclassificationrow
            ,sub__bookplanningmain_rendition_marketingclassificationus._vamarketingclassificationus
            ,sub__bookplanningmain_rendition_medium._vamedium
            ,sub__bookplanningmain_rendition_onixdistributioninformation._vaonixdistributioninformation
            ,sub__bookplanningmain_rendition_relevantforpromotion._relevantforpromotion
            ,sub__bookplanningmain_rendition_renditiontype._varenditiontype
            ,sub__bookplanningmain_rendition_retractionreason._varetractionreason
            ,sub__bookplanningmain_rendition_springerprojects._vaspringerprojects
            ,sub__bookplanningmain_rendition_volumename._vavolumename        
    
    from {{ source ('bookplaning_cleansing','bookplanningmain_rendition') }}  as main        
    
            left join {{ source ('bookplaning_cleansing','bookplanningmain_rendition_edition') }} as bookplanningmain_rendition_edition
            on main.id = bookplanningmain_rendition_edition.bookplanningmain_renditionid
            
            left join {{ source ('bookplaning_cleansing','bookplanningmain_rendition_offsetprintingdata') }} as bookplanningmain_rendition_offsetprintingdata
            on main.id = bookplanningmain_rendition_offsetprintingdata.bookplanningmain_renditionid
            
            left join {{ source ('bookplaning_cleansing','bookplanningmain_rendition_printingdata') }} as bookplanningmain_rendition_printingdata
            on main.id = bookplanningmain_rendition_printingdata.bookplanningmain_renditionid
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vaannouncementtomarketdelayreason                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_rendition') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_rendition_announcementtomarketdelayreason') }}     as bookplanningmain_rendition_announcementtomarketdelayreason 
                on main.id = bookplanningmain_rendition_announcementtomarketdelayreason.bookplanningmain_renditionid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_rendition_announcementtomarketdelayreason.avatarclient_va_ann_to_mkt_del_reasid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_rendition_announcementtomarketdelayreason 

            on main.id = sub__bookplanningmain_rendition_announcementtomarketdelayreason.id                      
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vacovertype                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_rendition') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_rendition_covertype') }}     as bookplanningmain_rendition_covertype 
                on main.id = bookplanningmain_rendition_covertype.bookplanningmain_renditionid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_rendition_covertype.avatarclient_va_cover_typeid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_rendition_covertype 

            on main.id = sub__bookplanningmain_rendition_covertype.id                      
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vadeliverystatus                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_rendition') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_rendition_deliverystatus') }}     as bookplanningmain_rendition_deliverystatus 
                on main.id = bookplanningmain_rendition_deliverystatus.bookplanningmain_renditionid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_rendition_deliverystatus.avatarclient_va_delivery_statusid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_rendition_deliverystatus 

            on main.id = sub__bookplanningmain_rendition_deliverystatus.id                      
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vadeliverystatusadditionalinformation                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_rendition') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_rendition_deliverystatusadditionalinformation') }}     as bookplanningmain_rendition_deliverystatusadditionalinformation 
                on main.id = bookplanningmain_rendition_deliverystatusadditionalinformation.bookplanningmain_renditionid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_rendition_deliverystatusadditionalinformation.avatarclient_va_del_status_add_infoid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_rendition_deliverystatusadditionalinformation 

            on main.id = sub__bookplanningmain_rendition_deliverystatusadditionalinformation.id                      
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vadeliverystatussecondarylocation                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_rendition') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_rendition_deliverystatussecondarylocation') }}     as bookplanningmain_rendition_deliverystatussecondarylocation 
                on main.id = bookplanningmain_rendition_deliverystatussecondarylocation.bookplanningmain_renditionid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_rendition_deliverystatussecondarylocation.avatarclient_va_delivery_statusid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_rendition_deliverystatussecondarylocation 

            on main.id = sub__bookplanningmain_rendition_deliverystatussecondarylocation.id                      
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vadigitalprintexclusion                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_rendition') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_rendition_digitalprintexclusion') }}     as bookplanningmain_rendition_digitalprintexclusion 
                on main.id = bookplanningmain_rendition_digitalprintexclusion.bookplanningmain_renditionid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_rendition_digitalprintexclusion.avatarclient_va_digital_print_exclusionid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_rendition_digitalprintexclusion 

            on main.id = sub__bookplanningmain_rendition_digitalprintexclusion.id                      
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vadiscountgroup                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_rendition') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_rendition_discountgroup') }}     as bookplanningmain_rendition_discountgroup 
                on main.id = bookplanningmain_rendition_discountgroup.bookplanningmain_renditionid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_rendition_discountgroup.avatarclient_va_disc_group_loc1id = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_rendition_discountgroup 

            on main.id = sub__bookplanningmain_rendition_discountgroup.id                      
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vadiscountgroupsecondarylocation                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_rendition') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_rendition_discountgroupsecondarylocation') }}     as bookplanningmain_rendition_discountgroupsecondarylocation 
                on main.id = bookplanningmain_rendition_discountgroupsecondarylocation.bookplanningmain_renditionid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_rendition_discountgroupsecondarylocation.avatarclient_va_disc_group_loc2id = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_rendition_discountgroupsecondarylocation 

            on main.id = sub__bookplanningmain_rendition_discountgroupsecondarylocation.id                      
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vaformattrimsize                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_rendition') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_rendition_formattrimsize') }}     as bookplanningmain_rendition_formattrimsize 
                on main.id = bookplanningmain_rendition_formattrimsize.bookplanningmain_renditionid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_rendition_formattrimsize.avatarclient_va_format_trimsizeid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_rendition_formattrimsize 

            on main.id = sub__bookplanningmain_rendition_formattrimsize.id                      
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vamarketingclassificationdach                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_rendition') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_rendition_marketingclassificationdach') }}     as bookplanningmain_rendition_marketingclassificationdach 
                on main.id = bookplanningmain_rendition_marketingclassificationdach.bookplanningmain_renditionid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_rendition_marketingclassificationdach.avatarclient_va_mark_class_dachid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_rendition_marketingclassificationdach 

            on main.id = sub__bookplanningmain_rendition_marketingclassificationdach.id                      
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vamarketingclassificationrow                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_rendition') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_rendition_marketingclassificationrow') }}     as bookplanningmain_rendition_marketingclassificationrow 
                on main.id = bookplanningmain_rendition_marketingclassificationrow.bookplanningmain_renditionid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_rendition_marketingclassificationrow.avatarclient_va_mark_class_rowid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_rendition_marketingclassificationrow 

            on main.id = sub__bookplanningmain_rendition_marketingclassificationrow.id                      
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vamarketingclassificationus                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_rendition') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_rendition_marketingclassificationus') }}     as bookplanningmain_rendition_marketingclassificationus 
                on main.id = bookplanningmain_rendition_marketingclassificationus.bookplanningmain_renditionid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_rendition_marketingclassificationus.avatarclient_va_mark_class_usid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_rendition_marketingclassificationus 

            on main.id = sub__bookplanningmain_rendition_marketingclassificationus.id                      
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vamedium                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_rendition') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_rendition_medium') }}     as bookplanningmain_rendition_medium 
                on main.id = bookplanningmain_rendition_medium.bookplanningmain_renditionid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_rendition_medium.avatarclient_va_mediumid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_rendition_medium 

            on main.id = sub__bookplanningmain_rendition_medium.id                      
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vaonixdistributioninformation                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_rendition') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_rendition_onixdistributioninformation') }}     as bookplanningmain_rendition_onixdistributioninformation 
                on main.id = bookplanningmain_rendition_onixdistributioninformation.bookplanningmain_renditionid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_rendition_onixdistributioninformation.avatarclient_va_onix_distrib_infoid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_rendition_onixdistributioninformation 

            on main.id = sub__bookplanningmain_rendition_onixdistributioninformation.id                      
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _relevantforpromotion                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_rendition') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_rendition_relevantforpromotion') }}     as bookplanningmain_rendition_relevantforpromotion 
                on main.id = bookplanningmain_rendition_relevantforpromotion.bookplanningmain_renditionid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_rendition_relevantforpromotion.avatarclient_va_relevant_for_promotionid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_rendition_relevantforpromotion 

            on main.id = sub__bookplanningmain_rendition_relevantforpromotion.id                      
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _varenditiontype                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_rendition') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_rendition_renditiontype') }}     as bookplanningmain_rendition_renditiontype 
                on main.id = bookplanningmain_rendition_renditiontype.bookplanningmain_renditionid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_rendition_renditiontype.avatarclient_va_rendition_typeid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_rendition_renditiontype 

            on main.id = sub__bookplanningmain_rendition_renditiontype.id                      
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _varetractionreason                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_rendition') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_rendition_retractionreason') }}     as bookplanningmain_rendition_retractionreason 
                on main.id = bookplanningmain_rendition_retractionreason.bookplanningmain_renditionid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_rendition_retractionreason.avatarclient_va_retraction_reasonid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_rendition_retractionreason 

            on main.id = sub__bookplanningmain_rendition_retractionreason.id                      
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vaspringerprojects                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_rendition') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_rendition_springerprojects') }}     as bookplanningmain_rendition_springerprojects 
                on main.id = bookplanningmain_rendition_springerprojects.bookplanningmain_renditionid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_rendition_springerprojects.avatarclient_va_springer_projectsid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_rendition_springerprojects 

            on main.id = sub__bookplanningmain_rendition_springerprojects.id                      
            
            left join 
            
            (
                Select 
                        main.id,
                        struct  (
                                    array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                    array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                    string_agg(avatarclient_lvaitem.code) as code_str,
                                    string_agg(avatarclient_lvaitem.term) as term_str
                                ) as _vavolumename                              
                
                from {{ source ('bookplaning_cleansing','bookplanningmain_rendition') }}     as main
                
                left join {{ source ('bookplaning_cleansing','bookplanningmain_rendition_volumename') }}     as bookplanningmain_rendition_volumename 
                on main.id = bookplanningmain_rendition_volumename.bookplanningmain_renditionid
                                    
                left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                on  bookplanningmain_rendition_volumename.avatarclient_va_volume_nameid = avatarclient_lvaitem.id

                group by 
                main.id

            )   sub__bookplanningmain_rendition_volumename 

            on main.id = sub__bookplanningmain_rendition_volumename.id                      
            