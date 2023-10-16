        

        ------------------------------------------------------------------------------
        -- Jira         :   DEPA - 1351
        -- Source       :   dbt
        -- Project      :   snproject52c6e272
        -- Dataset      :   bookplanning_staging
        -- Path         :   -
        -- Name         :   addresses_address.sql
        -- Author       :   Jörg Sachsenweger
        -- Date         :   2023-10-10
        -- Update       :   -                   
        --
        -- Requester    :   joerg.zanner@springer.com
        -- Short Descr. :   AUTOGENERATET SCRIPT which adds foreign-keys and va/avatarclient-structs to the source-table.
        --
        --------------------------------------------------------------------------    

        Select  main.* 
                ,sub__addresses_address_vabusinesspartnerroleindv._vabusinesspartnerroleindv
                ,sub__addresses_address_vabusinesspartnerroleinst._vabusinesspartnerroleinst
                ,sub__addresses_address_vacountry._vacountry
                ,sub__addresses_address_vaformofaddress._vaformofaddress
                ,sub__addresses_address_valanguage._valanguage
                ,sub__addresses_address_vanameprefix._vanameprefix
                ,sub__addresses_address_vapersonaltitle._vapersonaltitle
                ,sub__addresses_address_vastate._vastate        

        from {{ source ('bookplaning_cleansing','addresses_address') }}  as main        
        
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vabusinesspartnerroleindv                              

                    from {{ source ('bookplaning_cleansing','addresses_address') }}     as main

                    left join {{ source ('bookplaning_cleansing','addresses_address_vabusinesspartnerroleindv') }}     as addresses_address_vabusinesspartnerroleindv 
                    on main.id = addresses_address_vabusinesspartnerroleindv.addresses_addressid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  addresses_address_vabusinesspartnerroleindv.avatarclient_va_busin_partn_role_indvid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__addresses_address_vabusinesspartnerroleindv 

                on main.id = sub__addresses_address_vabusinesspartnerroleindv.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vabusinesspartnerroleinst                              

                    from {{ source ('bookplaning_cleansing','addresses_address') }}     as main

                    left join {{ source ('bookplaning_cleansing','addresses_address_vabusinesspartnerroleinst') }}     as addresses_address_vabusinesspartnerroleinst 
                    on main.id = addresses_address_vabusinesspartnerroleinst.addresses_addressid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  addresses_address_vabusinesspartnerroleinst.avatarclient_va_busin_partn_role_instid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__addresses_address_vabusinesspartnerroleinst 

                on main.id = sub__addresses_address_vabusinesspartnerroleinst.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vacountry                              

                    from {{ source ('bookplaning_cleansing','addresses_address') }}     as main

                    left join {{ source ('bookplaning_cleansing','addresses_address_vacountry') }}     as addresses_address_vacountry 
                    on main.id = addresses_address_vacountry.addresses_addressid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  addresses_address_vacountry.avatarclient_va_countryid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__addresses_address_vacountry 

                on main.id = sub__addresses_address_vacountry.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vaformofaddress                              

                    from {{ source ('bookplaning_cleansing','addresses_address') }}     as main

                    left join {{ source ('bookplaning_cleansing','addresses_address_vaformofaddress') }}     as addresses_address_vaformofaddress 
                    on main.id = addresses_address_vaformofaddress.addresses_addressid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  addresses_address_vaformofaddress.avatarclient_va_form_of_addressid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__addresses_address_vaformofaddress 

                on main.id = sub__addresses_address_vaformofaddress.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _valanguage                              

                    from {{ source ('bookplaning_cleansing','addresses_address') }}     as main

                    left join {{ source ('bookplaning_cleansing','addresses_address_valanguage') }}     as addresses_address_valanguage 
                    on main.id = addresses_address_valanguage.addresses_addressid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  addresses_address_valanguage.avatarclient_va_languageid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__addresses_address_valanguage 

                on main.id = sub__addresses_address_valanguage.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vanameprefix                              

                    from {{ source ('bookplaning_cleansing','addresses_address') }}     as main

                    left join {{ source ('bookplaning_cleansing','addresses_address_vanameprefix') }}     as addresses_address_vanameprefix 
                    on main.id = addresses_address_vanameprefix.addresses_addressid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  addresses_address_vanameprefix.avatarclient_va_name_prefixid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__addresses_address_vanameprefix 

                on main.id = sub__addresses_address_vanameprefix.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vapersonaltitle                              

                    from {{ source ('bookplaning_cleansing','addresses_address') }}     as main

                    left join {{ source ('bookplaning_cleansing','addresses_address_vapersonaltitle') }}     as addresses_address_vapersonaltitle 
                    on main.id = addresses_address_vapersonaltitle.addresses_addressid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  addresses_address_vapersonaltitle.avatarclient_va_personal_titleid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__addresses_address_vapersonaltitle 

                on main.id = sub__addresses_address_vapersonaltitle.id                      
                
                left join 

                (
                    Select 
                            main.id,
                            struct  (
                                        array_agg(avatarclient_lvaitem.code IGNORE NULLS) as code,
                                        array_agg(avatarclient_lvaitem.term IGNORE NULLS) as term,
                                        string_agg(avatarclient_lvaitem.code) as code_str,
                                        string_agg(avatarclient_lvaitem.term) as term_str
                                    ) as _vastate                              

                    from {{ source ('bookplaning_cleansing','addresses_address') }}     as main

                    left join {{ source ('bookplaning_cleansing','addresses_address_vastate') }}     as addresses_address_vastate 
                    on main.id = addresses_address_vastate.addresses_addressid
                                        
                    left join {{ source ('bookplaning_cleansing','avatarclient_lvaitem') }}    as avatarclient_lvaitem
                    on  addresses_address_vastate.avatarclient_va_stateid = avatarclient_lvaitem.id

                    group by 
                    main.id

                )   sub__addresses_address_vastate 

                on main.id = sub__addresses_address_vastate.id                      
                