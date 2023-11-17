select 
	cabAlloc.cabin_no as Cabin, 
	cabAlloc.category_code as Category_Code,
	bkg.booked_occupancy as Occupancy, 
	cabAlloc.max_occupancy as Max_Occupancy, 
    cat.cruise_code as Cruise_Id,
    cabAlloc.cabin_status as Cabin_Status,
    cat.departure_date as Departure_Date

from 
	itrvl_rpt.itrvl_cru_inv_cabin_alloc_vw cabAlloc
    full join itrvl_rpt.itrvl_cru_bkg_vw bkg on bkg.component_id = cabAlloc.component_id
	full join itrvl_rpt.itrvl_cru_cat_seg_details_vw seg on cabAlloc.segment_id = seg.segment_sequence_id 
	full join itrvl_rpt.itrvl_cru_cat_seg_ref_vw segRef on  segRef.segment_id = seg.segment_id 
	full join itrvl_rpt.itrvl_cru_catalogue_vw cat on cat.cruise_id = segRef.cruise_id 
	left join itrvl_rpt.itrvl_cru_inv_block_defn_vw blk on blk.cruise_code = cat.cruise_code 
    
where 
	segRef.segment_order = 1 and cat.departure_date > now() and (
    (cat.departure_date >= '9/1/2023' and cat.region_code <> 'AKA') or cat.departure_date >= '1/1/2024')
order by cat.departure_date asc, cabin asc