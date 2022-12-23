select
	itrvl_rpt.itrvl_bkg_pnr_main_vw.super_pnr_no,
	itrvl_rpt.itrvl_bkg_pnr_contact_vw.profile_id,
	itrvl_rpt.itrvl_bkg_pnr_contact_vw.first_name_en,
	itrvl_rpt.itrvl_bkg_pnr_contact_vw.last_name_en,
	itrvl_rpt.itrvl_bkg_pnr_contact_vw.email_id,
	itrvl_rpt.cusindprofile_vw.memdob

from
	itrvl_rpt.itrvl_bkg_pnr_main_vw
	left join itrvl_rpt.itrvl_bkg_pnr_contact_vw on itrvl_rpt.itrvl_bkg_pnr_main_vw.super_pnr_no = itrvl_rpt.itrvl_bkg_pnr_contact_vw.super_pnr_no
	left join itrvl_rpt.cusindprofile_vw on itrvl_rpt.itrvl_bkg_pnr_contact_vw.profile_id = itrvl_rpt.cusindprofile_vw.cusnum