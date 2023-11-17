select 
	itrvl_rpt.cusindprofile_vw.cusnum,
	itrvl_rpt.cusindprofile_vw.gvnnam,
	itrvl_rpt.cusindprofile_vw.famnam,
	itrvl_rpt.cusindprofile_vw.memdob,
	itrvl_rpt.cuscntinfo_vw.emladr,
	itrvl_rpt.cusdynatrdtl_vw.dynatr
from 
	itrvl_rpt.cusindprofile_vw
	left join itrvl_rpt.cuscntinfo_vw on itrvl_rpt.cusindprofile_vw.cusnum = itrvl_rpt.cuscntinfo_vw.cusnum
	left join itrvl_rpt.cusdynatrdtl_vw on itrvl_rpt.cusindprofile_vw.cusnum = itrvl_rpt.cusdynatrdtl_vw.cusnum
