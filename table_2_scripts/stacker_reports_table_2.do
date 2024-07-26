clear all

global data "D:\Descargas\table_2_formatting_reports"
global stata_path "$data/stata"

forvalues i=1(1)1000 {
	import delimited "$data/formatting_reports_`i'.csv", clear
	save "$stata_path/formatting_reports_`i'", replace
}

use "$stata_path/formatting_reports_1", clear
forvalues i=2(1)1000 {
	append using "$stata_path/formatting_reports_`i'", force
	save "$stata_path/all_reports", replace
}

tab num_control_types if num_rows_post_cleaning == 5