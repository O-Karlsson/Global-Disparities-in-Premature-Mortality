program define mdl
	syntax , dir(string) group(integer)
		
		cd "$dir"
		capture erase "yrs_`group'"

		clear
		gen year = .
		save yrs_`group', replace


		
		use clean, clear
		keep if group == `group' | group==.
		levelsof iso3 , local(clvl)
		
		
		foreach c in `clvl' {
		foreach s in 1 2 3 {

		preserve
		keep if sex == `s'  & (iso3 == "`c'" | group==.)
		
		
		levelsof year if year>=1950 & iso3 == "`c'" , local(ylvl)
		
		
		foreach e in f70 g70 p70 p270 n70 {
		gen y`e' = .
		
		foreach y in `ylvl' {
		
		sum q70 if year==`y'
		local mean = `r(mean)'

		dis `y'
				
		
		
		***********************************************************************
		*** Years behind/ahead of the global PPD/ North Atlantic
		if inlist("`e'","g70","n70") {
		sum `e' if year==`y'
		local m`e'  = `r(mean)'
		if `mean'>=`m`e'' { // if the target has greater mortality than the benchmark, the target is behind
		sum year if `e'>=`mean' & `e'<.  & year<=`y' // what is the most recent year the benchmark had as high mortality as the target?
		if r(N)!=0 replace y`e' = r(max)-year if iso3 == "`c'" & year == `y'
		}
		if `mean'<`m`e'' { // if the target has lower mortality than the benchmark, the target is ahead
		sum year if q70>=`m`e'' & q70<. & year<=`y' // what year did the target have as high mortality as the benchmark
		if r(N)!=0 replace y`e' = year-r(max) if iso3 == "`c'" & year == `y'
		}
		}
		***********************************************************************

		
		
		***********************************************************************
		*** Years behind frontier
		if "`e'"=="f70" {
		sum year if `e'>=`mean' & `e'<. & year<=`y'
		if r(N)!=0 replace y`e' = r(max)-year if iso3 == "`c'" & year == `y'
		}
		***********************************************************************
		
		
		
		***********************************************************************
		*** Years behind/ahead of the Preston curve
		if "`e'"=="p70" {
		sum lgdp if year==`y' 
		if r(N)!=0 {
		local gdp = `r(mean)'
		gen p70 = a+b*`gdp'
		sum p70 if year==`y' 
		local m`e' = `r(mean)'
		if `mean'>=`m`e'' { 
		sum year if `e'>=`mean' & `e'<.  & year<=`y' 
		if r(N)!=0 replace y`e' = r(max)-year if iso3 == "`c'" & year == `y'
		}
		if `mean'<`m`e'' { 
		sum year if q70>=`m`e'' & q70<. & year<=`y'
		if r(N)!=0 replace y`e' = year-r(max) if iso3 == "`c'" & year == `y'
		}
		drop p70
		}
		}
		***********************************************************************

		
		***********************************************************************
		*** Sensitivity, more flexible GDP
		if "`e'"=="p270" {
		sum lgdp if year==`y' 
		if r(N)!=0 {
		local gdp = `r(mean)'
		gen p270 = a2+b2*`gdp'/*+c2*`gdp'^2*/
		sum p270 if year==`y' 
		local m`e' = `r(mean)'
		if `mean'>=`m`e'' { 
		sum year if `e'>=`mean' & `e'<.  & year<=`y' 
		if r(N)!=0 replace y`e' = r(max)-year if iso3 == "`c'" & year == `y'
		}
		if `mean'<`m`e'' { 
		sum year if q70>=`m`e'' & q70<. & year<=`y'
		if r(N)!=0 replace y`e' = year-r(max) if iso3 == "`c'" & year == `y'
		}
		drop p270
		}
		}
		***********************************************************************
		
		
		
		}
		}
		
		drop if iso3 == ""
		drop if year < 1950
		keep iso3 y*70 year sex region q70 lgdp tp
		append using yrs_`group'
		save yrs_`group' , replace
		restore
		
		}
		}
			
	
end