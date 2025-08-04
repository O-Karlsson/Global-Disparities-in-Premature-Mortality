global dir "C:\Users\\Karls\\OneDrive\Everything\Work in progress\CIH\years behind paper\"
global data "C:\Users\\Karls\\OneDrive\Everything\Work in progress\CIH\data\"
cd "$dir"


******************************************************************************
******************************************************************************
*** GDP data
******************************************************************************
******************************************************************************
use "maddison2023_web.dta" , clear
drop if year<1950

/*
*explore gaps 
gen missing = gdppc == .
bys missing countrycode (year): gen gap = year-year[_n-1]
tab gap if missing==0
*/

bys countrycode (year): ipolate gdppc year, gen(gdppci)
gen lgdp = ln(gdppci)
rename countrycode iso3
keep year lgdp iso3 pop
save temp, replace

*** Aggregating GDP for regions
merge m:1 iso3 using "$data\regions",  keep(match) nogen keepusing(region)
collapse (mean) lgdp (rawsum) pop [aweight=pop] , by(region year)
append using temp
keep year lgdp region iso3
save gdp, replace

******************************************************************************
******************************************************************************
*** PPD data
******************************************************************************
******************************************************************************

use if loctype=="Country/Area" using "$data\\wpp_life_table_singleyr" , clear
drop if year > 2023
keep iso3 qx sex year x p
bys iso3 sex year (x): egen tp=total(p)
merge 1:1 iso3 sex x year using "$data\lt", keepusing(qx) nogen
save temp, replace

*** Aggregating for regions
drop if year<1950
merge m:1 iso3 using "$data\regions",  keep(match) nogen keepusing(region)
collapse (mean) qx (rawsum) p [aweight=p] , by(region sex year x)
append using temp

*** Making PPD
drop if qx==.
keep if x<70
bys region iso3 sex year (x): gen q70 = sum(ln(1-qx))
replace q70 = (1-exp(q70))*100
replace q70 = real(string(q70,"%9.4f"))
bys iso3 sex year: gen poq70=sum(p)
bys region iso3 sex year (x):  keep if _n==_N

merge m:1 region iso3 year using gdp, keep(master match) nogen keepusing(lgdp)
keep year sex region iso3 tp q70 lgdp poq70
merge m:1 iso3 using "$data\regions",  keep(master match) nogen keepusing(country)
replace country = subinstr(country, " and ", " & ",.)

save ppd, replace

******************************************************************************
******************************************************************************
*** Creating benchmark PPDs
******************************************************************************
******************************************************************************

*** Creating frontier
use ppd , clear
drop if iso3==""
drop if country == "United Arab Emirates"
drop if country == "Switzerland"
drop if country == "China, Hong Kong SAR"
drop if country == "Hong Kong"

bys iso3: egen small = max(year==2019 & tp<5000 & sex==3)
drop if small == 1
bys sex year (q70): keep if _n==1
gen of70 = q70	
gen ocountry = country
bys  sex (year): replace country = country[_n-1] if q70>=q70[_n-1] & q70[_n-1]<.
bys  sex (year): replace q70 = q70[_n-1]   if q70>=q70[_n-1] & q70[_n-1]<.
rename (country q70)(frontier f70)

gen f70hat = .
forval s = 1/3 {
reg f70 year if inrange(year,1820,2019) & sex == `s'
predict yhat
replace f70hat=yhat if sex == `s' & inrange(year,1820,2019) 
drop yhat
}
replace f70hat = real(string(f70hat,"%9.4f"))

keep frontier f70 year sex of70 ocountry f70hat
save bench , replace

*** Getting Preston curve parameters
use ppd , clear
drop if iso3=="" 
drop if sex!=3

keep iso3 year q70 lgdp sex poq70 
drop if lgdp+q70==.
drop if !inrange(year,1950,2019)

// fixed slope
gen a=.
gen b=.
reg q70 i.year lgdp // [pweight=poq70]
replace b = _b[lgdp]
replace a = _b[_cons]
forval i =1951/2019 {
replace a = a+_b[`i'.year] if year == `i'
}

// flexible slope
gen a2=.
gen b2=.
forval i =1950/2019 {
reg q70 lgdp if year == `i' // [pweight=poq70]
replace b2 = _b[lgdp] if year == `i'
replace a2 = _b[_cons] if year == `i'
}

// range of GDP for each year (for "out of sample" predictions)
bys year: egen gmax = max(lgdp)
bys year: egen gmin = min(lgdp)

duplicates drop year sex, force
gen oa = a
bys  sex (year): replace a = a[_n-1] if a>=a[_n-1] & a[_n-1]<.
keep year sex gmax gmin a b a2 b2 oa

merge 1:1 sex year using bench, nogen
save bench , replace


// the final datasets
use ppd, clear
sort tp
gen nr = -_n if year == 2019 & sex == 3 & iso3!=""
sort nr
replace nr = _n if year == 2019 & sex == 3 & iso3!=""
bys iso3 (nr): replace nr = nr[1] if iso3!=""
drop if nr > 30 & nr<.
drop if year<1970
keep year sex region iso3 q70 lgdp country
gen lid=iso3
replace lid = "region::"+region if lid==""
append using bench
save data, replace 

use data, clear

*** Years behind frontier
foreach metric in f70hat f70 {
gen y`metric'=.
levelsof lid , local(llvl)
foreach l in `llvl' {
forval s  = 1/3 {
forval y  = 1970/2023 {
sum q70 if year==`y' & lid=="`l'" & sex == `s'
local mean = `r(mean)'
sum year if `metric'>=`mean' & `metric'<. & year<=`y' & sex == `s'
if r(N)!=0 replace y`metric' = `y'-r(max) if lid == "`l'" & year == `y' & sex == `s'
}
}
}	
}
save data, replace


*** Years behind/ahead of the preston curve
use data, clear
capture drop yp70
gen yp70=.
levelsof region , local(llvl)
foreach l in `"United States*"' `"North Atlantic"' `llvl' {
sum q70 if year==2019 & region=="`l'" & sex ==3
local mean = `r(mean)'

sum lgdp if year==2019 & region=="`l'" & sex == 3
if r(N)!=0 {
local gdp = `r(mean)'
gen p70 = a+b*`gdp'

replace p70 = real(string(p70,"%9.4f"))


sum p70 if year==2019 & sex == 3
local bench = r(mean) 

if `mean'>=`bench' { 
sum year if p70>=`mean' & p70<.  & year<=2019  & sex == 3
if r(N)!=0 replace yp70 = r(max)-2019 if region=="`l'" & year == 2019 & sex == 3

}

if `mean'<`bench' { 
dis "`bench', `mean'"
sum year if q70>=`bench' & q70<. & year<=2019 & sex == 3 & region=="`l'"
if r(N)!=0 replace yp70 = 2019-r(max) if region=="`l'" & year==2019 & sex==3
}
drop p70
}
}

save data, replace

*** Years behind/ahead of the preston curve (flexible)
use data, clear
gen yp270=.
levelsof region , local(llvl)
foreach l in `llvl' {
sum q70 if year==2019 & region=="`l'" & sex ==3
local mean = `r(mean)'

sum lgdp if year==2019 & region=="`l'" & sex == 3
if r(N)!=0 {
local gdp = `r(mean)'
gen p270 = a2+b2*`gdp'
sum p270 if year==2019 & sex == 3
local bench = r(mean) 

if `mean'>=`bench' { 
sum year if p270>=`mean' & p270<. & year<=2019  & sex == 3
if r(N)!=0 replace yp270 = r(max)-2019 if region=="`l'" & year==2019 & sex == 3
}

if `mean'<`bench' { 
sum year if q70>=`bench' & q70<. & year<=2019 & sex == 3 & region=="`l'"
if r(N)!=0 replace yp270 = 2019-r(max) if region=="`l'" & year==2019 & sex == 3
}
drop p270
}
}
		
replace region = subinstr(subinstr(region, " and ", " & ",.),"*","",.)
save data, replace


use bench , clear
br
keep year sex frontier f70 of70 ocountry f70hat
rename (f70 frontier of70 ocountry)(frontierPPD frontierCountry lowestPPD lowestPPDcountry)
gen Sex = "Male" if sex == 1
replace Sex = "Female" if sex == 2
replace Sex = "Both" if sex == 3
drop sex
export delimited using frontierPPD.csv,replace
