global dir "..." // working directory
global data "..." // data directory
cd "$dir"

*************************************************************************************************************
*************************************************************************************************************
** UN WPP 2024 Life Tables and Population by age ************************************************************
*************************************************************************************************************
*************************************************************************************************************
/* datasets from UN WPP 2024

// life tables (single year) for males, females, and both
https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Life_Table_Complete_Medium_Both_1950-2023.csv.gz
https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Life_Table_Complete_Medium_Female_1950-2023.csv.gz
https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Life_Table_Complete_Medium_Male_1950-2023.csv.gz

// population by single year age groups
https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Population1JanuaryBySingleAgeSex_Medium_1950-2023.csv.gz
*/

clear
gen del=.
save temp, replace

local files WPP2024_Life_Table_Complete_Medium_Both_1950-2023.csv ///
WPP2024_Life_Table_Complete_Medium_Male_1950-2023.csv ///
WPP2024_Life_Table_Complete_Medium_Female_1950-2023.csv

foreach f in `files' {
import delimited using "$data\\WPP 2024\\`f'" , clear encoding("utf-8") 
rename  (location loctypename time agegrpstart agegrpspan v25 sx tx iso3_code) ///
		(country loctype year x len Lx Sx Tx iso3)
drop if  loctype==""
keep country loctype year x len mx qx px lx dx Lx Sx Tx ex ax iso3
if strpos("`f'","Male") gen sex = 1
if strpos("`f'","Female") gen sex = 2
if strpos("`f'","Both") gen sex = 3
append using "$data\\temp"
save "$data\\temp" , replace
}

import delimited using "$data\\WPP 2024\\WPP2024_PopulationBySingleAgeSex_Medium_1950-2023.csv" , clear encoding("utf-8") 
rename  (location loctypename time agegrpstart iso3_code popmale popfemale poptotal) ///
		(country loctype year x  iso3 p1 p2 p3)
drop if  loctype==""
keep country loctype year x iso3 p1 p2 p3
reshape long p , i(country iso3 loctype x year) j(sex)
merge 1:1 country iso3 loctype x year sex using "$data\\temp" , nogen
save "wpp_life_table_singleyr_years_behind" , replace

**********************************************************************************************
**********************************************************************************************
*** HMD Life Tables
**********************************************************************************************
**********************************************************************************************

/* data from HMD for mortality before 1950 
https://www.mortality.org/Account/Login
extracted to $data\HMD\
*/

clear
gen del =.
save lt , replace
foreach s in b f m {
local start=0
local files : dir "$data\HMD\\`s'ltper_1x1"  files "*.txt"
foreach f in `files' {
import  delimited   "$data\HMD\\`s'ltper_1x1\\`f'" , clear   varnames(noname)
drop if _n<3
local l =  length(v1)
local start = 0
local n = 0
forval i=`l'(-1)1 {
dis `i' 
count if inlist(substr(v1, `i',1),"1","2","3","4","5") | inlist(substr(v1, `i',1),"6","7","8","9","0",".")
if r(N)!=0 & `start'== 0 {
local start = 1
local n = `n'+1
gen j`n' = ""
}

if r(N)==0 & `start'== 1 local start = 0

if r(N)!=0 & `start'==1 {
replace j`n' =  substr(v1, `i',1)+j`n'
}
}

drop v*
destring j*, replace
gen file = "`f'"
gen sex = "`s'"
compress
append using lt
save lt, replace
}
}

use lt, clear
rename (j10 j9 j8 j7 j6 j5 j4 j3 j2 j1)(year x mx qx ax lx dx Lx Tx ex)
gen iso3 = subinstr(file, ".bltper_1x1.txt", "" ,.)
replace iso3 = subinstr(iso3, ".fltper_1x1.txt", "" ,.)
replace iso3 = subinstr(iso3, ".mltper_1x1.txt", "" ,.)
replace iso3 = "deu" if iso3 == "deutnp"
replace iso3 = "fra" if iso3 == "fratnp"
replace iso3 = "nzl" if iso3 == "nzl_np"
replace iso3 = "gbr" if iso3 == "gbr_np"
replace iso3 = upper(substr(iso3,1,3))
replace sex = "3" if sex == "b"
replace sex ="2" if sex =="f"
replace sex ="1" if sex =="m"
destring sex, replace
drop del
save lt, replace

******************************************************************************
******************************************************************************
*** GDP data
******************************************************************************
******************************************************************************
* data on GDP from https://www.rug.nl/ggdc/historicaldevelopment/maddison/releases/maddison-project-database-2023

use "maddison2023_web.dta" , clear
drop if year<1950
rename countrycode iso3
keep year gdppc iso3 pop
save temp, replace

*** Aggregating GDP for regions
merge m:1 iso3 using "$data\regions",  keep(match) nogen keepusing(region)
collapse (mean) gdppc [aweight=pop] , by(region year)
append using temp

gen lgdp = ln(gdppc)
keep year lgdp region iso3
save gdp, replace

******************************************************************************
******************************************************************************
*** PPD data
******************************************************************************
******************************************************************************

use iso3 qx sex year x p if iso3!="" & year <= 2023 using "$data\\wpp_life_table_singleyr_years_behind" , clear
bys iso3 sex year (x): egen tp=total(p)
merge 1:1 iso3 sex x year using "$data\lt", keepusing(qx) nogen
save temp, replace

* Aggregating for regions
drop if year<1950
merge m:1 iso3 using "$data\regions",  keep(match) nogen keepusing(region)
collapse (mean) qx  [aweight=p] , by(region sex year x)
append using temp

* Making PPD
drop if qx==.
keep if x<70
bys region iso3 sex year (x): gen q70 = sum(ln(1-qx))
replace q70 = real(string((1-exp(q70))*100,"%9.4f"))
bys region iso3 sex year (x):  keep if _n==_N

* merge the GDP data
merge m:1 region iso3 year using gdp, keep(master match) nogen keepusing(lgdp)
keep year sex region iso3 tp q70 lgdp
merge m:1 iso3 using "$data\regions",  keep(master match) nogen keepusing(country)
replace country = subinstr(country, " and ", " & ",.)
save ppd, replace

******************************************************************************
******************************************************************************
*** Creating benchmark PPDs
******************************************************************************
******************************************************************************

use ppd , clear

* drop regions, smaller countries, and frontier PPD countries with large migrant populations
drop if iso3==""
drop if inlist(country,"United Arab Emirates","Switzerland","Hong Kong","China, Hong Kong SAR")
bys iso3: egen small = max(year==2019 & tp<5000 & sex==3)
drop if small == 1

* keep lowest PPD each year
bys sex year (q70): keep if _n==1

* store the actual lowest PPD each year (before removing increases across years)
gen of70 = q70	
gen ocountry = country

* adjust PPD so it never increases across years (also keeping track of the frontier country)
bys  sex (year): replace country = country[_n-1] if q70>=q70[_n-1]
bys  sex (year): replace q70 = q70[_n-1]   if q70>=q70[_n-1]
rename (country q70)(frontier f70)

* Create linear frontier (for sensitivity analysis)
gen f70hat = .
forval s = 1/3 {
reg f70 year if inrange(year,1820,2019) & sex == `s'
predict yhat
replace f70hat= real(string(yhat,"%9.4f"))  if sex == `s' & inrange(year,1820,2019) 
drop yhat
}

keep frontier f70 year sex of70 ocountry f70hat
save bench , replace

* Getting Preston curve parameters
use ppd , clear
drop if iso3=="" 
drop if sex!=3
keep iso3 year q70 lgdp sex
drop if lgdp+q70==.
drop if !inrange(year,1950,2019)

* fixed slope
gen a=.
gen b=.
reg q70 i.year lgdp
replace b = _b[lgdp]
replace a = _b[_cons]
forval i =1951/2019 {
replace a = a+_b[`i'.year] if year == `i'
}

* flexible slope
gen a2=.
gen b2=.
forval i =1950/2019 {
reg q70 lgdp if year == `i'
replace b2 = _b[lgdp] if year == `i'
replace a2 = _b[_cons] if year == `i'
}

* range of GDP for each year (for indicating "out of sample" predictions)
bys year: egen gmax = max(lgdp)
bys year: egen gmin = min(lgdp)

duplicates drop year, force

* store unadjusted intercept
gen oa = a

* adjust the intercept so it never increases
bys  sex (year): replace a = a[_n-1] if a>=a[_n-1] & a[_n-1]<.
keep year sex gmax gmin a b a2 b2 oa

merge 1:1 sex year using bench, nogen
save bench , replace

* the final dataset
use ppd, clear

* keep regions and the 30 most largest countries
sort tp
gen nr = -_n if year == 2019 & sex == 3 & iso3!=""
sort nr
replace nr = _n if year == 2019 & sex == 3 & iso3!=""
bys iso3 (nr): replace nr = nr[1] if iso3!=""
drop if nr > 30 & nr<.
drop if year<1970
keep year sex region iso3 q70 lgdp country

* Location id
gen lid=iso3
replace lid = "region::"+region if lid==""

* Just append the benchmarks to the bottom
append using bench

* Years behind frontier (and linear frontier f70hat)
foreach metric in f70hat f70 {
gen y`metric'=.
levelsof lid , local(llvl) // for each location
foreach l in `llvl' {
forval s  = 1/3 { // for males, females, and both
forval y  = 1970/2023 { // for each year
sum q70 if year==`y' & lid=="`l'" & sex == `s'
scalar mean = `r(mean)'
sum year if `metric'>=mean & `metric'<. & sex == `s'
if r(N)!=0 replace y`metric' = `y'-r(max) if lid == "`l'" & year == `y' & sex == `s'
}
}
}	
}

* Years behind/ahead of the preston curve (and the more flexible gdp-ppd)
foreach metric in yp70 yp270 {
gen `metric' = .
levelsof region , local(llvl)
foreach l in `llvl' {
sum q70 if year==2019 & region=="`l'" & sex ==3
scalar mean = `r(mean)'

sum lgdp if year==2019 & region=="`l'" & sex == 3
if r(N)!=0 {
scalar gdp = `r(mean)'

if "`metric'"=="yp70" gen p70 = real(string(a+b*gdp,"%9.4f"))
if "`metric'"=="yp270" gen p70 = real(string(a2+b2*gdp,"%9.4f"))

sum p70 if year==2019 & sex == 3

scalar bench = r(mean) 

* PPD is greater than PPD predicted by GDP
if mean>=bench { 
sum year if p70>=mean & p70<.  & year<=2019  & sex == 3
if r(N)!=0 replace `metric' = r(max)-2019 if region=="`l'" & year == 2019 & sex == 3
}

* PPD is lower than PPD predicted by GDP
if mean<bench { 
sum year if q70>=bench & q70<. & year<=2019 & sex==3 & region=="`l'"
if r(N)!=0 replace `metric' = 2019-r(max) if region=="`l'" & year==2019 & sex==3
}
drop p70
}
}
}
save data, replace

use data , clear
keep if q70!= .
drop if year < 1970
keep year sex region q70 lgdp country yf70hat yf70 yp70 yp270
sort country region  year sex

* yp70 yp270 are negative for behind and positive for those ahead
order region country sex year q70 yf70 yf70hat lgdp yp70 yp270
br // Results

use bench , clear
sort year sex
order frontier sex year f70 f70hat ocountry of70 a b a2 b2 gmax gmin oa
br // frontier PPD 

