global dir "C:\Users\\Om\\OneDrive\Everything\Work in progress\CIH\years behind paper\"
global data "C:\Users\\Om\\OneDrive\Everything\Work in progress\CIH\data\"
cd "$dir"

use "maddison2023_web.dta" , clear
expand 2 if year == 2022, gen(exp)
replace gdppc = . if exp==1
replace year = 2023 if exp==1
bys countrycode (year): ipolate gdppc year, gen(gdppci) epolate
gen lgdp = ln(gdppci)
rename countrycode iso3
save gdp, replace

******************************************************************************
*** Making 70q0

use if loctype=="Country/Area" using "$data\\wpp_life_table_singleyr" , clear
drop if year > 2023
keep iso3 qx sex year x p
bys iso3 sex year: egen tp=total(p)
*gen unqx =  qx
merge 1:1 iso3 sex x year using "$data\lt", keepusing(qx) nogen
*corr qx unqx
drop if qx==.
keep if x<70
bys iso3 sex year (x): egen q70 = total(ln(1-qx))
replace q70 = (1-exp(q70))*100
bys iso3 sex year: egen poq70=total(p)
keep if x==0
sort year
br if iso3 == "NOR" & sex==3
******************************************************************************
*** Adding GDP

merge m:1 iso3 year using gdp, keep(master match) nogen keepusing(lgdp pop)

******************************************************************************
*** Aggregating for regions and world

merge m:1 iso3 using "$data\regions",  keep(match) nogen keepusing(region)
save temp, replace

bys region sex year: egen num1 = total(lgdp*pop)
bys region sex year: egen den1 = total(pop)
replace lgdp = num1/den1
bys region sex year: egen num = total(q70*poq70)
bys region sex year: egen den = total(poq70)
replace q70=num/den
duplicates drop region sex year, force
replace iso3 = region
drop num den num1 den1
save temp2, replace

use temp, clear
bys sex year: egen num1 = total(lgdp*pop)
bys sex year: egen den1 = total(pop)
replace lgdp = num1/den1
bys sex year: egen num = total(q70*poq70)
bys sex year: egen den = total(poq70)
replace q70=num/den
duplicates drop sex year, force
replace iso3 = "World"
replace region = "World"
drop num den num1 den1

append using temp
append using temp2

******************************************************************************
*** Creating benchmark PPDs
******************************************************************************
*** Creating global average
preserve
keep if region == "World"
gen og70= q70
bys  sex (year): replace q70 = q70[_n-1] if q70>=q70[_n-1] & q70[_n-1]<.
rename (region q70) (global g70)
keep global g70 year sex og70
drop if year < 1950
save bench , replace
restore

******************************************************************************
*** Creating global frontier
preserve
bys iso3: egen small = max(year==2019 & tp<3000 & region!=iso3 & sex==3)
drop if small == 1
bys  sex year (q70): keep if _n==1
gen of70 = q70	
gen oiso3 = iso3
bys  sex (year): replace iso3 = iso3[_n-1] if q70>=q70[_n-1] & q70[_n-1]<.
bys  sex (year): replace q70 = q70[_n-1]   if q70>=q70[_n-1] & q70[_n-1]<.
rename (iso3 q70)(frontier f70)
keep frontier f70 year sex of70 oiso3
append using bench
save bench , replace
restore

******************************************************************************
*** Creating North Atlantic benchmark
preserve
keep if region == "North Atlantic"
keep if iso3==region
gen on70= q70
bys  sex (year): replace q70 = q70[_n-1] if q70>=q70[_n-1] & q70[_n-1]<.
rename (region q70) (NA n70)
keep NA n70 year sex on70
drop if year < 1950
append using bench
save bench , replace
restore

******************************************************************************
*** Getting Preston curve parameters
preserve
drop if iso3==region
keep iso3 year q70 lgdp sex poq70
drop if lgdp+q70==.
drop if !inrange(year,1950,2023)
gen a=.
gen b=.
levelsof sex,local(slvl)
foreach s in `slvl' {
reg q70 i.year lgdp if sex == `s' & year < 2020 [pweight=poq70]
replace b = _b[lgdp] if sex == `s'
replace a = _b[_cons] if sex == `s'
forval i =1951/2019 {
replace a = a+_b[`i'.year] if year == `i' & sex == `s'
}
forval i =2020/2023 {
replace a = a+_b[2019.year] if year == `i' & sex == `s'
}
}

gen a2=.
gen b2=.
gen c2=.
levelsof sex,local(slvl)
foreach s in `slvl' {
forval i =1950/2023 {
reg q70 lgdp /* c.lgdp#c.lgdp */ if sex == `s' & year == `i' [pweight=poq70]
*replace c2 = _b[c.lgdp#c.lgdp] if sex == `s' & year == `i'
replace b2 = _b[lgdp] if sex == `s' & year == `i'
replace a2 = _b[_cons] if sex == `s' & year == `i'
}
}

bys year: egen gmax = max(lgdp)
bys year: egen gmin = min(lgdp)
duplicates drop sex year sex, force
gen oa = a
bys  sex (year): replace a = a[_n-1] if a>=a[_n-1] & a[_n-1]<.
keep year sex gmax gmin a b a2 b2 c2 oa
append using bench
save bench , replace
restore

append using bench // these are small so just attach them at the end of the file

******************************************************************************
*** Grouping countries for parallelization

encode iso3, gen(cntry)
egen group = cut(cntry), group(16)
replace group = group+1
keep iso3 year sex q70 f70 g70 region group lgdp iso3 a b tp a2 b2 c2 oa n70 NA
compress 
drop if year<1950 & iso3!=""
*drop if year > 2019
save clean , replace
use clean, clear
******************************************************************************
*** Creating distance from frontier, global PPD, and preston curve

cd "$dir"
sysdir set PERSONAL "$dir\\dos"
clear all
parallel initialize  16 , force
program def myprogram
	forval i = 1/16 {
	if	($pll_instance == `i') mdl, dir($dir) group(`i')
	}
end
parallel, nodata processors(8) prog(myprogram): myprogram

use yrs_1, clear
forval i = 2/16 {
append using yrs_`i'
}
merge m:1 iso3 using "$data\regions", keepusing(country) update nogen
replace country = region if country == ""

levelsof country if length(country)>10 , local(clvl)
foreach c in `clvl' {
dis `"replace country = "" if country == "`c'""'
}

replace country = "Congo DR" if country == "Congo, Dem. Rep."
replace country = "Egypt" if country == "Egypt, Arab Rep."
replace country = "Iran" if country == "Iran, Islamic Rep."
replace country = "Russia" if country == "Russian Federation"
replace country = "Yemen" if country == "Yemen, Rep."
replace country = "South Korea" if country == "Korea, Rep."
save yrs, replace



