*********************************************************;
* Demo: Summarizing Data and Benchmarking with SAS      *;
*       Compute Server, CAS-enabled Steps and CASL.     *;
*********************************************************;

/************************************************************/
/* Section 1: SAS Program Executed on the Compute Server    */
/************************************************************/

libname srcData "z:\CosmoOrders\Staging";
libname outpData "s:\AnalysisTables";

data srcData.orders_clean;
	set srcData.orders;
    Name=catx(' ',
              scan(Customer_Name,2,','),
              scan(Customer_Name,1,','));
run;

title "Compute Server Program";

proc contents data=srcData.orders;
run;

proc freq data=srcData.orders;
    tables Country OrderType;
run;

proc means data=srcData.orders;
    var RetailPrice;
    output out=outpData.orders_sum;
run;

title;


/*********************************************************************/
/* Section 2: SAS Program Executed CAS Server with CAS-enabled Steps */
/*********************************************************************/

cas mySess sessopts=(metrics=true);

* Define MYCAS caslib pointing to workshop files and map a libref;
caslib mycas path="/home/student/workshop/HODV" libref=mycas;

* Load orders to MYCAS caslib;
proc casutil;
	load casdata="orders.sas7bdat" incaslib="mycas" 
	outcaslib="mycas" casout="orders" replace;
run;

* Process DATA step in CAS to read mycas.orders and create mycas.oders_clean; 
data mycas.orders_clean;
	set mycas.orders;
    Name=catx(' ',
              scan(Customer_Name,2,','),
              scan(Customer_Name,1,','));
run;


title "CAS-Enabled Program";

proc contents data=mycas.orders;
run;

proc freqtab data=mycas.orders;
    tables Country OrderType;
run;

proc mdsummary data=mycas.orders;
    var RetailPrice;
    output out=mycas.orders_sum;
run;

title;


/************************************************************/
/* Section 3: SAS Program Executed on CAS Server with CASL */
/************************************************************/


title "CASL Program";

proc cas;
  * Create dictionary to reference orders table in Casuser;
    tbl={name='orders', caslib='mycas'};

  * Create CASL variable named DS to store DATA step code.;
    source ds;
        data mycas.orders_clean;
	        set mycas.orders;
            Name=catx(' ',
                 scan(Customer_Name,2,','),
                 scan(Customer_Name,1,','));
        run;
    endsource;

  * Execute DATA step code;
    dataStep.runCode / code=ds;

  * List orders column attributes, similar to PROC CONTENTS;
    table.columnInfo / 
        table=tbl;

  * Generate frequency report, similar to PROC FREQ;
    simple.freq / 
        table=tbl, 
        inputs={'Country', 'OrderType'};

  * Generate summary table, similar to PROC MEANS;
    simple.summary / 
        table=tbl, 
        input={'RetailPrice'}, 
        casOut={name='orders_sum', replace=true};
quit;
title;

cas mySess terminate;