*********************************************************;
* Demo: Summarizing Data and Benchmarking with SAS      *;
*       Compute Server, CAS-enabled Steps and CASL.     *;
*********************************************************;

/************************************************************/
/* Section 1: SAS Program Executed on the Compute Server    */
/************************************************************/

libname mydata "s:\workshop";

data mydata.orders_clean;
	set mydata.orders;
    Name=catx(' ',
              scan(Customer_Name,2,','),
              scan(Customer_Name,1,','));
run;

title "Compute Server Program";

proc contents data=mydata.orders;
run;

proc freq data=mydata.orders;
    tables Country OrderType;
run;

proc means data=mydata.orders;
    var RetailPrice;
    output out=mydata.orders_sum;
run;

title;


/*********************************************************************/
/* Section 2: SAS Program Executed CAS Server with CAS-enabled Steps */
/*     Link to CAS-enabled Procedure Documentation:                  */
/*     https://go.documentation.sas.com/doc/en/pgmsascdc/v_040/procs2actions/p0275qj00ns5pen16ijvuz8f8j5k.htm */
/*********************************************************************/

cas mySession;

libname mydata "/home/student/S23HMODV";

* Define MYCAS caslib pointing to workshop files and map a libref;
caslib mycas path="/home/student/S23HMODV" libref=mycas;

* Load orders.sashdat to MYCAS caslib;
proc casutil;
	load casdata="orders.sashdat" incaslib="mycas" 
	outcaslib="mycas" casout="orders" replace;
run;

* Load mycas.orders to MYCAS via the DATA step and Compute server;
data mycas.orders_clean;
	set mydata.orders;
    Name=catx(' ',
              scan(Customer_Name,2,','),
              scan(Customer_Name,1,','));
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

cas mysession terminate;


/************************************************************/
/* Section 3: SAS Program Executed on CAS Server with CASL */
/************************************************************/

cas mySession;

title "CASL Program";
proc cas;
  * Create dictionary to reference orders table in Casuser;
    tbl={name='orders', caslib='mycas'};

  * Create CASL variable named DS to store DATA step code. Both 
      input and output tables must be in-memory;
    source ds;
        data mycas.orders_clean;
	        set mycas.orders;
            Name=catx(' ',
                 scan(Customer_Name,2,','),
                 scan(Customer_Name,1,','));
        run;
    endsource;

  * Define caslib pointing to workship files and load orders.sashdat to mycas;
   table.addCaslib / 
         name="mycas",
         path="/home/student/S23HMODV";

  * Drop orders from mycas if it exists;
    table.dropTable / name="orders", 
                      caslib="mycas", 
                      quiet=true;

    table.loadTable / 
        path="orders.sashdat", caslib="mycas", 
        casOut={name="orders", caslib="mycas", replace=true};

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

cas mySession terminate;