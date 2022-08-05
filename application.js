const oracledb = require('oracledb');
const fs = require('fs');
const csv = require('fast-csv');

//read from csv
const file_name = "australian"
const file_path = `data\\${file_name}.csv`;
const rows = [];
fs.createReadStream(file_path)
    .pipe(csv.parse({ headers: true }))
    .on('error', error => console.error(error))
    .on('data', row => rows.push(row))
    .on('end', rowCount => console.log(`Parsed ${rowCount} rows`));


async function run() {

  try {
    
    //connect to db
    const connection = await oracledb.getConnection({
      user            : "EGITIM",
      password        : "EGITIM_1478.",
      connectionString: "5.189.178.35:1521/datateamdb"
    });
    
    //create table
    const table_name = `fatih_${file_name}`;
    const headers = Object.keys(rows[0]);
    var sql_create = `create table ${table_name}(`;

    
    for (let i = 0; i < headers.length; i++) {
      sql_create += `${headers[i]} varchar2(4000), `;
    }
    sql_create += `primary key (${headers[0]}))`; //assuming that the first column is the primary key
    console.log(sql_create);
    
  await connection.execute(sql_create);
  console.log("Table created successfully");


    //insert data into the table
    var sql_insert = `insert into ${table_name} values (`;
    for (let i =0; i<headers.length-1; i++) {
      sql_insert += `:${i}, `;
    }
    sql_insert += `:${headers.length-1})`;
    for (let i = 0; i < rows.length; i++) {
      connection.execute(
        sql_insert, Object.values(rows[i]));
        connection.commit();
    }
    console.log("Data inserted succesfully.");
  
  } catch (err) {
    console.log(err);
  }
}

run();
