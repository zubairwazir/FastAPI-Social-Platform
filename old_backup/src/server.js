const express = require("express");
const bodyParser = require("body-parser");

const app = express();
const fetch = require("node-fetch");

app.use(bodyParser.json());

//const hasura_token = 'zf4vDWFEQp2hDup';
const hasura_token = 'T65u84ww7F9SXq2';
//const hasura_api_url = 'https://data-api-69.hasura.app/'; test
const hasura_api_url = 'https://15rock.hasura.app/';

/**
  * @desc return result by fetching to hasura api with sql_query parameter.
  * @param string query - sql query to be executed on hasura.
  * @return data - sql query result.
*/
async function getResultByQuery(query) {
    var body_data = {
        "type": "run_sql",
        "args": {
            "sql": query
        }
    }

    try {
        const fetchData = await fetch(
            `${hasura_api_url}/v1/query`,
            {
                method: 'POST',
                body: JSON.stringify(body_data),
                headers: {
                    'x-hasura-admin-secret': hasura_token,
                    'X-Hasura-Role': 'admin',
                    'content-type': 'application/json'
                }
            }
        );

        const data = await fetchData.json()
        return data;
    }
    catch (error) {
        console.log("error", error);
    }
}
/**
  * @desc return a formatted string for query
  * @param array arr: array of fieldnames
  * @param string tname: tablename
  * @result result formatted string:
  * @example getTableString(["Carbon", "Income"], "Carbon") -> "\"Carbon\".\"Carbon\", \"Carbon\".\"Income\""
*/
function getTablesString(arr, tname) {
    return arr.map((ele) => ("\"" + tname + "\"." + "\"" + ele + "\"")).join(", ");
}

/**
 * @desc return result whether it is a recognized token or not.
 * @param username 
 * @param token
 * 
 */

 function isValidToken(username, token) {
    //  here you can recognize a pair of username & token.
    var tokens = [
        {username: 'aaa', token: 'bbb'},
        {username: 'abc', token: 'eee'},
        {username: 'cba', token: 'ddd'},
    ]
    return tokens.findIndex((i) => {return i.username === username && i.token === token}) >= 0;
 }

/**
 * @desc api_url: return the data categorized by group with score, rank field.
 * @param scoreField: the fieldname of Carbon table like weight in your example. 
 * @param groupFields: the array of fieldnames used for groupping
 * @param showFields1: the array of filednames of table1 to show.
 * @param showFields2: the array of filednames of table2 to show.
 */
app.get('/getData', async (req, res) => {
// get request input
    const { scoreField, groupFields, showFields1, showFields2, year } = req.body;
    const {token, username} = req.headers;

// token check
    if (!isValidToken(username, token)) {
        return res.json({ 'error': 'invalid-token' });
    }

    if(scoreField == undefined || groupFields == undefined || showFields1 == undefined || showFields2 == undefined || year == undefined) {
        return res.json({ 'error': 'not enough parameters' });
    }

//getData from hasura_api
    const tname1 = "companyData", tname2 = "carbon";
    var colnames1 = getTablesString(showFields1, tname1);
    var colnames2 = getTablesString(showFields2, tname2);
    var colnames = getTablesString(groupFields, tname1);
    var scoreColname = `"${tname2}"."${scoreField}"`;
    const getDataQuery = `SELECT ${colnames1}, ${colnames2}, ${colnames}, ${scoreColname} FROM \"company\".\"${tname1}\", \"company\".\"${tname2}\" WHERE \"${tname1}\".\"ticker\" = \"${tname2}\".\"ticker\" and \"${tname2}\".\"year\" = ${year};`;
    console.log(getDataQuery);
    var data = await getResultByQuery(getDataQuery);
    data = data["result"];

// get result from the data
  //data(array form)->objData(object form)
    const header_str = data[0];
    var objData = [];
    let len = data.length;
    for (let i = 1; i < len; ++i) {
        let len1 = header_str.length;
        var temp = {};
        for (let j = 0; j < len1; ++j) {
            temp[header_str[j]] = data[i][j];
        }
        temp[scoreField] = parseFloat(temp[scoreField]);
        objData.push(temp);
    }

  //groupping
    var groupByData = {};
    len = objData.length;
    for (i = 0; i < len; ++i) {
        var gpstr = groupFields.map((ele, j) => (`${ele}:${objData[i][ele]}`)).join(",");
        if (gpstr in groupByData) groupByData[gpstr].push(objData[i]);
        else groupByData[gpstr] = [objData[i]];
    }

  //get score, rank in each group
    len = groupByData.length;
    for (i in groupByData) {
        groupByData[i].sort((a, b) => b[scoreField] - a[scoreField]);
        var len1 = groupByData[i].length;
        var sum = 0.0;
        for (j = 0; j < len1; ++j) {
            var ele = groupByData[i][j]
            sum += parseFloat(ele[scoreField]);
        }
        for (j = 0; j < len1; ++j) {
            var ele = groupByData[i][j]
            ele["Score"] = parseFloat(ele[scoreField] / sum * len1).toFixed(2);
            ele["Rank"] = j + 1;
            groupByData[i][j] = ele;
        }
    }
    return res.json({ groupByData });
});

app.get('/', async (req, res) => {
    return res.json({ 'message': 'hello World' })
});

app.listen(process.env.PORT || 3000);