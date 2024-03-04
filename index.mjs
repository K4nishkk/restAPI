import { MongoClient, ServerApiVersion } from "mongodb";
import express from "express"
import bodyParser from "body-parser"
import cors from "cors"

// Status object
let status = {
    Current_status: "Not started yet",
    Symbols_list_size: 0,
    Symbols_fetched: 0,
    Symbols_added: 0,
    Groups_added: 0,
    Groups_left: 0,
    Error_log: [],
    DBConnection: false,
    estimatedTimeLeft: 0,
}

// *********************************** MongoDB ************************************************

const uri = "mongodb+srv://k4nishkk:qwerQWERasdfASDF@kanishkmongodbcluster.dv8uthu.mongodb.net/?retryWrites=true&w=majority&appName=KanishkMongoDBCluster";

// Create a MongoClient with a MongoClientOptions object to set the Stable API version
const client = new MongoClient(uri, {
    serverApi: {
        version: ServerApiVersion.v1,
        strict: true,
        deprecationErrors: true,
    }
});

// function to add a list of objects to MongoDB
async function mongoInsertMany(data, databaseName, collectionName) {
    try {
        const database = client.db(databaseName);
        const collection = database.collection(collectionName);

        const result = await collection.insertMany(data);

        // update status parameter
        status.Groups_left--;
        status.Groups_added++;
        status.Symbols_added += result.insertedCount;

        // console.log(`${result.insertedCount} items added`) // for debugging
    }
    catch (err) {
        status.Error_log.push(err.message);
    }
    finally {
        // close only if all groups have been successfully added to MongoDB
        if (status.DBConnection === false && status.Groups_left <= 0) {
            // console.log("MongoDB Atlas connection closed") // for debugging
            client.close();
        }
    }
}

// *************************************** Finnhub *******************************************

var timeTask; // setInterval declaration (used in func getCompanyDetails)

const tokens = [
    "cnid7vhr01qj1g5ptj70cnid7vhr01qj1g5ptj7g",
    "cnid931r01qj1g5ptn20cnid931r01qj1g5ptn2g", 
    "cnida99r01qj1g5ptr5gcnida99r01qj1g5ptr60",
    "cnidai9r01qj1g5ptrvgcnidai9r01qj1g5pts00",
    "cnidat1r01qj1g5ptt90cnidat1r01qj1g5ptt9g",
    "cnidb6hr01qj1g5pttogcnidb6hr01qj1g5pttp0",
    "cnidbepr01qj1g5ptu8gcnidbepr01qj1g5ptu90",
    "cnidbrhr01qj1g5ptvdgcnidbrhr01qj1g5ptve0",
    "cnidc51r01qj1g5pu040cnidc51r01qj1g5pu04g",
    "cnidcg9r01qj1g5pu0bgcnidcg9r01qj1g5pu0c0",
    "cnidct9r01qj1g5pu1mgcnidct9r01qj1g5pu1n0",
    "cnidd79r01qj1g5pu290cnidd79r01qj1g5pu29g",
    "cniddh9r01qj1g5pu2ggcniddh9r01qj1g5pu2h0",
    "cniddp9r01qj1g5pu3kgcniddp9r01qj1g5pu3l0",
    "cnide29r01qj1g5pu4a0cnide29r01qj1g5pu4ag",
    "cnideb9r01qj1g5pu4hgcnideb9r01qj1g5pu4i0",
    "cnidel1r01qj1g5pu630cnidel1r01qj1g5pu63g",
    "cnideu9r01qj1g5pu6h0cnideu9r01qj1g5pu6hg",
    "cnidf79r01qj1g5pu750cnidf79r01qj1g5pu75g",
    "cnidfi1r01qj1g5pu8egcnidfi1r01qj1g5pu8f0",
    "cnidfqpr01qj1g5pu8v0cnidfqpr01qj1g5pu8vg",
    "cnidg59r01qj1g5pua8gcnidg59r01qj1g5pua90",
    "cnidgf1r01qj1g5pubegcnidgf1r01qj1g5pubf0",
    "cnidgnpr01qj1g5pubu0cnidgnpr01qj1g5pubug",
    "cnidh11r01qj1g5pud30cnidh11r01qj1g5pud3g",
    "cnidh89r01qj1g5pudc0cnidh89r01qj1g5pudcg",
    "cnidhghr01qj1g5puef0cnidhghr01qj1g5puefg",
    "cnidhr1r01qj1g5pufl0cnidhr1r01qj1g5puflg",
    "cnidi29r01qj1g5pufs0cnidi29r01qj1g5pufsg",
    "cnidid1r01qj1g5puh3gcnidid1r01qj1g5puh40"
]

// parameter
const tokenLen = tokens.length;
const groupSize = 30; // request per token
const intervalTime = 1000;
var len;
var timeoutFactor;

// function to get data for each symbol
function getCompanyProfile(symbolsList) {

    // updating parameters based on symbols list
    len = symbolsList.length;
    timeoutFactor = len / groupSize + 30; // overshoot, doesn't need to be accurate

    timeTask = setInterval(async () => {
        var j = 0; // keep track of items in groups
        var tempList = [];

        status.Groups_left++;

        while (status.Symbols_fetched < len && j < groupSize) {
            const response = await fetch(`https://finnhub.io/api/v1/stock/profile2?symbol=${symbolsList[status.Symbols_fetched]}&token=${tokens[status.Symbols_fetched++ % tokenLen]}`);
            const data = await response.json();

            // donot store if empty object is returned
            if (Object.keys(data).length != 0) {
                tempList.push(data);
            }

            j++;
        }

        if (tempList.length) {
            mongoInsertMany(tempList, "StockMarket", "Profile")
        }

    }, intervalTime);
    
    /**
     * SetTimeout and javascript in general is not accurate
     * Give some extra time (30 more seconds given) before clearInterval is triggered
     * If tempList array is empty, donot call mongoInsertMany func
     */

    setTimeout(() => {
        clearInterval(timeTask);
    
        // reset the status
        status.DBConnection = false;
        status.Current_status = "Not started yet"

    }, timeoutFactor * intervalTime);
}

// function to get all stock symbols
async function getSymbols() {

    // initialize status
    status.Symbols_list_size = 0;
    status.Symbols_fetched = 0;
    status.Symbols_added = 0;
    status.Groups_added = 0;
    status.Error_log = [];

    // get list of symbols
    const response = await fetch("https://finnhub.io/api/v1/stock/symbol?exchange=US&token=cnia8e9r01qj1g5ppaogcnia8e9r01qj1g5ppap0");
    const data = await response.json();
    const symbols = data.map(element => element.symbol);

    // change status parameters
    status.Current_status = "Fetching company details";
    status.Symbols_list_size = symbols.length;

    // fetch data for symbols
    getCompanyProfile(symbols);
}


// *************************************** Express *******************************************

const app = express();
app.use(cors());
app.use(bodyParser.json());

const port = process.env.PORT || 3000;

app.listen(port, () => {
    // console.log(`Server is running on port ${port}`); // for debugging
})

// ____________________________________________ valid requests ____________________________________________________

app.get("/api/start", async (req, res) => {
    // return if already operating
    if (status.DBConnection === true) return res.json({ message: "Process already operational" });
    
    // change status
    status.Current_status = "Fetching Symbols";
    status.DBConnection = true;
    await client.connect();

    // start process
    getSymbols();

    // send response
    res.json({ message: "The process has started" });
})

app.get("/api/end", (req, res) => {
    // return if process not yet operating
    if (status.DBConnection === false) return res.json({ message: "The process is not operational" })

    // reset the status
    status.Current_status = "Not started yet"
    status.DBConnection = false;
    
    // end process
    clearInterval(timeTask)

    // send response
    res.json({ message: "The process has ended" })
})

app.get("/api/status", (req, res) => {
    try {
        status.estimatedTimeLeft = new Date((timeoutFactor - status.Groups_added) * intervalTime).toISOString().slice(11, 19);
    }
    catch (err) {
        if (err.message === "Invalid time value") {
            status.estimatedTimeLeft = "Not yet calculated"
        }
    }
    res.json(status)
})

// ____________________________________________ error handling ________________________________________________________

app.all("*", (req, res) => {
    throw new Error("Not Found")
})

app.use((e, req, res, next) => {
    if (e.message === "Not Found") {
        res.status(404).json({error: e.message});
    }
});