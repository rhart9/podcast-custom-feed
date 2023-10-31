async function launch() {
    let handlerValue = await require("./index.js").handler({ "mode": process.argv[2] });

    console.log("Finished");
    console.log(handlerValue);

    process.exit();
}
launch();
