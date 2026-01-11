const express = require("express");
const fs = require("fs");
const amqp = require('amqplib');

if (!process.env.PORT) {
    throw new Error("Please specify the port number for the HTTP server with the environment variable PORT.");
}

if (!process.env.RABBIT) {
    throw new Error("Please specify the name of the RabbitMQ host using environment variable RABBIT");
}

const PORT = process.env.PORT;
const RABBIT = process.env.RABBIT;

//
// Application entry point.
//
async function main() {
	
    console.log(`Connecting to RabbitMQ server at ${RABBIT}.`);

    const messagingConnection = await amqp.connect(RABBIT); // Connects to the RabbitMQ server.
    
    console.log("Connected to RabbitMQ.");

    const messageChannel = await messagingConnection.createChannel(); // Creates a RabbitMQ messaging channel.

	await messageChannel.assertExchange("viewed", "fanout"); // Asserts that we have a "viewed" exchange.

    //
    // Broadcasts the "viewed" message to other microservices.
    //
	function broadcastViewedMessage(messageChannel, id, videoPath) {
        const msg = { id: id, videoPath: videoPath };
        messageChannel.publish("viewed", "", Buffer.from(JSON.stringify(msg)));
    }


    const app = express();

    app.get("/video", async (req, res) => { // Route for streaming video.
        const id = req.query.id;

        // validate id
        if (id !== "1" && id !== "2") {
            return res.status(400).send("Please specify id=1 or id=2");
        }

        // pick mp4 by id
        const videoPath = id === "1"
            ? "./videos/SampleVideo1.mp4"
            : "./videos/SampleVideo2.mp4";

        const stats = await fs.promises.stat(videoPath);

        res.writeHead(200, {
            "Content-Length": stats.size,
            "Content-Type": "video/mp4",
        });

        fs.createReadStream(videoPath).pipe(res);

        // send id + path
        broadcastViewedMessage(messageChannel, id, videoPath);
    });



    app.listen(PORT, () => {
        console.log("Microservice online.");
    });
    }

main()
    .catch(err => {
        console.error("Microservice failed to start.");
        console.error(err && err.stack || err);
    });