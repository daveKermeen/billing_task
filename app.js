import pkg from "pg";
const { Client } = pkg;
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import fs from "fs";
import path from "path";
import { dbConfig } from "./config.js";
import nodemailer from "nodemailer";
import dotenv from "dotenv";

// Load env variables from .env file
dotenv.config();

async function main() {
  // 1. Connect to PostgreSQL and fetch the file path
  const filePath = await connectAndQuery();

  if (!filePath) {
    console.log("No valid file path retrieved from the database.");
    return; // Exit if no valid file path is found
  }

  // 2. Download the file from s3
  await downloadFromS3(filePath);

  // 3. Hardcoded contact list
  const contacts = [
    { name: "David Kermeen", email: "dkermeen@roubler.com" },
    { name: "David James", email: "dkermeen+david_james@roubler.com" },
    { name: "James David", email: "dkermeen+james_david@roubler.com" },
  ];

  // 4. Send the email
  await sendEmailsWithAttachment(contacts, filePath);
}

async function sendEmailsWithAttachment(contacts, filePath) {
  // Configure email transporter
  const transporter = nodemailer.createTransport({
    service: "Gmail",
    auth: {
      user: process.env.EMAIL_USER,
      pass: process.env.EMAIL_PASSWORD,
    },
  });

  // Path to download file
  const attachmentPath = path.join("downloads", path.basename(filePath));

  for (const contact of contacts) {
    const mailOptions = {
      from: process.env.EMAIL_USER,
      to: contact.email,
      subject: "Here is your file",
      text: `Hello ${contact.name},\n\nPlease find the attached file.\n\nBest regards, xyz`,
      attachments: [
        {
          path: attachmentPath, // Attach the file
        },
      ],
    };

    try {
      await transporter.sendMail(mailOptions);
      console.log(
        `Email sent to: ${contact.email} with attachment ${attachmentPath}`
      );
    } catch (err) {
      console.log(`Error sending email to ${contact.email}:`, err.message);
    }
  }
}

// Download file from S3
async function downloadFromS3(filePath) {
  // Initialise S3 client
  const s3 = new S3Client({
    region: process.env.AWS_REGION,
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    },
  });

  const downloadDirectory = path.join("downloads");
  const downloadPath = path.join(downloadDirectory, path.basename(filePath));

  const params = {
    Bucket: process.env.AWS_BUCKET_NAME,
    Key: filePath,
  };

  try {
    const data = await s3.send(new GetObjectCommand(params));
    const fileStream = fs.createWriteStream(downloadPath);
    data.Body.pipe(fileStream);

    fileStream.on("close", () => {
      console.log(`File downloaded successfully to ${downloadPath}`);
    });
  } catch (err) {
    console.log("Error downloading file from S3", err);
  }
}

// Connect to PSQL database and run query
async function connectAndQuery() {
  // Create a new postgres client using config
  const client = new Client(dbConfig);
  let filePath = null;

  try {
    // Connect to database
    await client.connect();
    console.log("Connected to PostgreSQL");

    // Run query
    const query =
      "select resource.path from resource join person_details on resource.id = person_details.id_photo where person_details.person = 122378;";
    const response = await client.query(query);

    if (response.rows.length > 0) {
      filePath = response.rows[0].path;
      console.log("S3 file path:", filePath);
    } else {
      console.log("No results found.");
    }
  } catch (err) {
    console.log("Error executing query", err.stack);
  } finally {
    // Disconnect from the database
    await client.end();
    console.log("Disconnected from PostgreSQL");
  }

  return filePath; // Return the filePath
}

// Call function to test the connection
// connectAndQuery();
main();
