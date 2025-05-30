exports = async function(changeEvent) {
  try {
    const dbName = changeEvent.ns.db; // Dynamically get the database name
    const db = context.services.get("mongodb-atlas").db(dbName);
    const stayCollection = db.collection("stay");

    console.log("Trigger invoked!");

    // Extract fields from the fullDocument
    const fullDocument = changeEvent.fullDocument || {};
    const player_id = fullDocument.player_id;
    const itemStartDate = new Date(fullDocument.start_date);
    const itemEndDate = new Date(fullDocument.end_date);

    // Validate fields
    if (!player_id || isNaN(itemStartDate.getTime()) || isNaN(itemEndDate.getTime())) {
      console.log("Essential fields are invalid or missing. Skipping processing.");
      return;
    }

    console.log(`Player ID: ${player_id}`);
    console.log(`Item Start Date: ${itemStartDate}`);
    console.log(`Item End Date: ${itemEndDate}`);

    // Search for a matching stay record
    const matchingStay = await stayCollection.findOne({
      player_id,
      $or: [
        { $and: [{ start_date: { $lte: itemStartDate } }, { end_date: { $gte: itemStartDate } }] },
        { $and: [{ start_date: { $lte: itemEndDate } }, { end_date: { $gte: itemEndDate } }] }
      ]
    });

    if (!matchingStay) {
      // No matching record found; insert a new record
      const newStay = {
        player_id,
        start_date: itemStartDate,
        end_date: itemEndDate
      };
      const insertResult = await stayCollection.insertOne(newStay);
      console.log("No matching stay record found. Inserted new record:", newStay);
    } else {
      // Matching record found; update it if necessary
      console.log("Matching stay record before update:", JSON.stringify(matchingStay, null, 2));

      const updates = {};
      if (itemStartDate < new Date(matchingStay.start_date)) {
        updates.start_date = itemStartDate;
      }
      if (itemEndDate > new Date(matchingStay.end_date)) {
        updates.end_date = itemEndDate;
      }

      if (Object.keys(updates).length > 0) {
        const updateResult = await stayCollection.updateOne(
          { _id: matchingStay._id },
          { $set: updates }
        );
        const updatedStay = await stayCollection.findOne({ _id: matchingStay._id });
        console.log("Matching stay record after update:", JSON.stringify(updatedStay, null, 2));
      } else {
        console.log("No updates required for the matching stay record.");
      }
    }
  } catch (error) {
    console.error("An error occurred while running the trigger:", error.message);
    console.error("Error Stack Trace:", error.stack);
  }
};

-------------------------------

exports = async function(changeEvent) {
  try {
    console.log("Trigger invoked!");

    // Get all services
    const services = context.services;

    // Iterate over and log all service names
    console.log("Available Service Names:");
    Object.keys(services).forEach(serviceName => {
      console.log(`- ${serviceName}`);
    });

  } catch (error) {
    console.error("An error occurred while retrieving service names:", error);
  }
};

--------------------------

exports = async function(changeEvent) {
  try {
    const dbName = changeEvent.ns.db; // Dynamically get the database name
    const db = context.services.get("mongodb-atlas").db(dbName);
    const stayCollection = db.collection("stay");

    // Log metadata
    console.log("Trigger invoked!");
    console.log("Namespace:", changeEvent.ns);
    console.log("Operation Type:", changeEvent.operationType);

    // Extract the full document
    const fullDocument = changeEvent.fullDocument || {};
    const player_id = fullDocument.player_id;
    const itemStartDate = new Date(fullDocument.start_date);
    const itemEndDate = new Date(fullDocument.end_date);

    // Log extracted fields
    console.log(`Player ID: ${player_id}`);
    console.log(`Item Start Date: ${itemStartDate}`);
    console.log(`Item End Date: ${itemEndDate}`);

    // Validate essential fields
    if (!player_id || !itemStartDate || !itemEndDate) {
      console.log("Essential fields missing from the document. Skipping processing.");
      return;
    }

    // Search for a matching stay record
    console.log("Searching for matching records in the 'stay' collection...");
    const matchingStay = await stayCollection.findOne({
      player_id,
      $or: [
        { $and: [{ start_date: { $lte: itemStartDate } }, { end_date: { $gte: itemStartDate } }] },
        { $and: [{ start_date: { $lte: itemEndDate } }, { end_date: { $gte: itemEndDate } }] }
      ]
    });

    // Log the result of the search
    if (matchingStay) {
      console.log("Matching stay record found:");
      console.log(JSON.stringify(matchingStay, null, 2));
    } else {
      console.log("No matching stay record found.");
    }
  } catch (error) {
    console.error("An error occurred while running the trigger:", error);
  }
};

-----------------------


exports = async function(changeEvent) {
  try {
    // Get the full document from the change event
    const item = changeEvent.fullDocument;

    if (item) {
      // Extract the required fields
      const player_id = item.player_id;
      const roomStartDate = item.room?.start_date;
      const roomEndDate = item.room?.end_date;

      // Log the extracted fields
      console.log("Trigger invoked!");
      console.log(`Player ID: ${player_id}`);
      console.log(`Room Start Date: ${roomStartDate}`);
      console.log(`Room End Date: ${roomEndDate}`);
    } else {
      console.log("No full document available in the change event.");
    }

    // Log additional metadata if needed
    console.log("Namespace (Database and Collection):", changeEvent.ns);
    console.log("Operation Type:", changeEvent.operationType);
  } catch (error) {
    console.error("An error occurred while running the test trigger:", error);
  }
};



exports = async function(changeEvent) {
  // Dynamically get the database name from the change event
  const dbName = changeEvent.ns.db;
  const db = context.services.get("mongodb-atlas").db(dbName);
  const stayCollection = db.collection("stay");
  const itemCollection = db.collection("item");

  // Get the document that triggered the event
  const item = changeEvent.fullDocument;

  const { player_id, start_date, end_date } = item;

  console.log(`Trigger invoked for item with _id: ${item._id}`);
  console.log(`Player ID: ${player_id}, Start Date: ${start_date}, End Date: ${end_date}`);

  // Ensure dates are in ISO format
  const itemStartDate = new Date(start_date);
  const itemEndDate = new Date(end_date);

  try {
    // Search in "stay" collection for matching conditions
    console.log("Searching for matching records in the 'stay' collection...");
    const matchingStay = await stayCollection.findOne({
      player_id,
      $or: [
        { $and: [{ start_date: { $lte: itemStartDate } }, { end_date: { $gte: itemStartDate } }] },
        { $and: [{ start_date: { $lte: itemEndDate } }, { end_date: { $gte: itemEndDate } }] }
      ]
    });

    if (matchingStay) {
      console.log(`Matching stay found with _id: ${matchingStay._id}`);
      console.log(`Stay start_date: ${matchingStay.start_date}, end_date: ${matchingStay.end_date}`);

      // Update "stay" start_date and end_date if necessary
      const updates = {};
      if (new Date(matchingStay.start_date) > itemStartDate) {
        updates.start_date = itemStartDate;
        console.log(`Updating stay start_date to: ${itemStartDate}`);
      }
      if (new Date(matchingStay.end_date) < itemEndDate) {
        updates.end_date = itemEndDate;
        console.log(`Updating stay end_date to: ${itemEndDate}`);
      }

      if (Object.keys(updates).length > 0) {
        await stayCollection.updateOne({ _id: matchingStay._id }, { $set: updates });
        console.log(`Stay updated successfully.`);
      } else {
        console.log("No updates needed for stay dates.");
      }

      // Update the "stay_id" field in "item" collection
      await itemCollection.updateOne({ _id: item._id }, { $set: { stay_id: matchingStay._id } });
      console.log(`Item stay_id updated to: ${matchingStay._id}`);
    } else {
      console.log("No matching stay record found.");
      // No matching "stay" record; leave "stay_id" as null or undefined
      await itemCollection.updateOne({ _id: item._id }, { $set: { stay_id: null } });
      console.log("Item stay_id set to null.");
    }
  } catch (error) {
    console.error("An error occurred during trigger execution:", error);
  }
};
