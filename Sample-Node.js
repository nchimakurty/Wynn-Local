exports = async function(changeEvent) {
  try {
    // Log the full document from the change event
    console.log("Trigger invoked!");
    console.log("Full Document:", JSON.stringify(changeEvent.fullDocument, null, 2));

    // Log additional metadata if needed
    console.log("Namespace (Database and Collection):", changeEvent.ns);
    console.log("Operation Type:", changeEvent.operationType);
  } catch (error) {
    console.error("An error occurred while running the test trigger:", error);
  }
};


exports = async function(changeEvent) {

  
  const db = context.services.get("mongodb-atlas").db("<your_database_name>");
  const stayCollection = db.collection("stay");
  const itemCollection = db.collection("item");

  // Get the document that triggered the event
  const item = changeEvent.fullDocument;

  const { player_id, start_date, end_date } = item;

  // Ensure dates are in ISO format
  const itemStartDate = new Date(start_date);
  const itemEndDate = new Date(end_date);

  // Search in "stay" collection for matching conditions
  const matchingStay = await stayCollection.findOne({
    player_id,
    $or: [
      { $and: [{ start_date: { $lte: itemStartDate } }, { end_date: { $gte: itemStartDate } }] },
      { $and: [{ start_date: { $lte: itemEndDate } }, { end_date: { $gte: itemEndDate } }] }
    ]
  });

  if (matchingStay) {
    // Update "stay" start_date and end_date if necessary
    const updates = {};
    if (new Date(matchingStay.start_date) > itemStartDate) {
      updates.start_date = itemStartDate;
    }
    if (new Date(matchingStay.end_date) < itemEndDate) {
      updates.end_date = itemEndDate;
    }

    if (Object.keys(updates).length > 0) {
      await stayCollection.updateOne({ _id: matchingStay._id }, { $set: updates });
    }

    // Update the "stay_id" field in "item" collection
    await itemCollection.updateOne({ _id: item._id }, { $set: { stay_id: matchingStay._id } });
  } else {
    // No matching "stay" record; leave "stay_id" as null or undefined
    await itemCollection.updateOne({ _id: item._id }, { $set: { stay_id: null } });
  }
};
