db.collection.aggregate([
  // Stage 1: Ensure both fields exist
  {
    $match: {
      $and: [
        { venue_id: { $exists: true } },
        { venue_key: { $exists: true } }
      ]
    }
  },

  // Stage 2: Filter documents where venue_id == venue_key and neither is null or empty
  {
    $match: {
      $expr: {
        $and: [
          { $eq: ["$venue_id", "$venue_key"] },
          { $ne: ["$venue_id", ""] },
          { $ne: ["$venue_key", ""] },
          { $ne: ["$venue_id", null] },
          { $ne: ["$venue_key", null] }
        ]
      }
    }
  },

  // Stage 3: Update venue_key to "123"
  {
    $set: {
      venue_key: "123"
    }
  },

  // Stage 4: Write updated docs back into the collection
  {
    $merge: {
      into: "collection", // Change this to your actual collection name
      whenMatched: "merge",
      whenNotMatched: "discard"
    }
  }
]);
