db.collection.aggregate([
  {
    $addFields: {
      venue_key: {
        $cond: [
          { $eq: ["$venue_id", "$venue_key"] },
          "123",
          "$venue_key"
        ]
      }
    }
  },
  {
    $merge: {
      into: "collection",
      whenMatched: "merge",
      whenNotMatched: "discard"
    }
  }
]);


db.collection.aggregate([
  {
    $addFields: {
      venue_key: {
        $cond: [
          { $eq: ["$venue_key", ""] },
          "xyz",
          "$venue_key"
        ]
      }
    }
  },
  {
    $merge: {
      into: "collection",
      whenMatched: "merge",
      whenNotMatched: "discard"
    }
  }
]);
