exports = async function pickAndDeliver(args) {
  var sourceColl = context.services
    .get("federated-instance")
    .db("federated")
    .collection("changestreams");

  var milestoneColl = context.services
    .get("mongodb-atlas")
    .db("federated")
    .collection("milestone");

  var prevMilestone = milestoneColl.findOne({}, { ts: 1, batch: 1, _id: 0 });
  var nextMilestone = prevMilestone.ts + 5 * 60 * 1000; //5 mins into ms

  var agg = [
    {
      $match: { clusterTime: { $gte: prevMilestone.ts, $lte: nextMilestone } },
    },
    {
      $out: {
        s3: {
          bucket: "ankurs-bizprod-bucket",
          region: "us-east-1",
          filename: "BATCH-".concat(prevMilestone.batch),
          format: {
            name: "parquet",
          },
        },
      },
    },
  ];

  await sourceColl.aggregate(agg, { background: true });

  await milestoneColl.updateOne(
    {},
    { $set: { ts: nextMilestone }, $inc: { batch: 1 } }
  );

  //test deploy on app service on commit
};
