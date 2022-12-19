exports = async function pickAndDeliver(args) {
  var sourceColl = context.services
    .get("federated-instance")
    .db("federated")
    .collection("changestreams");

  var milestoneColl = context.services
    .get("mongodb-atlas")
    .db("persist")
    .collection("milestone");

  var prevMilestone = await milestoneColl.findOne(
    {},
    { ts: 1, batch: 1, _id: 0 }
  );
  var nextMilestone = prevMilestone.ts.getTime() + 5 * 60 * 1000; //5 mins into ms

  console.log(JSON.stringify(new Date(prevMilestone.ts.getTime())));
  console.log(JSON.stringify(new Date(nextMilestone)));

  var agg = [
    {
      $match: {
        clusterTime: {
          $gte: new Date(prevMilestone.ts.getTime()),
          $lte: new Date(nextMilestone),
        },
      },
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

  var res = await sourceColl.aggregate(agg, { background: true });

  await milestoneColl.updateOne(
    {},
    { $set: { ts: new Date(nextMilestone) }, $inc: { batch: 1 } }
  );

  //test deploy on app service on commit

  console.log(res);
  return res;
};
