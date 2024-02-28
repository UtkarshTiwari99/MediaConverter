import aws from 'aws-sdk';
import ffmpeg from 'fluent-ffmpeg';
import fs from 'fs';
export const mediaConverter = async (mp4VideoBucket, videoHlsFilesBucket) => {

    aws.config.update({
        accessKeyId: "ACCESSKEY",
        secretAccessKey: "SECRESTACCESSKEY",
        region:"ap-south-1"
    });

    const clusterName = 'MediaConverter'

    const serviceName = "originalconversion"

    const sqs = new aws.SQS();

    const queueUrl = 'https://sqs.ap-south-1.amazonaws.com/843904961121/videoHlsQueue.fifo';

    try {
        const len = await getQueueLength(sqs, queueUrl);
        if (len <= 0)
            throw "Queue is Empty"
        const messageBlock = await receiveMessageFromQueue(sqs, queueUrl)
        const message = JSON.parse(messageBlock.Body)
        const id = message.videoId
        await mediaConverterForOriginal(id, mp4VideoBucket, videoHlsFilesBucket)
        const deleteMessage = await deleteMessageFromQueue(sqs, queueUrl, messageBlock)
    } catch (e) {
        console.log("Getting error During mediaConversion " + e)
    } finally {
        await furtherConfig(clusterName, serviceName, sqs, queueUrl)
    }
    console.log("Hls Conversion Completes")
};

const furtherConfig = async (clusterName, serviceName, sqs, queryUrl) => {

    const ecs = new aws.ECS();

    try {

        const queueLength = await getQueueLength(sqs, queryUrl);
        if(queueLength===0)
            await updateServiceDesiredCount(ecs,clusterName,serviceName,1)

    } catch (error) {
        console.error('Error launching task:', error);
    }
};

const getCurrentDesiredCount = async (ecs,clusterName,serviceName) => {
    const params = {
        cluster: clusterName,
        services: [serviceName],
    };

    try {
        const result = await ecs.describeServices(params).promise();
        if(result.services.length===0)
            return 0;
        return result.services[0].desiredCount;
    } catch (error) {
        console.error('Error getting current desired count:', error);
        return null;
    }
};
const updateServiceDesiredCount = async (ecs,clusterName,serviceName,decreaseBy) => {
    const currentDesiredCount = await getCurrentDesiredCount(ecs,clusterName,serviceName);

    if (currentDesiredCount !== null) {
        const newDesiredCount = Math.max(0, currentDesiredCount - decreaseBy);

        const params = {
            cluster: clusterName,
            service: serviceName,
            desiredCount: newDesiredCount,
        };

        try {
            const result = await ecs.updateService(params).promise();
            console.log(`Desired count updated successfully: ${result.service.desiredCount}`);
        } catch (error) {
            console.error('Error updating desired count:', error);
        }
    }
};

const getQueueLength = async (sqs, queueUrl) => {
    const params = {
        QueueUrl: queueUrl,
        AttributeNames: ['ApproximateNumberOfMessages','ApproximateNumberOfMessagesNotVisible']
    };

    try {
        const result = await sqs.getQueueAttributes(params).promise();
        console.log(result.Attributes)
        return result.Attributes.ApproximateNumberOfMessages-result.Attributes.ApproximateNumberOfMessagesNotVisible
    } catch (e) {
        console.log('Error during retrieval of queue length:', e.message);
    }

    return 0;
};


const receiveMessageFromQueue = async (sqs, queueUrl) => {
    const data=await sqs.receiveMessage({QueueUrl: queueUrl, MaxNumberOfMessages: 1})
        .promise()
    return data.Messages[0]
};

const mediaConverterForOriginal = async (videoId, mp4VideoBucket, videoHlsFilesBucket) => {

    const s3 = new aws.S3();

    const params = { Bucket: mp4VideoBucket, Key: `${videoId}.mp4` };
    const object = s3.getObject(params);
    var totalTime;

    try {
        const tmpDirPath = "videohls"
        try {
            await fs.promises.mkdir(tmpDirPath, {recursive: true, mode: 0o755}); // Recommended
        } catch (e) {
            console.log("tmpDirPath Error" + e);
        }

        const readStream = object.createReadStream();

        await new Promise(async (resolve, reject) => {
            const ffmpegProcess = ffmpeg(readStream)
                .on('start', () => {
                    console.log(`Transcoding ${videoId} for HLS with original resolution`);
                })
                .on('error', (err, stdout, stderr) => {
                    console.log('HLS error:', stderr);
                    console.log('stdout:', stdout);
                    console.error('error:', err);
                    reject(err);
                    ffmpegProcess.kill();
                })
                .on('end', async () => {
                    const fileUploadPromises = [];

                    for (const file of await fs.promises.readdir(tmpDirPath)) {

                        if (!fs.statSync(`${tmpDirPath}/${file}`).isFile()) {
                            console.log(`Skipping directory: ${file}`);
                            continue;
                        }

                        const uploadParams = {
                            Bucket: videoHlsFilesBucket,
                            Key: file,
                            Body: fs.readFileSync(`${tmpDirPath}/${file}`),
                        };

                        fileUploadPromises.push(s3.putObject(uploadParams).promise());
                        console.log(`Uploading ${file} to S3`);

                    }

                    await Promise.all(fileUploadPromises);

                    fs.rmSync(tmpDirPath, {recursive: true})

                    ffmpegProcess.kill();
                    resolve();
                })
                .on('codecData', (data) => {
                    totalTime = parseInt(data.duration.replace(/:/g, ''));
                })
                .on('progress', (progress) => {
                    const time = parseInt(progress.timemark.replace(/:/g, ''));
                    const percent = Math.ceil((time / totalTime) * 100);
                    console.log(`Progress: ${percent}%`);
                })
                .outputOptions([
                    "-c:v", "libx264",
                    "-c:a", "aac",
                    "-hls_time", "4",
                    "-hls_list_size", "0",
                    '-f hls',
                    '-hls_flags delete_segments',
                    `-hls_segment_filename`, `${tmpDirPath}/${videoId}_Original_%d.ts`,
                ])
                .output(`${tmpDirPath}/${videoId}_Original_main.m3u8`);

            await new Promise((resolve) => {
                ffmpegProcess.run();
            });
        });

        console.log('ECS task Completed');
    } catch (e) {
        console.error('ECS task Failed during conversion:', e);
    }
};

const deleteMessageFromQueue = async (sqs, queueUrl, message) => {

    const receiptHandle = message.ReceiptHandle;

    console.log('Received message:', message);

    return sqs.deleteMessage({QueueUrl: queueUrl, ReceiptHandle: receiptHandle})
        .promise()
};