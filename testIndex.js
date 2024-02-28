import {mediaConverter} from './index.mjs'
const mp4VideoBucket = 'videosforhls'
const videoHlsFilesBucket= 'videohlsfiles'
console.log(await mediaConverter(mp4VideoBucket,videoHlsFilesBucket))