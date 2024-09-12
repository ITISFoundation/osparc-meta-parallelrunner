const express = require('express');
const fs = require('fs').promises;
const path = require('path');
const cors = require('cors');

const app = express();
const port = 8888;

app.use(cors());
app.use(express.json());

const dataFile = path.join(__dirname, 'jobs.json');

const calculateCompletionTime = (job) => {
  return job.endTime && job.startTime 
    ? Math.floor((job.endTime - job.startTime) / 1000)
    : null;
};

const calculateDynamicETA = (runningTime, finishedJobs) => {
  const validFinishedJobs = finishedJobs.filter(job => job.completionTime !== null);
  
  if (validFinishedJobs.length === 0) {
    return null;
  }
  
  const averageCompletionTime = validFinishedJobs.reduce((sum, job) => sum + job.completionTime, 0) / validFinishedJobs.length;
  const estimatedTimeRemaining = Math.max(1, Math.floor(averageCompletionTime - runningTime));
  return estimatedTimeRemaining;
};

const calculateRunningTimeAndETA = (job, finishedJobs) => {
  const now = Date.now();
  if (job.status === 'running') {
    job.runningTime = Math.floor((now - job.startTime) / 1000);
    job.eta = calculateDynamicETA(job.runningTime, finishedJobs);
  } else if (job.status === 'done') {
    job.completionTime = calculateCompletionTime(job);
  }
  return job;
};

app.get('/api/jobs', async (req, res) => {
  try {
    const data = await fs.readFile(dataFile, 'utf8');
    const jobs = JSON.parse(data);
    
    // First, calculate completion times for finished jobs
    const finishedJobs = Object.values(jobs)
      .filter(job => job.status === 'done')
      .map(job => ({...job, completionTime: calculateCompletionTime(job)}));

    let totalRemainingTime = 0;
    let runningJobsCount = 0;
    let todoJobsCount = 0;

    const updatedJobs = Object.entries(jobs).reduce((acc, [id, job]) => {
      const updatedJob = calculateRunningTimeAndETA(job, finishedJobs);
      acc[id] = updatedJob;
      
      if (updatedJob.status === 'running' && updatedJob.eta !== null) {
        totalRemainingTime += updatedJob.eta;
        runningJobsCount++;
      } else if (updatedJob.status === 'todo') {
        todoJobsCount++;
      }
      
      return acc;
    }, {});

    // Calculate average completion time for estimating todo jobs
    const averageCompletionTime = finishedJobs.length > 0
      ? finishedJobs.reduce((sum, job) => sum + job.completionTime, 0) / finishedJobs.length
      : null;

    // Add estimated time for todo jobs
    if (averageCompletionTime !== null) {
      totalRemainingTime += todoJobsCount * averageCompletionTime;
    }

    const overallETA = (runningJobsCount > 0 || todoJobsCount > 0) ? Math.floor(totalRemainingTime) : null;

    res.json({
      jobs: updatedJobs,
      totalETA: overallETA
    });
  } catch (err) {
    console.error('Error reading file:', err);
    res.status(500).json({ error: 'Error reading job data' });
  }
});

app.put('/api/jobs/:id', async (req, res) => {
  const { id } = req.params;
  const { status } = req.body;

  try {
    const data = await fs.readFile(dataFile, 'utf8');
    const jobs = JSON.parse(data);

    if (!jobs[id]) {
      return res.status(404).json({ error: 'Job not found' });
    }

    const now = Date.now();
    if (status === 'running' && jobs[id].status !== 'running') {
      jobs[id].startTime = now;
    } else if (status === 'done' && jobs[id].status !== 'done') {
      jobs[id].endTime = now;
    }

    jobs[id].status = status;

    // Recalculate completion times for all finished jobs
    const finishedJobs = Object.values(jobs)
      .filter(job => job.status === 'done')
      .map(job => ({...job, completionTime: calculateCompletionTime(job)}));

    const updatedJob = calculateRunningTimeAndETA(jobs[id], finishedJobs);

    await fs.writeFile(dataFile, JSON.stringify(jobs, null, 2));
    res.json(updatedJob);
  } catch (err) {
    console.error('Error updating job:', err);
    res.status(500).json({ error: 'Error updating job' });
  }
});

app.use(express.static(path.join(__dirname, 'dist')));

app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'dist', 'index.html'));
});

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});
